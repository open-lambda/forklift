package docker

import (
	"bufio"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/archive"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"rb/platform_adapter"
	"rb/util"
	"rb/workload"
	"strconv"
	"strings"
	"sync"
	"time"
)

var runHandlerCode = `
import sys, time
import json

if __name__ == "__main__":
    start_time = time.time()
    # change sys.path to import packages
    with open("/app/requirements.txt", "r") as file:
        reqs = file.read().splitlines()
        for req in reqs:
            req = req.split(";")[0].strip()
            if req != "" and not req.strip().startswith("#"):
                sys.path.insert(0, f"/packages/{req}")
    req = json.loads(sys.argv[1])
    import f
    res = f.f(req)
    res["start_time"] = start_time*1000
    print(json.dumps(res))
`

var packageBaseDockerfile = `
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y python3 python3-pip

COPY pkg_list.txt /pkg_list.txt
COPY install_all.py /install_all.py
RUN python3 /install_all.py /pkg_list.txt

CMD ["sleep", "3"]
`

func getMem(containerLongID string) (int, error) {
	path := fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope/memory.current", containerLongID)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, err
	}
	memoryCurrentStr := strings.TrimSpace(string(data))
	memoryCurrent, err := strconv.Atoi(memoryCurrentStr)
	if err != nil {
		return 0, err
	}
	return memoryCurrent, nil
}

func maxMemUsage(containerLongID string,
	stopChan <-chan struct{}, returnChan chan<- int,
	interval time.Duration) {
	maxMem := 0
	for {
		select {
		case <-stopChan:
			returnChan <- maxMem
			return
		default:
			mem, err := getMem(containerLongID)
			if err == nil && mem > maxMem {
				maxMem = mem
			}
			time.Sleep(interval * time.Millisecond)
		}
	}
}

type LatencyRecord struct {
	Id          string   `json:"invoke_id"`
	Mem         int      `json:"mem"`
	Req         float64  `json:"req"`
	Create      float64  `json:"create_done"` // Client.ContainerCreate finished
	Start       float64  `json:"start"`       // Client.ContainerStart finished
	StartImport float64  `json:"start_import"`
	EndImport   float64  `json:"end_import"`
	EndExec     float64  `json:"end_execute"`
	Failed      []string `json:"failed"`
	Received    float64  `json:"received"`
}

func (record *LatencyRecord) ToSlice() []string {
	failedStr := "[]"
	if len(record.Failed) > 0 {
		failedStr = fmt.Sprintf("[%s]", strings.Join(record.Failed, ","))
	}
	return []string{
		record.Id,
		strconv.Itoa(record.Mem),
		strconv.FormatFloat(record.Req, 'f', 3, 64),
		strconv.FormatFloat(record.Create, 'f', 3, 64),
		strconv.FormatFloat(record.Start, 'f', 3, 64),
		strconv.FormatFloat(record.StartImport, 'f', 3, 64),
		strconv.FormatFloat(record.EndImport, 'f', 3, 64),
		strconv.FormatFloat(record.EndExec, 'f', 3, 64),
		strconv.FormatFloat(record.Received, 'f', 3, 64),
		failedStr,
	}
}

func (record *LatencyRecord) GetHeaders() []string {
	return []string{"invoke_id", "Mem", "Req", "Create", "Start", "StartImport", "EndImport", "EndExec", "Received", "Failed"}
}

type DockerPlatform struct {
	platform_adapter.BasePlatformAdapter
	currentDir         string // docker.go(current file)'s path, should be defined in the config.json
	client             *client.Client
	handlersDir        string
	containers         list.List
	containersListLock sync.Mutex
	metricsLock        sync.Mutex
	evictorStopChan    chan struct{}
	metrics            []platform_adapter.Record
}

func NewDockerPlatform() (*DockerPlatform, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &DockerPlatform{
		client:     cli,
		containers: list.List{},
	}, nil
}

func evictor(d *DockerPlatform, interval time.Duration, stopChan <-chan struct{}, size int) {
	for {
		select {
		case <-stopChan:
			return
		default:
			time.Sleep(interval * time.Millisecond)
			for {
				d.containersListLock.Lock()
				containerID := ""
				if d.containers.Len() <= size {
					d.containersListLock.Unlock()
					break
				} else {
					containerID = d.containers.Front().Value.(string)
					d.containers.Remove(d.containers.Front())
				}
				d.containersListLock.Unlock()

				ctx := context.Background()
				err := d.client.ContainerRemove(ctx, containerID,
					types.ContainerRemoveOptions{
						RemoveVolumes: false,
						RemoveLinks:   false,
						Force:         true,
					})
				if err != nil {
					log.Printf("Error removing container %s: %v", containerID, err)
				}
			}
		}
	}
}

func (d *DockerPlatform) StartWorker(options map[string]interface{}) error {
	tmpDir := d.Config["tmp_dir"].(string)
	base := d.Config["base_url"].(string)
	cli, err := client.NewClientWithOpts(client.WithHost(base),
		client.WithAPIVersionNegotiation(), client.WithVersion("1.43"))
	if err != nil {
		return err
	}
	d.client = cli

	if err := os.MkdirAll(tmpDir, 0755); err != nil && !os.IsExist(err) {
		return nil
	}
	_ = ioutil.WriteFile(filepath.Join(tmpDir, ".dockerignore"), []byte("\n"), 0644)
	packages := options["packages"].([]string)

	// build the docker image
	ioutil.WriteFile(filepath.Join(tmpDir, "pkg_list.txt"), []byte(strings.Join(packages, "\n")), 0644)
	ioutil.WriteFile(filepath.Join(tmpDir, "run_handler.py"), []byte(runHandlerCode), 0644)
	ioutil.WriteFile(filepath.Join(tmpDir, "pkg_base.Dockerfile"), []byte(packageBaseDockerfile), 0644)
	destInstallAll := filepath.Join(tmpDir, "install_all.py")
	srcInstallAll := filepath.Join(d.currentDir, "install_all.py")
	err = CopyFile(srcInstallAll, destInstallAll)
	if err != nil {
		return err
	}

	ctx := context.Background()
	buildOptions := types.ImageBuildOptions{
		Dockerfile: "pkg_base.Dockerfile",
		Tags:       []string{"package-base"},
	}
	tar, err := archive.TarWithOptions(tmpDir, &archive.TarOptions{})
	if err != nil {
		panic(err)
	}
	response, err := d.client.ImageBuild(ctx, tar, buildOptions)
	if err != nil {
		fmt.Println("Error building Docker image:", err)
		return err
	}
	defer response.Body.Close()

	// wait for the build to finish but discard the output
	err = printBuildLog(response)
	if err != nil {
		return err
	}

	// start evictor
	stopChan := make(chan struct{})
	d.evictorStopChan = stopChan
	go evictor(d, 1000, stopChan, 1024)

	return nil
}

func (d *DockerPlatform) KillWorker(options map[string]interface{}) error {
	// stop the evictor
	close(d.evictorStopChan)

	// save metrics
	csvPath := d.Config["csv_path"].(string)
	dir := filepath.Dir(csvPath)
	base := filepath.Base(csvPath)
	ext := filepath.Ext(base)
	util.GenerateUniqueFilename(dir, base[:len(base)-len(ext)], ext)
	err := platform_adapter.FlushToFile(d.metrics, csvPath)
	if err != nil {
		log.Printf("Error flushing metrics to file: %v", err)
	}

	// remove all the containers
	ctx := context.Background()
	for e := d.containers.Front(); e != nil; e = e.Next() {
		containerID := e.Value.(string)
		err := d.client.ContainerRemove(ctx, containerID, types.ContainerRemoveOptions{})
		if err != nil {
			log.Printf("Error removing container %s: %v", containerID, err)
		}
	}

	return nil
}

func (d *DockerPlatform) DeployFuncs(funcs []workload.Function) error {
	for _, f := range funcs {
		funcPath := filepath.Join(d.handlersDir, f.Name)
		if err := os.RemoveAll(funcPath); err != nil {
			return err
		}
		if err := os.MkdirAll(funcPath, 0755); err != nil {
			return err
		}

		// write the handler code
		handlerCodePath := filepath.Join(funcPath, "f.py")
		if err := ioutil.WriteFile(handlerCodePath,
			[]byte(strings.Join(f.Code, "\n")), 0644); err != nil {
			return err
		}
		// write run_handler.py
		if err := ioutil.WriteFile(filepath.Join(funcPath, "run_handler.py"),
			[]byte(runHandlerCode), 0644); err != nil {
			return err
		}
		// write requirements.txt
		if err := ioutil.WriteFile(filepath.Join(funcPath, "requirements.txt"),
			[]byte(f.Meta.RequirementsTxt), 0644); err != nil {
			return err
		}
	}
	return nil
}

func (d *DockerPlatform) InvokeFunc(funcName string, timeout int, options map[string]interface{}) error {
	jsonData, err := json.Marshal(options)

	ctx := context.Background()

	containerConfig := &container.Config{
		Image: "package-base",
		Cmd:   []string{"python3", "/app/run_handler.py", string(jsonData)},
	}
	hostConfig := &container.HostConfig{
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: filepath.Join(d.handlersDir, funcName),
				Target: "/app",
			},
		},
	}
	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig,
		nil, nil, "")

	if err != nil {
		log.Fatalf("Unable to create container: %v", err)
	}
	tCreate := util.GetCurrTime()

	containerID := resp.ID

	d.containersListLock.Lock()
	d.containers.PushBack(containerID)
	d.containersListLock.Unlock()

	// collect the memory usage before starting the container
	stopCh := make(chan struct{})
	memCh := make(chan int)
	go maxMemUsage(containerID, stopCh, memCh, 50)
	err = d.client.ContainerStart(ctx, containerID, types.ContainerStartOptions{})
	if err != nil {
		log.Fatalf("Unable to start container: %v", err)
	}
	tStart := util.GetCurrTime()

	// wait for the container to stop
	statusCh, errCh := d.client.ContainerWait(context.Background(), containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("Error waiting for container %s to stop: %v", containerID, err)
		}
	case status := <-statusCh:
		if status.StatusCode != 0 {
			errMessage := "nil or empty"
			if status.Error != nil {
				errMessage = status.Error.Message
			}
			log.Printf("Container %s stopped, Statuscode is %d, status.Error.Message is %s", containerID, status.StatusCode, errMessage)
		}
	}
	tReceived := util.GetCurrTime()

	// stop the memory usage collector
	close(stopCh)
	maxMem := <-memCh

	// get the output (logs)
	out, err := d.client.ContainerLogs(context.Background(), resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		panic(err)
	}
	defer out.Close()

	// parse the output
	var res map[string]interface{}

	outputBytes, err := ioutil.ReadAll(out)
	if err != nil {
		panic(err)
	}
	outputBytes = removeNonASCIIBytes(outputBytes)
	err = json.Unmarshal(outputBytes, &res)
	if err != nil {
		log.Printf("Error decoding the output in container %s, the output log is", containerID)
		outputString := string(outputBytes)
		fmt.Println(outputString)
	}

	// save metrics
	rec := LatencyRecord{
		Id:          res["invoke_id"].(string),
		Mem:         maxMem,
		Req:         res["req"].(float64),
		Create:      tCreate,
		Start:       tStart,
		StartImport: res["start_import"].(float64),
		EndImport:   res["end_import"].(float64),
		EndExec:     res["end_execute"].(float64),
		Failed:      toStrSlice(res["failed"].([]interface{})),
		Received:    tReceived,
	}
	d.metricsLock.Lock()
	d.metrics = append(d.metrics, &rec)
	fmt.Println("Metrics length:", len(d.metrics))
	d.metricsLock.Unlock()
	return nil
}

func (d *DockerPlatform) LoadConfig(config interface{}) error {
	err := d.BasePlatformAdapter.LoadConfig(config)
	d.currentDir = d.Config["docker_platform_dir"].(string)
	d.handlersDir = d.Config["handlers_dir"].(string)
	return err
}

func (d *DockerPlatform) GetStats() map[string]interface{} {
	return nil
}

func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	if err = dstFile.Sync(); err != nil {
		return err
	}

	return nil
}

func printBuildLog(response types.ImageBuildResponse) error {
	type buildResponseLine struct {
		Stream string `json:"stream"`
		Error  string `json:"error"`
	}

	reader := bufio.NewReader(response.Body)
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading Docker image build response:", err)
			return err
		}

		var respLine buildResponseLine
		if err := json.Unmarshal(line, &respLine); err != nil {
			fmt.Println("Error unmarshalling Docker image build response line:", err)
			continue
		}

		if respLine.Error != "" {
			fmt.Println("Error during Docker image build:", respLine.Error)
			return fmt.Errorf(respLine.Error)
		}

		if respLine.Stream != "" {
			fmt.Print(respLine.Stream)
		}
	}

	fmt.Println("Docker image built successfully.")
	return nil
}

func removeNonASCIIBytes(input []byte) []byte {
	filtered := make([]byte, 0, len(input))
	for _, b := range input {
		if b >= 0x20 && b <= 0x7E {
			filtered = append(filtered, b)
		}
	}
	return filtered
}

func toStrSlice(input []interface{}) []string {
	output := make([]string, len(input))
	for i, v := range input {
		output[i] = v.(string)
	}
	return output
}
