package openlambda

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	path2 "path"
	"path/filepath"
	"rb/platform_adapter"
	"rb/util"
	"rb/workload"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type LatencyRecord struct {
	Id               string   `json:"invoke_id"`
	SplitGen         int      `json:"split_gen"`
	Req              float64  `json:"req"`
	Received         float64  `json:"received"`
	StartCreate      float64  `json:"start_create"`
	EndCreate        float64  `json:"end_create"`
	StartPullHandler float64  `json:"start_pullHandler"`
	EndPullHandler   float64  `json:"end_pullHandler"`
	Unpause          float64  `json:"unpause"`
	StartImport      float64  `json:"start_import"`
	EndImport        float64  `json:"end_import"`
	StartExecute     float64  `json:"start_execute"`
	EndExecute       float64  `json:"end_execute"`
	ZygoteMiss       int      `json:"zygote_miss"`
	SbID             string   `json:"sb_id"`
	Failed           []string `json:"failed"`
}

func (record *LatencyRecord) ToSlice() []string {
	failedStr := "[]"
	if len(record.Failed) > 0 {
		failedStr = fmt.Sprintf("[%s]", strings.Join(record.Failed, ","))
	}
	return []string{
		record.Id,
		strconv.Itoa(record.SplitGen),
		strconv.FormatFloat(record.Req, 'f', 3, 64),
		strconv.FormatFloat(record.Received, 'f', 3, 64),
		strconv.FormatFloat(record.StartCreate, 'f', 3, 64),
		strconv.FormatFloat(record.EndCreate, 'f', 3, 64),
		strconv.FormatFloat(record.StartPullHandler, 'f', 3, 64),
		strconv.FormatFloat(record.EndPullHandler, 'f', 3, 64),
		strconv.FormatFloat(record.Unpause, 'f', 3, 64),
		strconv.FormatFloat(record.StartImport, 'f', 3, 64),
		strconv.FormatFloat(record.EndImport, 'f', 3, 64),
		strconv.FormatFloat(record.StartExecute, 'f', 3, 64),
		strconv.FormatFloat(record.EndExecute, 'f', 3, 64),
		strconv.Itoa(record.ZygoteMiss),
		record.SbID,
		failedStr,
	}
}

func (record *LatencyRecord) GetHeaders() []string {
	return []string{"invoke_id", "split_gen",
		"req", "received", "start_create", "end_create", "start_pullHandler", "end_pullHandler",
		"unpause", "start_import", "end_import", "start_execute", "end_execute",
		"zygote_miss", "sb_id", "failed"}
}

func (record *LatencyRecord) parseJSON(jsonData []byte) error {
	err := json.Unmarshal(jsonData, &record)
	if err != nil {
		return err
	}

	// Use reflection to ensure fields() and fieldByName() are available
	v := reflect.ValueOf(record).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)

		// Only check for nil values for pointers, slices, and maps
		if field.Kind() == reflect.Ptr || field.Kind() == reflect.Slice || field.Kind() == reflect.Map {
			if field.IsNil() {
				switch field.Kind() {
				case reflect.Int:
					field.SetInt(0)
				case reflect.Slice:
					field.Set(reflect.MakeSlice(field.Type(), 0, 0))
				}
			}
		}
	}
	return nil
}

type OpenLambda struct {
	platform_adapter.BasePlatformAdapter
	PID          int64
	warmupTime   float64
	warmupMemory uint64

	olDir          string
	runUrl         string
	saveMetrics    bool
	latencyRecords []platform_adapter.Record
	LatenciesMutex *sync.Mutex
	currentDir     string

	lockMonitor        *platform_adapter.LockStatMonitor
	lockMonitorErrChan chan error
	nsMonitor          *platform_adapter.BPFTracer

	// deprecated, please use nsMonitor
	ContainerMonitor        *ContainerMonitor
	containerMonitorErrChan chan error

	startConfig map[string]interface{}
	killConfig  map[string]interface{}
}

func (o *OpenLambda) StartWorker(options map[string]interface{}) error {
	if options == nil {
		startOptions, ok := o.Config["start_options"].(map[string]interface{})
		if !ok {
			startOptions = make(map[string]interface{})
		}
		options = startOptions
	}
	o.startConfig = options

	tmpFilePath := o.currentDir + "/tmp.csv"
	if _, err := os.Stat(tmpFilePath); err == nil {
		os.Remove(tmpFilePath)
	}

	var optParts []string
	for k, v := range o.startConfig {
		optParts = append(optParts, k+"="+fmt.Sprintf("%v", v))
	}
	optstr := strings.Join(optParts, ",")

	cgName := "ol"
	cgroupPath := "/sys/fs/cgroup/" + cgName
	if _, err := os.Stat(cgroupPath); err == nil {
		os.Remove(cgroupPath)
	}
	os.MkdirAll(cgroupPath, 0755)

	cmdArgs := []string{"cgexec", "-g", "memory,cpu:" + cgName, "./ol", "worker", "up", "-d"}
	if optstr != "" {
		cmdArgs = append(cmdArgs, "-o", optstr)
	}
	cmd := exec.Command("sudo", cmdArgs...)
	cmd.Dir = o.olDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("OL start error %v: %s", err, out)
	}

	output := string(out)

	if warmup, ok := o.startConfig["features.warmup"].(bool); ok && warmup {
		o.warmupMemory, _ = util.GetTotalMem(o.Config["cg_dir"].(string), "CG")
		o.warmupTime = extractWarmupTime(output)
	}

	if lockStatConf, exists := o.Config["profile_lock"]; exists {
		lockStatConf := lockStatConf.(map[string]interface{})
		monitor := platform_adapter.NewLockStatMonitor(
			lockStatConf["interval"].(float64),
			lockStatConf["output_path"].(string))
		o.lockMonitor = monitor
		monitor.StartMonitor(o.lockMonitorErrChan)
	}

	if nsMonitorConf, exists := o.Config["monitor_ns"]; exists {
		nsMonitorConf := nsMonitorConf.(map[string]interface{})
		configs, err := platform_adapter.ConfigFromMap(nsMonitorConf)
		if err == nil {
			tracer := platform_adapter.NewBPFTracer(configs)
			err := tracer.StartTracing()
			if err != nil {
				fmt.Println("Error starting BPF tracer: ", err)
			} else {
				o.nsMonitor = tracer
			}
		}
	}

	// todo: it is a deprecated feature, please use monitor_ns instead
	if containerMonitorConf, exists := o.Config["monitor_container"]; exists {
		containerMonitorConf := containerMonitorConf.(map[string]interface{})
		monitor := NewContainerMonitor(
			int(containerMonitorConf["port"].(float64)),
			containerMonitorConf["output_path"].(string))
		o.ContainerMonitor = monitor
		monitor.StartContainerMonitor(o.containerMonitorErrChan)

	}

	re := regexp.MustCompile(`PID: (\d+)`)
	match := re.FindStringSubmatch(output)
	if len(match) > 1 {
		pid, err := strconv.Atoi(match[1])
		if err != nil {

		}
		o.PID = int64(pid)
		fmt.Println("The OL PID is", pid)
		return nil
	} else {
		return fmt.Errorf("failed to parse PID from output: %s", output)
	}
}

func (o *OpenLambda) KillWorker(options map[string]interface{}) error {
	if options == nil {
		killOptions, ok := o.Config["kill_options"].(map[string]interface{})
		if !ok {
			killOptions = make(map[string]interface{})
		}
		options = killOptions
	}
	o.killConfig = options

	// kill worker
	// 1. stop monitors (if any)
	if o.lockMonitor != nil {
		o.lockMonitor.StopMonitor()
	}
	if o.nsMonitor != nil {
		o.nsMonitor.StopTracing()
	}
	if o.ContainerMonitor != nil {
		o.ContainerMonitor.StopContainerMonitor()
	}

	// 2. set stats & save metrics
	if warmup, ok := o.startConfig["features.warmup"].(bool); ok && warmup {
		o.Stats["warmup_time"] = o.warmupTime
		o.Stats["warmup_memory"] = o.warmupMemory
	}
	if saveMetrics, ok := o.killConfig["save_metrics"].(bool); ok && saveMetrics {
		metricsFilePath := o.killConfig["csv_path"].(string)
		if csvPath := o.killConfig["csv_name"]; csvPath != nil {
			metricsFilePath = csvPath.(string)
		}
		err := platform_adapter.FlushToFile(o.latencyRecords, metricsFilePath)
		if err != nil {
			return err
		}
	}

	// 3. kill worker
	if o.PID == 0 {
		fmt.Println("PID has not been set")
		return fmt.Errorf("PID has not been set")
	}

	cmd := exec.Command("./ol", "worker", "down")
	cmd.Dir = o.olDir
	_, err := cmd.Output()
	if err != nil {
		fmt.Println(err)
		fmt.Println("force kill")

		fmt.Printf("Killing process %d on port 5000\n", o.PID)
		killCmd := exec.Command("kill", "-9", fmt.Sprint(o.PID))
		killCmd.Dir = o.olDir
		killCmd.Run()

		cleanupCmd := exec.Command("./ol", "worker", "force-cleanup")
		cleanupCmd.Dir = o.olDir
		cleanupCmd.Stdout = nil
		cleanupCmd.Stderr = nil
		cleanupCmd.Run()

		upCmd := exec.Command("./ol", "worker", "up")
		upCmd.Dir = o.olDir
		if err := upCmd.Start(); err != nil {
			fmt.Println("Failed to start the command:", err)
		} else {
			// Send SIGINT to the process
			if err := upCmd.Process.Signal(syscall.SIGINT); err != nil {
				fmt.Println("Failed to send SIGINT:", err)
			}
		}

		cleanupCmd2 := exec.Command("./ol", "worker", "force-cleanup")
		cleanupCmd2.Dir = o.olDir
		cleanupCmd2.Stdout = nil
		cleanupCmd2.Stderr = nil
		cleanupCmd2.Run()
		fmt.Printf("force kill done\n")
		return nil
	} else {
		return nil
	}
}

func (o *OpenLambda) DeployFuncs(funcs []workload.Function) error {
	deployChan := make(chan workload.Function, 64)
	errChan := make(chan error)
	for i := 0; i < 8; i++ {
		go o.deployFunction(deployChan, errChan)
	}
	for _, f := range funcs {
		select {
		case deployChan <- f:
		case err := <-errChan:
			return err
		}
	}
	close(deployChan)
	close(errChan)
	return nil
}

func (o *OpenLambda) deployFunction(deployTask chan workload.Function, errChan chan error) {
	for {
		f, ok := <-deployTask
		if !ok {
			return
		}
		// write code to registry dir
		meta := f.Meta
		path := path2.Join(o.olDir, "/default-ol/registry/", f.Name)
		if os.IsExist(os.MkdirAll(path, 0777)) {
			err := os.RemoveAll(path)
			if err != nil {
				panic(err)
			}
			err = os.MkdirAll(path, 0777)
			if err != nil {
				panic(err)
			}
		}

		_lines := f.Code
		var lines []string
		for _, line := range _lines {
			lines = append(lines, line)
		}
		code := strings.Join(lines, "\n")

		funcPath := filepath.Join(path, "f.py")
		requirementsInPath := filepath.Join(path, "requirements.in")
		requirementsTxtPath := filepath.Join(path, "requirements.txt")

		if err := ioutil.WriteFile(funcPath, []byte(code), 0777); err != nil {
			errChan <- err
		}
		if err := ioutil.WriteFile(requirementsInPath, []byte(meta.RequirementsIn), 0777); err != nil {
			errChan <- err
		}
		if err := ioutil.WriteFile(requirementsTxtPath, []byte(meta.RequirementsTxt), 0777); err != nil {
			errChan <- err
		}
	}
}

func (o *OpenLambda) InvokeFunc(funcName string, timeout int, options map[string]interface{}) error {
	// invoke function
	url := o.runUrl + funcName
	var resp *http.Response
	var err error

	jsonData, err := json.Marshal(options)
	if err != nil {
		err := fmt.Errorf("InvokeFunc: failed to marshal options: %v", err)
		return err
	}
	client := &http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}
	resp, err = client.Post(url, "text/json", bytes.NewBuffer(jsonData))
	if err != nil {
		err := fmt.Errorf("InvokeFunc: failed to post to %s: %v", url, err)
		return err
	}
	defer resp.Body.Close()
	tRecv := util.GetCurrTime()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err := fmt.Errorf("InvokeFunc: failed to read response body: %v", err)
		return err
	}

	if o.saveMetrics {
		var record LatencyRecord
		err = record.parseJSON(body)
		if err != nil {
			err := fmt.Errorf("InvokeFunc: failed to parse latency record: %v", err)
			return err
		}
		record.Received = tRecv
		o.LatenciesMutex.Lock()
		o.latencyRecords = append(o.latencyRecords, &record)
		o.LatenciesMutex.Unlock()
	}
	return nil
}

func (o *OpenLambda) LoadConfig(config interface{}) error {
	o.BasePlatformAdapter.LoadConfig(config)
	o.killConfig = getOrDefault(o.Config, "kill_options", map[string]interface{}{}).(map[string]interface{})
	o.startConfig = getOrDefault(o.Config, "start_options", map[string]interface{}{}).(map[string]interface{})
	o.olDir = getOrDefault(o.Config, "ol_dir", "/root/open-lambda").(string)
	o.runUrl = getOrDefault(o.Config, "run_url", "http://localhost:5000/run/").(string)
	o.currentDir = getOrDefault(o.Config, "current_dir", ".").(string)
	o.saveMetrics = getOrDefault(o.killConfig, "save_metrics", false).(bool)
	return nil
}

func getOrDefault(m map[string]interface{}, key string, defaultValue interface{}) interface{} {
	if val, ok := m[key]; ok {
		return val
	}
	return defaultValue
}

func NewOpenLambda() (*OpenLambda, error) {
	return &OpenLambda{
		BasePlatformAdapter: platform_adapter.BasePlatformAdapter{
			Stats: make(map[string]interface{}),
		},
		LatenciesMutex:          &sync.Mutex{},
		lockMonitorErrChan:      make(chan error),
		containerMonitorErrChan: make(chan error),
	}, nil
}

func extractWarmupTime(out string) float64 {
	logFilePathRegex := regexp.MustCompile(`Log File: (.+\.out)`)
	matches := logFilePathRegex.FindStringSubmatch(out)
	var logFilePath string
	if len(matches) > 1 {
		logFilePath = matches[1]
	}

	if logFilePath != "" {
		fileContent, err := ioutil.ReadFile(logFilePath)
		if err != nil {
			fmt.Printf("Error reading file: %s\n", err)
			return 0
		}

		warmupTimeRegex := regexp.MustCompile(`warmup time is (\d+(\.\d+)?) ms`)
		warmupMatches := warmupTimeRegex.FindStringSubmatch(string(fileContent))
		if len(warmupMatches) > 1 {
			var warmupTime float64
			fmt.Sscanf(warmupMatches[1], "%f", &warmupTime)
			return warmupTime
		}
	}
	return 0
}
