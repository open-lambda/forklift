package openlambda

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"rb/platform_adapter"
	"rb/util"
	"strconv"
)

type ContainerMonitor struct {
	port      int    // default port should be 4998
	path      string // where the records will be stored
	forkRecs  []platform_adapter.Record
	startRecs []platform_adapter.Record
	server    *http.Server
}

type forkRec struct {
	ForkSt float64 `json:"fork_st"`
	Chroot float64 `json:"chroot"`
	MvCg   float64 `json:"mv_cg"`
	End    float64 `json:"end"`
}

type startRec struct {
	Unshare float64 `json:"unshare"`
	Fork    float64 `json:"fork"`
	End     float64 `json:"end"`
}

func (forkRec *forkRec) GetHeaders() []string {
	return []string{"fork_st", "chroot", "mv_cg", "end"}
}

func (startRec *startRec) GetHeaders() []string {
	return []string{"unshare", "fork", "end"}
}

func (forkRec *forkRec) ToSlice() []string {
	return []string{
		strconv.FormatFloat(forkRec.ForkSt, 'f', 3, 64),
		strconv.FormatFloat(forkRec.Chroot, 'f', 3, 64),
		strconv.FormatFloat(forkRec.MvCg, 'f', 3, 64),
		strconv.FormatFloat(forkRec.End, 'f', 3, 64),
	}
}

func (startRec *startRec) ToSlice() []string {
	return []string{
		strconv.FormatFloat(startRec.Unshare, 'f', 3, 64),
		strconv.FormatFloat(startRec.Fork, 'f', 3, 64),
		strconv.FormatFloat(startRec.End, 'f', 3, 64),
	}
}

func (contM *ContainerMonitor) StartContainerMonitor(errCh chan error) {
	go func() {
		err := contM._startContainerMonitor()
		if err != nil {
			err = fmt.Errorf("error starting container monitor: %s", err)
			errCh <- err
		}
	}()
}

// listen on localhost:port
func (contM *ContainerMonitor) _startContainerMonitor() error {
	contM.server = &http.Server{Addr: fmt.Sprintf(":%d", contM.port)}
	http.HandleFunc("/fork", contM.forkHandler)
	http.HandleFunc("/start", contM.startHandler)

	err := contM.server.ListenAndServe()
	if err != nil {
		return err
	}
	return nil
}

func (contM *ContainerMonitor) forkHandler(w http.ResponseWriter, r *http.Request) {
	// parse the request
	rec := forkRec{}
	err := json.NewDecoder(r.Body).Decode(&rec)
	if err != nil {
		fmt.Println("Error decoding fork record: ", err)
	}
	// add the record to the list
	contM.forkRecs = append(contM.forkRecs, &rec)

	return
}

func (contM *ContainerMonitor) startHandler(w http.ResponseWriter, r *http.Request) {
	// parse the request
	rec := startRec{}
	err := json.NewDecoder(r.Body).Decode(&rec)
	if err != nil {
		fmt.Println("Error decoding start record: ", err)
	}
	// add the record to the list
	contM.startRecs = append(contM.startRecs, &rec)

	return
}

// stop listening and save the records
func (contM *ContainerMonitor) StopContainerMonitor() {
	if contM.server != nil {
		if err := contM.server.Shutdown(context.Background()); err != nil {
			fmt.Printf("HTTP server Shutdown: %v", err)
		}
	}

	forkPath := util.GenerateUniqueFilename(contM.path, "fork", ".csv")
	startPath := util.GenerateUniqueFilename(contM.path, "start", ".csv")
	err := platform_adapter.FlushToFile(contM.forkRecs, forkPath)
	if err != nil {
		fmt.Println("Error flushing fork records to file: ", err)
		return
	}
	err = platform_adapter.FlushToFile(contM.startRecs, startPath)
	if err != nil {
		fmt.Println("Error flushing start records to file: ", err)
		return
	}
	return
}

func NewContainerMonitor(port int, path string) *ContainerMonitor {
	return &ContainerMonitor{
		port: port,
		path: path,
	}
}
