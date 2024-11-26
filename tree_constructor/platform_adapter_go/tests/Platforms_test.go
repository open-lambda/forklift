package tests

import (
	"fmt"
	"rb/request"
	"rb/util"
	"sort"
	"testing"
)

func TestDockerPlatform(t *testing.T) {
	wl, err := util.ReadWorkload("/root/ReqBench/filtered_workloads.json")
	wl.GenerateTrace(len(wl.Funcs), false, nil, 0)
	wl.AddMetrics([]string{"latency"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	pkgDict := wl.PkgWithVersion
	var pkgList []string
	for pkg, vers := range pkgDict {
		for _, ver := range vers {
			pkgList = append(pkgList, fmt.Sprintf("%s==%s", pkg, ver))
		}
	}
	sort.Strings(pkgList)

	config := "./platform_adapter/docker/config.json"
	tasks := 5
	timeout := 30
	totalTime := 0
	// docker need a package list to build the image
	startOptions := map[string]interface{}{
		"packages": pkgList,
	}

	opts := request.RunOptions{
		PlatformType: "docker",
		Workload:     &wl,
		Config:       config,
		Tasks:        tasks,
		Timeout:      timeout,
		TotalTime:    totalTime,
		StartOptions: startOptions,
		KillOptions:  nil,
	}
	_, err = request.AutoRun(opts)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}

func TestOlPlatform(t *testing.T) {
	wl, err := util.ReadWorkload("/root/ReqBench/filtered_workloads.json")
	wl.GenerateTrace(100, false, nil, 0)
	wl.AddMetrics([]string{"latency"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	config := "./platform_adapter/openlambda/config.json"
	tasks := 5
	timeout := 30
	totalTime := 0

	opts := request.RunOptions{
		PlatformType: "openlambda",
		Workload:     &wl,
		Config:       config,
		Tasks:        tasks,
		Timeout:      timeout,
		TotalTime:    totalTime,
		StartOptions: nil,
		KillOptions:  nil,
	}
	_, err = request.AutoRun(opts)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}

func TestAWS(t *testing.T) {
	wl, err := util.ReadWorkload("/root/ReqBench/workloads_5.json")
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	pkgDict := wl.PkgWithVersion
	var pkgList []string
	for pkg, vers := range pkgDict {
		for _, ver := range vers {
			pkgList = append(pkgList, fmt.Sprintf("%s==%s", pkg, ver))
		}
	}
	sort.Strings(pkgList)

	config := "./platform_adapter/docker/config.json"
	tasks := 1
	timeout := 2
	totalTime := 1

	startOptions := map[string]interface{}{
		"packages": pkgList,
	}

	opts := request.RunOptions{
		PlatformType: "aws",
		Workload:     &wl,
		Config:       config,
		Tasks:        tasks,
		Timeout:      timeout,
		TotalTime:    totalTime,
		StartOptions: startOptions,
		KillOptions:  nil,
	}
	_, err = request.AutoRun(opts)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}
