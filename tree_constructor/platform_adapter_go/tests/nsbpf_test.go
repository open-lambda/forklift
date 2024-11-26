package tests

import (
	"rb/platform_adapter"
	"rb/request"
	"rb/util"
	"testing"
)

//{platform_adapter.PutIPCScript, "./putIPC.csv"},
//{platform_adapter.IpcScript, "./ipc.csv"},
//{platform_adapter.PidScript, "./pid.csv"},
//{platform_adapter.UtsScript, "./uts.csv"},
//{platform_adapter.NewNsScript, "./newns.csv"},
//{platform_adapter.MqCreateScript, "./mqcreate.csv"},

func TestBPFNsTracer(t *testing.T) {
	// trace config
	breakIPCConfig := platform_adapter.TracerConfig{
		Script: platform_adapter.NewNsScript,
		Output: "./newns.csv",
	}

	tracer := platform_adapter.NewBPFTracer(
		[]platform_adapter.TracerConfig{
			breakIPCConfig,
		},
	)
	err := tracer.StartTracing()
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	// start ol, run some workloads
	wl, err := util.ReadWorkload("/root/ReqBench/filtered_workloads.json")
	wl.GenerateTrace(200, false, nil, 0)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	config := map[string]interface{}{
		"timeout": 10,
		"ol_dir":  "/root/open-lambda/",
		"run_url": "http://localhost:5000/run/",
		"cg_dir":  "/sys/fs/cgroup/default-ol-sandboxes",
		"start_options": map[string]interface{}{
			"features.warmup":   true,
			"import_cache_tree": "",
			"limits.mem_mb":     500,
		},
	}

	tasks := 5
	timeout := 30
	totalTime := 120
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

	err = tracer.StopTracing()
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}
