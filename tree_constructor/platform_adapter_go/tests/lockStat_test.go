package tests

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"rb/platform_adapter"
	"strings"
	"testing"
	"time"
)

func TestLockStatMonitor(t *testing.T) {
	intervals := 1.0
	dur := 10.1

	tempDir, err := os.MkdirTemp("", "lockStatTest")
	if err != nil {
		return
	}

	monitor := platform_adapter.NewLockStatMonitor(intervals, tempDir)

	go func() {
		err := monitor._startMonitor()
		if err != nil {
			t.Fatalf("StartMonitor error: %v", err)
		}
	}()

	time.Sleep(time.Duration(dur) * time.Second)

	monitor.StopMonitor()

	// Check 1: How many lockStat files are created
	files, err := ioutil.ReadDir(tempDir)
	cnt := 0
	if err != nil {
		t.Fatalf("Failed to read temp directory: %v", err)
	}

	// Check 2: If the lockStat file is not empty
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "lockstat_") {
			cnt++
			data, err := ioutil.ReadFile(filepath.Join(tempDir, file.Name()))
			if err != nil {
				t.Fatalf("Failed to read file %s: %v", file.Name(), err)
			}
			if len(data) == 0 {
				t.Errorf("File %s is empty", file.Name())
			}
		}
	}
	if cnt < int(dur/intervals)-1 || cnt > int(dur/intervals)+1 {
		t.Errorf("Expected %d lockStat files, got %d", int(dur/intervals), cnt)
	}

	// Check 3: If cpuUsages are collected
	cpuUsageFilePath := filepath.Join(tempDir, "cpu_usages.txt")
	if _, err := os.Stat(cpuUsageFilePath); os.IsNotExist(err) {
		t.Errorf("cpuUsages file %s does not exist", cpuUsageFilePath)
	}

	os.Remove(tempDir)
}
