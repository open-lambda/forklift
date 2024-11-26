package platform_adapter

import (
	"fmt"
	"github.com/shirou/gopsutil/cpu"
	"io/ioutil"
	"path"
	"sync"
	"time"
)

type LockStatMonitor struct {
	index     int
	Intervals float64
	FilePath  string
	cpuUsages []float64
	startTime time.Time
	endTime   time.Time
	lock      sync.Mutex
	stopChan  chan struct{}
}

func NewLockStatMonitor(intervals float64, filePath string) *LockStatMonitor {
	return &LockStatMonitor{
		index:     0,
		Intervals: intervals,
		FilePath:  filePath,
		cpuUsages: make([]float64, 0),
		stopChan:  make(chan struct{}),
	}
}

func (ls *LockStatMonitor) clearLockStat() error {
	err := ioutil.WriteFile("/proc/lock_stat", []byte("0"), 0644)
	if err != nil {
		return err
	}
	return nil
}

func (ls *LockStatMonitor) StartMonitor(errCh chan error) {
	var err error
	go func() {
		err = ls._startMonitor()
		if err != nil {
			err = fmt.Errorf("error monitoring lock: %s", err)
			errCh <- err
		}
	}()
}

func (ls *LockStatMonitor) _startMonitor() error {
	err := ls.clearLockStat()
	if err != nil {
		fmt.Printf("StartMonitor failed, Error clearLockStat: %s\n", err)
		return err
	}
	ls.startTime = time.Now()

	cpu.Percent(0, false)

	ticker := time.NewTicker(time.Duration(ls.Intervals * float64(time.Second)))
	defer ticker.Stop()

	// if ls.Intervals <= 0, then only collect lock stat once
	if ls.Intervals <= 0 {
		return nil
	}
	for {
		select {
		case <-ticker.C:
			cpuUsage, _ := cpu.Percent(0, false)
			ls.cpuUsages = append(ls.cpuUsages, cpuUsage[0])
			go func() {
				output := ls.readLockStat()
				ls.writeToFile(output)
			}()
		case <-ls.stopChan:
			return nil
		}
	}
}

func (ls *LockStatMonitor) StopMonitor() {
	ls.clearLockStat()
	ls.endTime = time.Now()
	ls.stopChan <- struct{}{}

	// collect last cpu usage and write to file
	if cpuUsage, err := cpu.Percent(0, false); err == nil && len(cpuUsage) > 0 {
		ls.cpuUsages = append(ls.cpuUsages, cpuUsage[0])
	}
	cpuUsageFilePath := path.Join(ls.FilePath, "cpu_usages.txt")
	cpuUsagesData := fmt.Sprintf("%v", ls.cpuUsages)
	if err := ioutil.WriteFile(cpuUsageFilePath, []byte(cpuUsagesData), 0644); err != nil {
		fmt.Printf("Error writing CPU usages to file: %s\n", err)
	}

	output := ls.readLockStat()
	ls.writeToFile(output)

	ls.index = 0
	ls.cpuUsages = make([]float64, 0)
}

func (ls *LockStatMonitor) readLockStat() string {
	data, err := ioutil.ReadFile("/proc/lock_stat")
	if err != nil {
		fmt.Printf("Error reading /proc/lock_stat: %s\n", err)
		return ""
	}
	return string(data)
}

func (ls *LockStatMonitor) writeToFile(output string) {
	ls.lock.Lock()
	defer ls.lock.Unlock()

	fileName := fmt.Sprintf("lockstat_%d.txt", ls.index)
	ls.index++
	if err := ioutil.WriteFile(path.Join(ls.FilePath, fileName), []byte(output), 0644); err != nil {
		fmt.Printf("Error writing to file: %s\n", err)
	}
}
