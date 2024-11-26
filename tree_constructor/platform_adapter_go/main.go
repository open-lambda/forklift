package main

import (
	"encoding/json"
	"fmt"
	"os"
	"rb/request"
	"rb/util"
	"strconv"
)

// start OL from CMD
func main() {
	if len(os.Args) < 7 {
		panic("Not enough arguments")
	}
	PlatformType := os.Args[1]

	wl, err := util.ReadWorkload(os.Args[2])
	if err != nil {
		panic(err)
	}

	configArg := os.Args[3]
	var configMap map[string]interface{}
	err = json.Unmarshal([]byte(configArg), &configMap)
	var config interface{}
	if err == nil {
		config = configMap
	} else {
		config = configArg
	}

	tasks, err := strconv.Atoi(os.Args[4])
	if err != nil {
		panic(err)
	}

	timeout, err := strconv.Atoi(os.Args[5])
	if err != nil {
		panic(err)
	}

	totalTime, err := strconv.Atoi(os.Args[6])
	if err != nil {
		panic(err)
	}

	opts := request.RunOptions{
		PlatformType: PlatformType,
		Workload:     &wl,
		Config:       config,
		Tasks:        tasks,
		Timeout:      timeout,
		TotalTime:    totalTime,
		StartOptions: nil,
		KillOptions:  nil,
	}

	stats, err := request.AutoRun(opts)
	if err != nil {
		panic(err)
	}

	statsJsonStr, err := json.Marshal(stats)
	if err != nil {
		panic(err)
	}
	fmt.Println("stats:", string(statsJsonStr))
}
