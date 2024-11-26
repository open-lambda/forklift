package platform_adapter

import (
	"encoding/json"
	"errors"
	"os"
	"rb/workload"
	"strings"
)

type PlatformAdapter interface {
	StartWorker(options map[string]interface{}) error
	KillWorker(options map[string]interface{}) error
	DeployFuncs(funcs []workload.Function) error
	InvokeFunc(funcName string, timeout int, options map[string]interface{}) error
	LoadConfig(config interface{}) error
	GetStats() map[string]interface{}
}

type BasePlatformAdapter struct {
	Config map[string]interface{}

	// after each run, there might be some stats collected by the platform
	// this map help to store those stats and can be easily accessed by the caller
	Stats map[string]interface{}
}

// if config is a string, it is treated as a path to a json file; if it is a map, it is treated as a json object
func (c *BasePlatformAdapter) LoadConfig(config interface{}) error {
	switch v := config.(type) {
	case string:
		file, err := os.Open(v)
		if err != nil {
			return err
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		err = decoder.Decode(&c.Config)
		if err != nil {
			return err
		}
	case map[string]interface{}:
		c.Config = v
	default:
		return errors.New("unsupported config type")
	}
	return nil
}

func (c *BasePlatformAdapter) GetStats() map[string]interface{} {
	return c.Stats
}

type Record interface {
	GetHeaders() []string
	ToSlice() []string
}

func FlushToFile(records []Record, filePath string) error {
	if len(records) == 0 {
		return nil
	}

	// Check if the file exists
	_, err := os.Stat(filePath)
	fileExists := !os.IsNotExist(err)

	// Open the file for appending, creating it if it does not exist
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write headers if the file did not exist
	if !fileExists {
		headers := records[0].GetHeaders() // Assuming records slice is not empty
		_, err := file.WriteString(strings.Join(headers, ",") + "\n")
		if err != nil {
			return err
		}
	}

	// Write each record to the file
	for _, record := range records {
		data := record.ToSlice()
		_, err := file.WriteString(strings.Join(data, ",") + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}
