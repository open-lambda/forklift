package workload

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

type Workload struct {
	Funcs          []Function          `json:"funcs"`
	Calls          []Call              `json:"calls"`
	PkgWithVersion map[string][]string `json:"pkg_with_version"`

	fnIndex  int
	emptyPkg map[string]bool // emptyPkg indicates whether a function's pkg list is empty
}

type Call struct {
	Name string `json:"name"`
}

type Function struct {
	Name string   `json:"name"`
	Meta Meta     `json:"meta"`
	Code []string `json:"code"`
}

type Meta struct {
	RequirementsIn  string   `json:"requirements_in"`
	RequirementsTxt string   `json:"requirements_txt"`
	ImportMods      []string `json:"import_mods"`
}

func generateNonMeasureCodeLines(modules []string, returnVal string) []string {
	lines := []string{
		"import time, importlib, os",
		"os.environ['OPENBLAS_NUM_THREADS'] = '2'",
		fmt.Sprintf("for mod in %s:", formatPythonList(modules)),
		"    try:",
		"        importlib.import_module(mod)",
		"    except Exception as e:",
		"        pass",
		fmt.Sprintf("def f(event):"),
		fmt.Sprintf("    return \"%s\"", returnVal),
	}
	return lines
}

func genMeasureCode(modules []string, measureLatency bool, measureMem bool) []string {
	lines := []string{
		"import time, importlib, os",
		"os.environ['OPENBLAS_NUM_THREADS'] = '2'",
		"called = False",
		"split_gen = -1",
	}
	if measureMem {
		lines = append(lines,
			"import tracemalloc, gc, sys, json",
			"gc.collect()",
			"tracemalloc.start()",
		)
	}
	if measureLatency {
		lines = append(lines, "t_StartImport = time.time()*1000")
	}
	lines = append(lines,
		"failed = []",
		fmt.Sprintf("for mod in %s:", formatPythonList(modules)),
		"    try:",
		"        importlib.import_module(mod)",
		"    except Exception as e:",
		"        failed.append(mod)",
		"        pass",
	)
	if measureLatency {
		lines = append(lines, "t_EndImport = time.time()*1000")
	}
	lines = append(lines,
		"def f(event):",
		"    global t_StartImport, t_EndImport, t_EndExecute, failed",
		"    time_start = time.time()*1000",
	)
	if measureLatency {
		lines = append(lines,
			"    t_EndExecute = time.time()*1000",
			"    event['start_import'] = t_StartImport",
			"    event['end_import'] = t_EndImport",
			"    event['start_execute'] = time_start",
			"    event['end_execute'] = t_EndExecute",
			"    event['failed'] = failed",
		)
	}
	if measureMem {
		lines = append(lines,
			"    mb = (tracemalloc.get_traced_memory()[0] - tracemalloc.get_traced_memory()[1]) / 1024 / 1024",
			"    event['memory_usage_mb'] = mb",
		)
	}
	lines = append(lines, "    return event")
	return lines
}

// formatPythonList convert go slice to python list
func formatPythonList(list []string) string {
	var quoted []string
	for _, item := range list {
		quoted = append(quoted, fmt.Sprintf("'%s'", item))
	}
	return fmt.Sprintf("[%s]", strings.Join(quoted, ", "))
}

func (wl *Workload) AddMetrics(metrics []string) {
	generateLatency := contains(metrics, "latency")
	generateMem := contains(metrics, "memory")

	for i := range wl.Funcs {
		wl.Funcs[i].Code = genMeasureCode(wl.Funcs[i].Meta.ImportMods, generateLatency, generateMem)
	}
}

func (wl *Workload) ShuffleCalls() {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(wl.Calls), func(i, j int) { wl.Calls[i], wl.Calls[j] = wl.Calls[j], wl.Calls[i] })
}

func (wl *Workload) GenerateTrace(target int, skew bool, weights []float64, s float64) {
	wl.Calls = []Call{}

	functionNames := make([]string, len(wl.Funcs))
	for i, f := range wl.Funcs {
		functionNames[i] = f.Name
	}

	rand.Seed(time.Now().UnixNano())

	if !skew {
		if target <= len(functionNames) {
			for _, name := range RandomSample(functionNames, target) {
				wl.addCall(name)
			}
		} else {
			for _, name := range RandomChoices(functionNames, nil, target) {
				wl.addCall(name)
			}
		}
		return
	}

	if weights != nil {
		for _, name := range RandomChoices(functionNames, weights, target) {
			wl.addCall(name)
		}
	} else {
		numFuncs := len(functionNames)
		for _, idx := range Zipf(numFuncs, target, s) {
			wl.addCall(functionNames[idx])
		}
	}
}

func (wl *Workload) RandomSplit(ratio float64) (*Workload, *Workload) {
	wlTrain := Workload{}
	wlTest := Workload{}
	wlTrainAdded := make(map[string]bool)
	wlTestAdded := make(map[string]bool)

	wl.ShuffleCalls()

	trainSize := int(float64(len(wl.Calls)) * ratio)
	wlTrain.Calls = wl.Calls[:trainSize]
	wlTest.Calls = wl.Calls[trainSize:]
	for _, call := range wlTrain.Calls {
		f := wl.getFunction(call.Name)
		if _, exists := wlTrainAdded[call.Name]; !exists {
			wlTrain.addFunction(f.Meta, f.Code)
		}
		wlTrain.addCall(call.Name)
		wlTrainAdded[call.Name] = true
	}

	for _, call := range wlTest.Calls {
		f := wl.getFunction(call.Name)
		if _, exists := wlTestAdded[call.Name]; !exists {
			wlTest.addFunction(f.Meta, f.Code)
		}
		wlTest.addCall(call.Name)
		wlTestAdded[call.Name] = true
	}
	return &wlTrain, &wlTest
}

func (wl *Workload) GetEmptyPkgCallsCnt() int {
	cnt := 0
	for _, call := range wl.Calls {
		if wl.emptyPkg[call.Name] {
			cnt++
		}
	}
	return cnt
}

func (wl *Workload) getFunction(name string) Function {
	for _, f := range wl.Funcs {
		if f.Name == name {
			return f
		}
	}
	return Function{}
}

func (wl *Workload) addFunction(meta Meta, code []string) {
	name := fmt.Sprintf("fn%d", wl.fnIndex)
	wl.fnIndex++
	wl.Funcs = append(wl.Funcs, Function{Name: name, Meta: meta, Code: code})
}

func (wl *Workload) addCall(name string) {
	wl.Calls = append(wl.Calls, Call{Name: name})
}

func (wl *Workload) SaveToJson(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(wl)
	if err != nil {
		return err
	}

	return nil
}

func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func RandomSample(array []string, n int) []string {
	rand.Seed(time.Now().UnixNano())

	if n >= len(array) {
		return array
	}

	temp := make([]string, len(array))
	copy(temp, array)

	result := make([]string, n)
	for i := 0; i < n; i++ {
		index := rand.Intn(len(temp))
		result[i] = temp[index]

		temp = append(temp[:index], temp[index+1:]...)
	}

	return result
}

func RandomChoices(array []string, weights []float64, k int) []string {
	if weights == nil || len(weights) != len(array) {
		weights = make([]float64, len(array))
		for i := range weights {
			weights[i] = 1.0
		}
	}
	rand.Seed(time.Now().UnixNano())

	if len(array) != len(weights) {
		panic("array and weights must be of the same length")
	}

	totalWeight := 0.0
	for _, weight := range weights {
		totalWeight += weight
	}

	results := make([]string, k)
	for i := 0; i < k; i++ {
		r := rand.Float64() * totalWeight
		for j, weight := range weights {
			r -= weight
			if r <= 0 {
				results[i] = array[j]
				break
			}
		}
	}

	return results
}

func Zipf(numFuncs, target int, s float64) []int {
	rand.Seed(time.Now().UnixNano())
	// range is [1, numFuncs]
	zipf := rand.NewZipf(rand.New(rand.NewSource(rand.Int63())), s, 1, uint64(numFuncs))

	samples := make([]int, target)
	for i := 0; i < target; i++ {
		samples[i] = int(zipf.Uint64())
	}

	return samples
}
