package aws

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	log_types "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambda_types "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	docker_types "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/archive"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"rb/platform_adapter"
	"rb/util"
	"rb/workload"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	AWS_HANDLER = `import time, importlib, os, sys
os.environ['OPENBLAS_NUM_THREADS'] = '2'
for req in %s:
    sys.path.insert(0, f'/packages/{req}')
start = time.time()
for mod in %s:
    try:
        importlib.import_module(mod)
    except Exception as e:
        pass
end = time.time()
def handler(event, context):
    return (end-start)*1000`

	DOCKERFILE = `FROM python:3.10 as build-image
	COPY install_all.py /install_all.py
	COPY pkg_list.txt /pkg_list.txt
	RUN python3 /install_all.py /pkg_list.txt
	RUN pip install awslambdaric
	COPY . .
	RUN mkdir -p /tmp/.cache
	ENTRYPOINT ["/usr/local/bin/python", "-m", "awslambdaric"]`
)

func writeFile(path string, content string) error {
	if _, err := os.Stat(path); err == nil {
		existingContent, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		if string(existingContent) == content {
			return nil
		}
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	if err != nil {
		return err
	}

	return nil
}

type AWSPlatform struct {
	platform_adapter.BasePlatformAdapter
	Workload_path string
	lambdaClient  *lambda.Client
	logClient     *cloudwatchlogs.Client
	//logClient
	functions    []string
	imageName    string
	region       string
	executionArn string
	tmpPath      string
	metricLock   sync.Mutex
	metrics      []LatencyRecord
}

type LatencyRecord struct {
	RequestId      string  `json:"request_id"`
	FuncName       string  `json:"function_name"`
	Duration       float64 `json:"duration(ms)"`
	BilledDuration float64 `json:"billed_duration(ms)"`
	MemorySize     float64 `json:"memory_size(mb)"`
	MaxMemoryUsed  float64 `json:"max_memory_used(mb)"`
	InitDuration   float64 `json:"init_duration(ms)"`
	ImportDuration float64 `json:"import_duration(ms)"`
}

func (record *LatencyRecord) getHeaders() []string {
	return []string{"RequestId", "FuncName", "Duration(ms)", "BilledDuration(ms)", "MemorySize(mb)", "MaxMemoryUsed(mb)", "InitDuration(ms)", "ImportDuration(ms)"}
}

func (record *LatencyRecord) toSlice() []string {
	return []string{
		record.RequestId,
		record.FuncName,
		fmt.Sprintf("%.3f", record.Duration),
		fmt.Sprintf("%.3f", record.BilledDuration),
		fmt.Sprintf("%.3f", record.MemorySize),
		fmt.Sprintf("%.3f", record.MaxMemoryUsed),
		fmt.Sprintf("%.3f", record.InitDuration),
		fmt.Sprintf("%.3f", record.ImportDuration),
	}
}

func flushToFile(records []LatencyRecord, filePath string) error {
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
		headers := (&LatencyRecord{}).getHeaders() // Assuming records slice is not empty
		_, err := file.WriteString(strings.Join(headers, ",") + "\n")
		if err != nil {
			return err
		}
	}

	// Write each record to the file
	for _, record := range records {
		data := record.toSlice()
		_, err := file.WriteString(strings.Join(data, ",") + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *AWSPlatform) listFunctions(maxItems int) ([]string, error) {

	var functions []string

	paginator := lambda.NewListFunctionsPaginator(a.lambdaClient, &lambda.ListFunctionsInput{
		MaxItems: aws.Int32(int32(maxItems)),
	})

	for paginator.HasMorePages() && len(functions) < maxItems {
		pageOutput, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		if len(pageOutput.Functions) == 0 {
			break
		}

		for _, f := range pageOutput.Functions {
			functions = append(functions, *f.FunctionName)
		}
	}

	return functions, nil
}

func (a *AWSPlatform) buildImage() error {
	err := os.MkdirAll(a.tmpPath, 0755)
	if err != nil {
		return err
	}

	wk, err := util.ReadWorkload(a.Workload_path)
	if err != nil {
		return err
	}

	//generate list of all packages used for the bench
	// Extract keys into a slice
	keys := make([]string, 0, len(wk.PkgWithVersion))
	for key := range wk.PkgWithVersion {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	pkg_list := ""
	for _, pkg := range keys {
		for _, ver := range wk.PkgWithVersion[pkg] {
			pkg_list = fmt.Sprintf("%s%s==%s\n", pkg_list, pkg, ver)
		}
	}
	writeFile(a.tmpPath+"/pkg_list.txt", pkg_list)

	//write lambda handler function
	for _, f := range wk.Funcs {
		regex := regexp.MustCompile(`([a-zA-Z0-9_-]+\s*==\s*[0-9.]+)`)
		matches := regex.FindAllString(f.Meta.RequirementsTxt, -1)

		deps := "["
		for _, m := range matches {
			deps = fmt.Sprintf("%s%q,", deps, m)
		}
		deps = deps[:len(deps)-1] + "]"

		imports := "["
		for _, m := range f.Meta.ImportMods {
			imports = fmt.Sprintf("%s%q,", imports, m)
		}
		imports = imports[:len(imports)-1] + "]"
		handler_code := fmt.Sprintf(AWS_HANDLER, deps, imports)

		writeFile(fmt.Sprintf(a.tmpPath+"/%s.py", f.Name), handler_code)
	}

	//copy install_all.py to .tmp
	cmd := exec.Command("cp", "-n", "/root/ReqBench/src/platform_adapter/docker/install_all.py", a.tmpPath+"/install_all.py")

	err = cmd.Run()
	if err != nil {
		fmt.Println("here")
		return err
	}

	//write Dockerfile
	writeFile(a.tmpPath+"/Dockerfile", DOCKERFILE)

	//authenticate docker to ecr
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(a.region))
	if err != nil {
		return err
	}
	ecr_client := ecr.NewFromConfig(cfg)

	auth_response, err := ecr_client.GetAuthorizationToken(context.TODO(), nil)
	if err != nil {
		return err
	}

	token := *auth_response.AuthorizationData[0].AuthorizationToken
	decodedBytes, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return err
	}

	parts := strings.Split(string(decodedBytes), ":")
	username, password := parts[0], parts[1]

	sts_client := sts.NewFromConfig(cfg)
	identity_response, err := sts_client.GetCallerIdentity(context.TODO(), nil)
	account_id := *identity_response.Account
	server_url := fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com", account_id, a.region)

	//create ecr repo
	repo_name, repo_url := "reqbench", ""
	repo_response, err := ecr_client.CreateRepository(context.TODO(), &ecr.CreateRepositoryInput{
		RepositoryName: &repo_name,
	})
	if err != nil {
		if strings.Contains(err.Error(), "RepositoryAlreadyExistsException") {
			re := regexp.MustCompile(`registry with id '(\d+)'`)
			repo_id := re.FindStringSubmatch(err.Error())[1]
			repo_url = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com/%s", repo_id, a.region, repo_name)
		} else {
			return err
		}
	} else {
		repo_url = *repo_response.Repository.RepositoryUri
	}

	//build docker image
	fmt.Println("building docker image... (this may take few minuites)")
	docker_client, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	defer docker_client.Close()

	ctx, err := archive.TarWithOptions(a.tmpPath, &archive.TarOptions{})
	if err != nil {
		return err
	}

	a.imageName = repo_url + ":latest"
	build_response, err := docker_client.ImageBuild(context.TODO(), ctx, docker_types.ImageBuildOptions{
		Tags:     []string{a.imageName},
		Platform: "linux/amd64",
	})
	if err != nil {
		return err
	}
	defer build_response.Body.Close()
	io.Copy(io.Discard, build_response.Body)

	//push image to ecr
	var authConfig = registry.AuthConfig{
		Username:      username,
		Password:      password,
		ServerAddress: server_url,
	}
	authConfigBytes, _ := json.Marshal(authConfig)
	authConfigEncoded := base64.URLEncoding.EncodeToString(authConfigBytes)
	push_opts := docker_types.ImagePushOptions{RegistryAuth: authConfigEncoded}

	fmt.Println("pushing docker image to ECR... (this may take few minuites)")
	push_response, err := docker_client.ImagePush(context.TODO(), a.imageName, push_opts)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, push_response)

	fmt.Println("ECR ready!")
	return nil
}

func NewAWSPlatform() (*AWSPlatform, error) {
	return &AWSPlatform{}, nil
}

func (a *AWSPlatform) StartWorker(options map[string]interface{}) error {
	a.LoadConfig("/root/ReqBench/src/platform_adapter/aws/config.json")
	a.Workload_path = a.Config["workload_path"].(string)
	a.region = a.Config["region"].(string)
	a.executionArn = a.Config["lambda_execution_role_arn"].(string)
	a.tmpPath = a.Config["tmp_path"].(string)

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(a.region))
	if err != nil {
		return err
	}
	a.lambdaClient = lambda.NewFromConfig(cfg)
	a.logClient = cloudwatchlogs.NewFromConfig(cfg)

	err = a.buildImage()
	if err != nil {
		return err
	}

	a.metrics = []LatencyRecord{}

	return nil
}

func (a *AWSPlatform) KillWorker(options map[string]interface{}) error {
	// save metrics
	a.metricLock.Lock()
	defer a.metricLock.Unlock()
	csvPath := a.Config["csv_name"].(string)
	csvFile, err := os.Create(csvPath)
	if err != nil {
		fmt.Println("Error creating metrics file:", err)
	} else {
		defer csvFile.Close()
		err = flushToFile(a.metrics, csvPath)
	}

	return nil
}

func (a *AWSPlatform) DeployFuncs(funcs []workload.Function) error {
	var memSize int32 = 1024
	for _, f := range funcs {
		_, err := a.lambdaClient.CreateFunction(context.TODO(), &lambda.CreateFunctionInput{
			FunctionName: &f.Name,
			Code: &lambda_types.FunctionCode{
				ImageUri: &a.imageName,
			},
			Role:        &a.executionArn,
			PackageType: lambda_types.PackageTypeImage,
			ImageConfig: &lambda_types.ImageConfig{
				Command: []string{f.Name + ".handler"},
			},
			MemorySize: &memSize,
		})
		if err != nil {
			if strings.Contains(err.Error(), "ResourceConflictException") {
				//force cold start
				a.lambdaClient.UpdateFunctionConfiguration(context.TODO(), &lambda.UpdateFunctionConfigurationInput{
					FunctionName: &f.Name,
				})
			} else {
				return err
			}
		}
	}

	time.Sleep(60 * time.Second) //wait few secs until lambda is ready
	return nil
}

func (a *AWSPlatform) InvokeFunc(funcName string, timeout int, options map[string]interface{}) error {
	response, err := a.lambdaClient.Invoke(context.TODO(), &lambda.InvokeInput{
		FunctionName: &funcName,
	})
	if err != nil {
		return err
	}

	import_duration, _ := strconv.ParseFloat(string(response.Payload), 64)
	request_id, _ := middleware.GetRequestIDMetadata(response.ResultMetadata)

	//todo: collect metrics from cloudwatch log
	log_group_name := "/aws/lambda/" + funcName
	descending := true
	var limit int32 = 1
	log_response, err := a.logClient.DescribeLogStreams(context.TODO(), &cloudwatchlogs.DescribeLogStreamsInput{
		Descending:   &descending,
		Limit:        &limit,
		OrderBy:      log_types.OrderByLastEventTime,
		LogGroupName: &log_group_name,
	})

	log_message := ""
	for i := 0; i < 10; i++ {
		filter_pattern := fmt.Sprintf("\"REPORT RequestId: %s\"", request_id)
		log_response2, err := a.logClient.FilterLogEvents(context.TODO(), &cloudwatchlogs.FilterLogEventsInput{
			FilterPattern:  &filter_pattern,
			LogGroupName:   &log_group_name,
			LogStreamNames: []string{*log_response.LogStreams[0].LogStreamName},
		})

		if err != nil {
			return err
		}

		for _, r := range log_response2.Events {
			log_message = *r.Message
		}

		if len(log_response2.Events) > 0 {
			break
		}
		time.Sleep(3 * time.Second)
	}

	metrics := make(map[string]float64)
	patterns := map[string]*regexp.Regexp{
		"Duration":       regexp.MustCompile(`Duration: ([\d.]+) ms`),
		"BilledDuration": regexp.MustCompile(`Billed Duration: ([\d.]+) ms`),
		"MemorySize":     regexp.MustCompile(`Memory Size: ([\d.]+) MB`),
		"MaxMemoryUsed":  regexp.MustCompile(`Max Memory Used: ([\d.]+) MB`),
		"InitDuration":   regexp.MustCompile(`Init Duration: ([\d.]+) ms`),
	}

	for key, pattern := range patterns {
		matches := pattern.FindStringSubmatch(log_message)
		if len(matches) > 1 {
			value, err := strconv.ParseFloat(matches[1], 64)
			if err != nil {
				return err
			}
			metrics[key] = value
		} else {
			metrics[key] = -1
		}
	}

	record := LatencyRecord{
		RequestId:      request_id,
		FuncName:       funcName,
		Duration:       metrics["Duration"],
		BilledDuration: metrics["BilledDuration"],
		MemorySize:     metrics["MemorySize"],
		MaxMemoryUsed:  metrics["MaxMemoryUsed"],
		InitDuration:   metrics["InitDuration"],
		ImportDuration: import_duration,
	}
	fmt.Println(record)

	a.metricLock.Lock()
	a.metrics = append(a.metrics, record)
	a.metricLock.Unlock()

	return nil
}

func (a *AWSPlatform) DeleteAll() error {
	var err error
	a.functions, err = a.listFunctions(10000)
	if err != nil {
		return err
	}

	for _, functionName := range a.functions {
		_, err := a.lambdaClient.DeleteFunction(context.TODO(), &lambda.DeleteFunctionInput{
			FunctionName: aws.String(functionName),
		})
		if err != nil {
			return err
		}
	}
	a.functions = []string{}
	return nil
}
