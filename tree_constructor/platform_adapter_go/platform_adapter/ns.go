package platform_adapter

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

var NewNsScript = `
BEGIN {
    printf("tid,start,copy_ipcs,copy_pid_ns,copy_utsname,duration\n");
}

kprobe:create_new_namespaces {
    @newns_start[tid] = nsecs;
}

kprobe:copy_ipcs {
	@newipc_start[tid] = nsecs;
}

kretprobe:copy_ipcs /@newipc_start[tid]/ {
	@newipc_start[tid] = nsecs-@newipc_start[tid];
}

kprobe:copy_pid_ns {
	@newpid_start[tid] = nsecs;
}

kretprobe:copy_pid_ns /@newpid_start[tid]/ {
	@newpid_start[tid] = nsecs-@newpid_start[tid];
}

kprobe:copy_utsname {
	@newuts_start[tid] = nsecs;
}

kretprobe:copy_utsname /@newuts_start[tid]/ {
	@newuts_start[tid] = nsecs-@newuts_start[tid];
}

kretprobe:create_new_namespaces {
	printf("%lld, %lld, %lld, %lld, %lld, %lld\n",
	tid, @newns_start[tid], @newipc_start[tid], @newpid_start[tid], @newuts_start[tid], nsecs-@newns_start[tid]);
	delete(@newns_start[tid]);
	delete(@newipc_start[tid]);
	delete(@newpid_start[tid]);
	delete(@newuts_start[tid]);
}

END {
    clear(@newns_start);
	clear(@newpid_start);
	clear(@newuts_start);
	clear(@newipc_start);
}
`

var MqCreateBreakup = `
BEGIN {
	printf("tid,start,fs_context_for_mount,put_ipc_ns,fc_mount,put_fs_context,end\n");
}

kprobe:mq_init_ns {
	@mq_create_start[tid] = nsecs;
}

kretprobe:fs_context_for_mount /@mq_create_start[tid]/ {
	@mq_create_end[tid] = nsecs;
}

kretprobe:put_ipc_ns /@mq_create_end[tid]/ {
	@put_ipc_end[tid] = nsecs;
}

kretprobe:fc_mount /@put_ipc_end[tid]/ {
	@fc_mount_end[tid] = nsecs;
}

kretprobe:put_fs_context /@fc_mount_end[tid]/ {
	@put_fs_context_end[tid] = nsecs;
}

kretprobe:mq_init_ns /@put_fs_context_end[tid]/ {
	printf("%lld, %lld, %lld, %lld, %lld, %lld, %lld\n",
	tid, @mq_create_start[tid], @mq_create_end[tid], @put_ipc_end[tid], @fc_mount_end[tid], @put_fs_context_end[tid], nsecs);
}

kretpobe:create_new_namespaces {
	delete(@mq_create_start[tid]);
	delete(@mq_create_end[tid]);
	delete(@put_ipc_end[tid]);
	delete(@fc_mount_end[tid]);
	delete(@put_fs_context_end[tid]);
}

END {
	clear(@mq_create_start);
	clear(@mq_create_end);
	clear(@put_ipc_end);
	clear(@fc_mount_end);
	clear(@put_fs_context_end);
}
`

var CreateIPCBreakup = `
BEGIN {
	printf("tid,start,mq_init_st,mq_init,setup_mq,setup_ipc,msg_init,end\n");
}

kprobe:copy_ipcs {
	@ipcs_start[tid] = nsecs;
}

kprobe:mq_init_ns {
	@mq_init_start[tid] = nsecs;
}

kretprobe:mq_init_ns /@mq_init_start[tid]/ {
	@mq_init_end[tid] = nsecs;
}

kretprobe:setup_mq_sysctls /@mq_init_end[tid]/ {
	@setup_mq_end[tid] = nsecs;
}

kretprobe:setup_ipc_sysctls /@setup_mq_end[tid]/ {
	@setup_ipc_end[tid] = nsecs;
}

kretprobe:msg_init_ns /@setup_ipc_end[tid]/ {
	@msg_init_end[tid] = nsecs;
}

kretprobe:copy_ipcs {
	printf("%lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld\n",
	tid, @ipcs_start[tid], 
	@mq_init_start[tid], @mq_init_end[tid], @setup_mq_end[tid], @setup_ipc_end[tid], @msg_init_end[tid], nsecs);
}

kretprobe:create_new_namespaces {
	delete(@ipcs_start[tid]);
	delete(@mq_init_start[tid]);
	delete(@mq_init_end[tid]);
	delete(@setup_mq_end[tid]);
	delete(@setup_ipc_end[tid]);
	delete(@msg_init_end[tid]);
}

END {
	clear(@ipcs_start);
	clear(@mq_init_start);
	clear(@mq_init_end);
	clear(@setup_mq_end);
	clear(@setup_ipc_end);
	clear(@msg_init_end);
}
`

type subTracer struct {
	script  string
	output  string
	process *os.Process
}

type BPFTracer struct {
	tracers []subTracer
}

type TracerConfig struct {
	Script string
	Output string
}

var scripts = map[string]string{
	"NewNsScript":      NewNsScript,
	"MqCreateBreakup":  MqCreateBreakup,
	"CreateIPCBreakup": CreateIPCBreakup,
}

func ConfigFromMap(configMap map[string]interface{}) ([]TracerConfig, error) {
	// key is the script variable name, value is the output file
	var tracerConfigs []TracerConfig
	for key, value := range configMap {
		output, ok := value.(string)
		if !ok {
			return tracerConfigs, fmt.Errorf("output file must be a string")
		}
		config, err := NewTracerConfig(key, output)
		if err != nil {
			return tracerConfigs, err
		}
		tracerConfigs = append(tracerConfigs, config)
	}
	return tracerConfigs, nil
}

func NewTracerConfig(scriptVar string, output string) (TracerConfig, error) {
	script, ok := scripts[scriptVar]
	if !ok {
		return TracerConfig{}, fmt.Errorf("script variable %s not found", scriptVar)
	}
	return TracerConfig{Script: script, Output: output}, nil
}

// set tracers by an array
func NewBPFTracer(configs []TracerConfig) *BPFTracer {
	bpfTracer := &BPFTracer{
		tracers: []subTracer{},
	}

	for _, config := range configs {
		if config.Output != "" {
			bpfTracer.tracers = append(bpfTracer.tracers, subTracer{script: config.Script, output: config.Output})
		}
	}

	return bpfTracer
}

func (b *BPFTracer) StartTracing() error {
	for i, tracer := range b.tracers {
		cmd := exec.Command("bpftrace", "-e", tracer.script, "-o", tracer.output)
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("bpftrace start error: %s", err)
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		b.tracers[i].process = cmd.Process
	}
	return nil
}

func (b *BPFTracer) StopTracing() error {
	for i, tracer := range b.tracers {
		if tracer.process != nil {
			if err := tracer.process.Signal(syscall.SIGINT); err != nil {
				return fmt.Errorf("error stopping bpftrace process (PID %lld): %s", tracer.process.Pid, err)
			}
			b.tracers[i].process = nil
		}
	}
	return nil
}
