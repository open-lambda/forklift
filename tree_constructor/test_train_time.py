from skew_run import *
import sys, io
import config

def main():
    print("pid is %d" % os.getpid())
    resource.setrlimit(resource.RLIMIT_NOFILE, (40960, 40960))
    use_cache_wl = False
    invoke_len = 100000
    TREE_SIZES = [640]

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()

    round = 50
    for i in range(round):
        res_dir = os.path.join(experiment_dir, "trials", "train_time")
        if os.path.exists(res_dir) and not use_cache_wl:
            shutil.rmtree(res_dir)
        if not os.path.exists(res_dir):
            os.makedirs(res_dir)

        wl = None
        if use_cache_wl:
            wl = Workload(workload_path=os.path.join(res_dir, "w1.json"))
        else:
            wl = Workload(
                None,
                os.path.join(experiment_dir, "filtered_workloads.json")
            )
            wl.gen_trace(target=invoke_len, skew=False)
            wl.save(os.path.join(res_dir, "w1.json"))

        Package.from_json(path=os.path.join(experiment_dir, "packages.json"))

        cost_dict = Package.cost_dict()
        opts_list = gen_opts_list(cost_dict)
        process_tree(3, opts_list, wl, res_dir, TREE_SIZES)

    output = sys.stdout.getvalue()
    error = sys.stderr.getvalue()
    sys.stdout = original_stdout
    sys.stderr = original_stderr
    training_time = []
    # extract the time after "time elapsed:"
    output_lines = output.split("\n")
    for line in output_lines:
        if line.startswith("time elapsed:"):
            training_time.append(float(line.split(":")[1].strip()))
    # print mean
    print("mean val:", sum(training_time)/round)
    # print std dev
    print("std dev:", np.std(training_time))

if __name__ == "__main__":
    main()