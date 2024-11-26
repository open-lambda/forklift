import os.path

from skew_run import *

def main():
    print("mem_run pid is %d" % os.getpid())
    resource.setrlimit(resource.RLIMIT_NOFILE, (40960, 40960))
    pool_sizes = [8, 12, 16, 20, 24, 28, 32] # 8GB is the smallest memory we could set, less than 8 will cause the warmup to fail
    warmup = False
    skew_opt = False
    use_cache_wl = True
    use_cache_tree = False
    monitor_ns = False
    profile_lock = False
    invoke_len = 50000
    TREE_SIZES = [1, 40, 80, 160, 320, 640] # 1, 40, 80, 160, 320, 640
    TASKS = 5

    # pre generate the workload for reuse
    dir = os.path.join(experiment_dir, "trials", "mem")
    if not os.path.exists(dir):
        os.makedirs(dir)
    # gen wl first
    wl_with_top_mods.gen_trace(invoke_len*2,skew=skew_opt)
    wl_train, wl_test = wl_with_top_mods.random_split(0.5)
    wl_train.save(os.path.join(dir, "w1.json")), wl_test.save(os.path.join(dir, "w2.json"))
    resource.setrlimit(resource.RLIMIT_NOFILE, (40960, 40960))

    for pool_size in pool_sizes:
        res_dir = os.path.join(experiment_dir, "trials", "mem", f"pool_{pool_size}")
        if os.path.exists(res_dir):
            shutil.rmtree(res_dir)
        os.makedirs(res_dir)

        # copy the workload and trees to diff directory (representing diff memory) to reuse them
        csv_files = glob.glob(os.path.join(dir, '*.csv'))
        for f in csv_files:
            shutil.copy(f, res_dir)
        tree_files = glob.glob(os.path.join(dir, '*.json'))
        for f in tree_files:
            shutil.copy(f, res_dir)

        run_one_trial(res_dir, tree_sizes=TREE_SIZES, tasks=TASKS,
                      warmup=warmup, reuse_cgroups=True, downsize_on_pause=True, pool_size=pool_size*1024,
                      save_metrics=True, monitor_ns=monitor_ns, profile_lock=profile_lock,
                      skew=skew_opt, invoke_len=invoke_len,
                      use_cache_wl=use_cache_wl, use_cache_tree=use_cache_tree)

if __name__ == "__main__":
    turn_off_on_finish = True
    try:
        main()
    except KeyboardInterrupt:
        turn_off_on_finish = False
    except Exception as e:
        print(e)
        print(traceback.format_exc())
    finally:
        if turn_off_on_finish:
            credential = ManagedIdentityCredential()
            shutdown_vm(credential, subscription_id, resource_group, vm_name)