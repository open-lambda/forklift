# this py will run the skewed workload
import glob
import os
import time
import traceback
import shutil
from bench import *
from config import *
from util import copy_worker_out
import resource


def gen_opts_list(costs_dict):
    # opts_list is hard coded, used to preset some opts
    uniform_costs_dict = deepcopy(costs_dict)
    for k in uniform_costs_dict:
        if uniform_costs_dict[k] == None:
            print("costs_dict[%s] is None" % k)
        uniform_costs_dict[k]["ms"] = 1
        uniform_costs_dict[k]["i-ms"] = 1

    opts_list = [
        {  # Single-package+Uniform Weight
            "prereq_first": True,  # todo: pre-req should be renamed to multi-pkg
            "entropy_penalty": 0,
            "costs": uniform_costs_dict,
            "avg_dist_weights": False,
            "biased_dist_weights": False
        },
        { # Single-package+Time-based Weight
            "prereq_first": True,
            "entropy_penalty": 0,
            "costs": costs_dict,
            "avg_dist_weights": False,
            "biased_dist_weights": False,
        },
        { # Multi-package+Uniform Weight
            "prereq_first": False,
            "entropy_penalty": 0,
            "costs": uniform_costs_dict,
            "avg_dist_weights": False,
            "biased_dist_weights": False,
        },
        { # Multi-package+Time-based Weight
            "prereq_first": False,
            "entropy_penalty": 0,
            "costs": costs_dict,
            "avg_dist_weights": False,
            "biased_dist_weights": False,
        }
    ]
    return opts_list


def make_trees(dirName, tree: Tree, name, tree_sizes):
    data_types = {
        'name': 'string',
        'path': 'string',
        'nodes': 'int'
    }
    df = pd.DataFrame(columns=list(data_types.keys())).astype(data_types)

    nodes = 1
    start = time.time()
    for i, size in enumerate(tree_sizes):
        splits = size - nodes
        try:
            tree.do_splits(splits)
        except Exception as e:
            # cannot add more child, even the tree is less than tree_sizes, just save it
            print("error splitting tree", e)
        nodes += splits
        path = dirName + '/tree-%s.node-%d.json' % (name, nodes)
        tree.root.json(path)  # save the tree to a file
        print("saved tree-%s.node-%d tree" % (name, nodes))
        print("time elapsed: %f" % (time.time() - start))
        df.loc[nodes, "name"] = name
        df.loc[nodes, "path"] = path
        df.loc[nodes, "nodes"] = nodes
    return df


def process_tree(i, opts_list, w1, dirName, tree_sizes):
    print("fitting tree opt %d" % i)
    opts = opts_list[i]
    tree = Tree(w1.call_matrix(),
                w1.dep_matrix(Package.packages_factory),
                w1,
                SplitOpts(**opts))

    return make_trees(dirName, tree, "v%d" % i, tree_sizes)


def run_one_trial(dirName, tree_sizes=None, tasks=5, dry_run=False, ignore_index=None,
                  warmup=False, reuse_cgroups=True, downsize_on_pause=True, pool_size=32768,  # ol's options
                  profile_lock=False, monitor_ns=False, save_metrics=False, # reqbench's options
                  skew=False, invoke_len=20000, total_time=None, # workload's options
                  use_cache_wl=True, use_cache_tree=True,
                  sleep_time=120):
    if ignore_index is None:
        ignore_index = [0, 1, 2] #option 3 represent the "Multi-package+Time-based Weight" as described in the paper
    if tree_sizes is None:
        tree_sizes = [1]
    print("generating workloads")
    # step 1: generate a training workload and a test workload
    if use_cache_wl:
        print("using cached workloads")
        w1 = Workload(os.path.join(dirName, "w1.json"))
        w2 = Workload(os.path.join(dirName, "w2.json"))
        if save_metrics:
            w2.add_metrics(["latency"])
    else:
        wl_with_top_mods.gen_trace(target=invoke_len*2, skew=skew)
        w1, w2 = wl_with_top_mods.random_split(0.5)
        w1.save(os.path.join(dirName, "w1.json"))
        w2.save(os.path.join(dirName, "w2.json"))
        if save_metrics:
            w2.add_metrics(["latency"])
    # record how many calls are having empty importing modules, used to calculate hit rate
    empty_pkgs_call_w1 = 0
    for call in w1.calls:
        empty_pkgs_call_w1 += 1 if call["name"] in w1.empty_pkgs_funcs else 0
    empty_pkgs_call_w2 = 0
    for call in w2.calls:
        empty_pkgs_call_w2 += 1 if call["name"] in w2.empty_pkgs_funcs else 0

    # step 2: fit trees of various sizes with various rules
    dfs = []
    os.chdir(experiment_dir)
    cost_dict = Package.cost_dict()
    json.dump(cost_dict, open(os.path.join(experiment_dir, "costs.json"), "w"))
    opts_list = gen_opts_list(cost_dict)

    if not use_cache_tree:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            future_to_index = {executor.submit(process_tree, i, opts_list, w1, dirName, tree_sizes): i for i in
                               range(len(opts_list)) if
                               i not in ignore_index}

            for future in concurrent.futures.as_completed(future_to_index):
                i = future_to_index[future]
                try:
                    dfs.append(future.result())
                except Exception as exc:
                    print('rule %r generated an exception: %s' % (i, exc))
                    print(traceback.format_exc())
    else:
        data_types = {
            'name': 'string',
            'path': 'string',
            'nodes': 'int'
        }
        for rule in range(len(opts_list)):
            if rule in ignore_index:
                continue
            df = pd.DataFrame(columns=list(data_types.keys())).astype(data_types)
            for i, size in enumerate(tree_sizes):
                nodes = size
                name = "v%d" % rule
                path = dirName + '/tree-%s.node-%d.json' % (name, nodes)
                if not os.path.exists(path):
                    continue
                df.loc[nodes, "name"] = name
                df.loc[nodes, "path"] = path
                df.loc[nodes, "nodes"] = nodes
            dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    # step 3: run the test workload against each tree from each workload
    df_combined = pd.DataFrame()
    for i in range(len(df)):
        tree_path = df.loc[i, "path"]
        nodes = df.loc[i, 'nodes']
        name = df.loc[i, 'name']
        if not dry_run:
            #  lock_stat_path=os.path.join(dirName, f"{name}-{int(nodes)}-lock.data")
            csv_path = os.path.join(dirName, f"{name}-{int(nodes)}.csv")
            options = {
                "timeout": 10,
                "ol_dir": "/root/open-lambda/",
                "run_url": "http://localhost:5000/run/",
                "cg_dir": "/sys/fs/cgroup/default-ol-sandboxes",
                "start_options": {
                    "import_cache_tree": os.path.join(experiment_dir, tree_path),
                    "mem_pool_mb": pool_size,
                    "limits.mem_mb": 600,
                    "features.warmup": warmup,
                    "features.reuse_cgroups": reuse_cgroups,
                    "features.downsize_paused_mem": downsize_on_pause,
                },
                "kill_options": {
                    "save_metrics": save_metrics,
                    "csv_path": csv_path,
                },
            }
            if profile_lock:
                options["profile_lock"] = {
                    "interval": 1,
                    "output_path": dirName,
                }
            if monitor_ns:
                options["monitor_ns"] = {
                    "NewNsScript": dirName + "/ns.csv",
                }
            stats = w2.play(options=options, tasks=tasks, total_time=total_time)
            throughput = stats["ops/s"]
            mem_usage = stats["warmup_memory"] if warmup else None

            df_combined.loc[i, 'nodes'] = nodes
            df_combined.loc[i, f'{name}_throughput'] = throughput
            df_combined.loc[i, f'{name}_mem_usage'] = mem_usage
            if i != range(len(df))[-1]:
                time.sleep(sleep_time)


wl_with_top_mods = Workload(
    os.path.join(experiment_dir, "filtered_workloads.json")
)
Package.from_json(path=os.path.join(experiment_dir, "packages.json"))


def main():
    print("skew_run pid is %d" % os.getpid())
    resource.setrlimit(resource.RLIMIT_NOFILE, (40960, 40960))
    warmup = True
    skew_opts = [False, True]
    use_cache_wl = False
    use_cache_tree = False
    monitor_ns = False
    profile_lock = False
    invoke_len = 50000
    TREE_SIZES = [40]
    TASKS = 5

    for skew in skew_opts:
        res_dir = os.path.join(experiment_dir, "trials", "skew", "random_wl" if not skew else "skewed_wl")
        if os.path.exists(res_dir) and not use_cache_wl and not use_cache_tree:
            shutil.rmtree(res_dir)
        if not os.path.exists(res_dir):
            os.makedirs(res_dir)
        run_one_trial(res_dir, tree_sizes=TREE_SIZES, tasks=TASKS,
                      warmup=warmup, reuse_cgroups=True, downsize_on_pause=True,
                      save_metrics=True, monitor_ns=monitor_ns, profile_lock=profile_lock,
                      skew=skew, invoke_len=invoke_len,
                      use_cache_wl=use_cache_wl, use_cache_tree=use_cache_tree)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        print(traceback.format_exc())
    finally:
        # credential = ManagedIdentityCredential()
        # shutdown_vm(credential, subscription_id, resource_group, vm_name)
        pass
