import glob
import os
import time
import traceback
import shutil
from bench import *
from config import *
from mock_run import estimate_cost
from pathlib import Path


# focus rules doesn't work anymore
def gen_opts_list(costs_dict):
    # opts_list is hard coded, used to preset some opts
    uniform_costs_dict = deepcopy(costs_dict)
    for k in uniform_costs_dict:
        if uniform_costs_dict[k] is None:
            print("costs_dict[%s] is None" % k)
        uniform_costs_dict[k]["ms"] = 1
        uniform_costs_dict[k]["i-ms"] = 1

    opts_list = [
        {  # baseline tests
            "prereq_first": True,  # pre-req should be renamed to multi-pkg
            "entropy_penalty": 0,
            "costs": uniform_costs_dict,
            "avg_dist_weights": False,
            "biased_dist_weights": False
        },
        {
            "prereq_first": True,
            "entropy_penalty": 0,
            "costs": costs_dict,
            "avg_dist_weights": False,
            "biased_dist_weights": False,
        },
        {
            "prereq_first": False,
            "entropy_penalty": 0,
            "costs": uniform_costs_dict,
            "avg_dist_weights": False,
            "biased_dist_weights": False,
        },
        {
            "prereq_first": False,
            "entropy_penalty": 0,
            "costs": costs_dict,
            "avg_dist_weights": False,
            "biased_dist_weights": False,
        }
    ]
    return opts_list


def process_tree(i, opts_list, w1, TREE_SIZES, dirName):
    print("fitting tree opt %d" % i)
    opts = opts_list[i]
    tree = Tree(w1.call_matrix(),
                w1.dep_matrix(Package.packages_factory),
                w1,
                SplitOpts(**opts))

    return make_trees(dirName, tree, TREE_SIZES, "v%d" % i)


# packages is a list of Package objects
def run_one_trial(dirName, packages, TREE_SIZES=None, TASKS=5, dry_run=False, warmup=False, save_metrics=False, use_cache_wl=True,
                  use_cache_tree=True):
    # pick pkg and versions from this dict
    if TREE_SIZES is None:
        TREE_SIZES = [0]
    print("generating workloads")
    # step 1: generate a training workload and a test workload
    if use_cache_wl:
        print("using cached workloads")
        w1 = Workload(os.path.join(dirName, "w1.json"))
        w2 = Workload(os.path.join(dirName, "w2.json"))
        w2.add_metrics(["latency"])
    else:
        wl_with_top_mods.gen_trace(target=len(wl_with_top_mods.funcs), skew=False)
        w1, w2 = wl_with_top_mods.random_split(0.5)
        w1.save(os.path.join(dirName, "w1.json"))
        w2.save(os.path.join(dirName, "w2.json"))
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
    costs_file_path = os.path.join(experiment_dir, "costs.json")
    with open(costs_file_path, "w") as file:
        json.dump(cost_dict, file)
    opts_list = gen_opts_list(cost_dict)

    ignore_index = []  # 0, 1, 2
    if not use_cache_tree:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            future_to_index = {executor.submit(process_tree, i, opts_list, w1, TREE_SIZES, dirName): i for i in
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
            for i, size in enumerate(TREE_SIZES):
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
            csv_path = os.path.join(dirName, f"{name}-{int(nodes)}.csv")
            stats = w2.play(
                options={
                        "timeout": 10,
                        "ol_dir": "/root/open-lambda/",
                        "run_url": "http://localhost:5000/run/",
                        "cg_dir": "/sys/fs/cgroup/default-ol-sandboxes",
                        "start_options": {
                            "import_cache_tree": os.path.join(experiment_dir, tree_path),
                            "limits.mem_mb": 500,
                            "features.warmup": warmup,
                        },
                        "kill_options": {
                            "save_metrics": save_metrics,
                            "csv_path": csv_path,
                        },
                    },
                tasks=TASKS)
            throughput = stats["ops/s"]
            mem_usage = stats["warmup_memory"] if warmup else None
            warmup_time = stats["warmup_time"] if warmup else None

            df_combined.loc[i, 'nodes'] = nodes
            df_combined.loc[i, f'{name}_throughput'] = throughput
            df_combined.loc[i, f'{name}_mem_usage'] = mem_usage
            df_combined.loc[i, f'{name}_warm_time'] = float(warmup_time)
            time.sleep(3)

        # use estimate_cost function to calculate hit rate
        miss, tree = estimate_cost(w1, tree_path, metric="PKG_MISS", uniform_weight=True)
        df_combined.loc[i, 'nodes'] = nodes
        df_combined.loc[i, f'{name}_train_hit'] = round(
            (len(w1.calls) - tree.root.count + empty_pkgs_call_w1) / len(w1.calls), 5)
        func_to_pkg_len = {func.name: len(func.meta.pkg_with_version) for func in w1.funcs}
        w1_total_pkgs = sum(func_to_pkg_len[call["name"]] for call in w1.calls)
        df_combined.loc[i, f'{name}_train_pkg_hit'] = round((w1_total_pkgs - miss) / w1_total_pkgs, 5)

        miss, tree = estimate_cost(w2, tree_path, metric="PKG_MISS", uniform_weight=True)
        tree.json(tree_path)
        df_combined.loc[i, f'{name}_test_hit'] = round(
            (len(w2.calls) - tree.root.count + empty_pkgs_call_w2) / len(w2.calls), 5)
        func_to_pkg_len = {func.name: len(func.meta.pkg_with_version) for func in w2.funcs}
        w2_total_pkgs = sum(func_to_pkg_len[call["name"]] for call in w2.calls)
        df_combined.loc[i, f'{name}_test_pkg_hit'] = round((w2_total_pkgs - miss) / w2_total_pkgs, 5)

    def aggregate_func(x):
        return x.dropna().iloc[0] if len(x.dropna()) > 0 else None

    result_df = df_combined.groupby(['nodes']).agg(aggregate_func).reset_index()
    return result_df


# given a trace, fit trees of various sizes (e.g., 10 to 50 nodes)
def make_trees(dirName, tree: Tree, TREE_SIZES, name):
    data_types = {
        'name': 'string',
        'path': 'string',
        'nodes': 'int'
    }
    df = pd.DataFrame(columns=list(data_types.keys())).astype(data_types)

    nodes = 1
    for i, size in enumerate(TREE_SIZES):
        splits = size - nodes
        try:
            tree.do_splits(splits)
        except Exception as e:
            # cannot add more child, even the tree is less than TREE_SIZES, just save it
            print("error splitting tree", e)
        nodes += splits
        path = dirName + '/tree-%s.node-%d.json' % (name, nodes)
        tree.root.json(path)  # save the tree to a file
        print("saved tree-%s.node-%d tree" % (name, nodes))
        df.loc[nodes, "name"] = name
        df.loc[nodes, "path"] = path
        df.loc[nodes, "nodes"] = nodes
    return df


def clean_trial(dirName):
    files = glob.glob(dirName + "/*")
    for f in files:
        os.remove(f)


wl_with_top_mods = Workload(
    os.path.join(experiment_dir, "filtered_workloads.json")
)
Package.from_json(path=os.path.join(experiment_dir, "packages.json"))


def main():
    shutil.copy(os.path.join(ol_dir, "min-image/runtimes/python/server_no_unshare.py"),
                os.path.join(ol_dir, "default-ol/lambda/runtimes/python/server.py"))
    use_cache_wl = False
    use_cache_tree = False

    TRIALS = 1 # number of trials to run
    TREE_SIZES = [1]
    tasks = 5 # number of concurrent requests

    trials = []
    for i in range(TRIALS):
        dirName = os.path.join(experiment_dir, "trials/tree_gen/%d" % i)
        if not os.path.exists(dirName):
            os.makedirs(dirName)
        if not use_cache_tree:
            clean_trial(dirName)

        t0 = time.time()
        # try:
        df = run_one_trial(dirName, Package.packages_factory,  TREE_SIZES=TREE_SIZES, TASKS=tasks,
                           dry_run=False, warmup=True, save_metrics=True, use_cache_wl=use_cache_wl,
                           use_cache_tree=use_cache_tree)

        # except Exception as e:
        #     print(traceback.format_exc())
        #     print("trial", i, "YIKES!")
        #     continue
        df["trial"] = i
        trials.append(df)
        t1 = time.time()

        print("TRIAL", i, "SECONDS", t1 - t0)
        pd.concat(trials).set_index(["nodes", "trial"]).to_csv(experiment_dir + "results.csv")


main()
