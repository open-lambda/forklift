#!/usr/bin/env python3
import copy
import heapq
import json
import math
import multiprocessing
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import time
from collections import namedtuple
from copy import deepcopy
from subprocess import check_output

from version import *
from workload import *

# weights: vector of direct weight of each package; None means all equal
# prereq_first: pandas should be descendent of numpy (not vice versa)
# entropy_penalty: multiple weights by 1+penalty*entropy
# dist_weights: True means spread weight of pkg over all its prereqs
SplitOpts = namedtuple("SplitOpts", ["costs", "prereq_first", "entropy_penalty",
                                     "avg_dist_weights", "biased_dist_weights"])


# return a matrix, where matrix[a][b] = t means that
# a depends could on b, and import b (not include indirect) cost is t ms
def make_costs_matrix(dep_matrix, costs_dict):
    cost_matrix = dep_matrix.copy(deep=True)
    for i in cost_matrix.index:
        for j in cost_matrix.columns:
            if cost_matrix.loc[i, j] == 1:
                # our pre-measured cost can not cover every version,
                # it this case we give a estimated cost
                if costs_dict[i]["ms"] is None:
                    print("costs_dict[i][\"ms\"] is None, use an estimated one")
                    find_close_cost(costs_dict, i)
                cost_matrix.loc[i, j] = costs_dict[i]["ms"]
    return cost_matrix.values


def find_close_cost(costs_dict, i):
    for pkg in costs_dict:
        if pkg.startswith(i):
            costs_dict[i]["ms"] = costs_dict[pkg]["ms"]
            costs_dict[i]["i-ms"] = costs_dict[pkg]["i-ms"]
            return


def get_costs_vector(costs_dict, required_pkgs: List[str], mode="ms OR i-ms"):
    required_pkgs = sorted(required_pkgs)
    costs_vector = []
    for pkg in required_pkgs:
        if mode == "ms":
            costs_vector.append(costs_dict[pkg]["ms"])
        elif mode == "i-ms":
            costs_vector.append(costs_dict[pkg]["i-ms"])
        else:
            raise Exception("mode must be ms or i-ms")
    return np.array(costs_vector)


# this tree class is used when generating the zygote tree
class Tree:
    # calls: df matrix CxP (C is num of calls, P is num of pkgs)
    # deps: df matrix PxP (a 1 in a cell means the col pkg depends on the row pkg)
    # weights: P-length vector of pkg weights
    def __init__(self, calls, deps, workloads: Workload, split_opts):
        self.possible_deps, deps_set, deps = workloads.parse_deps(Package.deps_dict())
        self.deps_set = deps_set

        self.calls = calls
        self.workloads = workloads
        # self.weights should be a numpy array
        if split_opts.costs is not None:
            required_pkgs = deps.columns
            ms_costs_vector = get_costs_vector(split_opts.costs, required_pkgs, mode="ms")
            i_ms_costs_vector = get_costs_vector(split_opts.costs, required_pkgs, mode="i-ms")
            self.weights = ms_costs_vector.astype(np.float64)
        else:
            self.weights = np.ones(len(list(deps))).astype(np.float64)

        P = len(self.weights)
        assert calls.shape[1] == P
        assert deps.shape[0] == P
        assert deps.shape[1] == P

        # redistribute weights based on newly deps_set
        if split_opts.avg_dist_weights:
            self.weights = np.dot(deps / deps.sum(axis=0), self.weights)
        elif split_opts.biased_dist_weights:
            biased_cost = make_costs_matrix(deps, split_opts.costs)
            self.weights = np.dot(np.nan_to_num(biased_cost / biased_cost.sum(axis=0)), self.weights)

        # column names, each of which is a package
        self.pkg_names = list(calls)
        self.deps = deps
        self.split_opts = split_opts

        # priority queue of best splits (we keep popping from the front to grow the tree)
        self.split_queue = []
        self.split_generation = 0

        # do last, because the Node depends on other attrs of Tree
        self.root = Node(self, None, set(), set(), calls)
        self.root.recursively_update_costs()
        self.root.enqueue_best_split()

    def calls_cost(self, calls):
        return calls_cost_1(calls, self.weights, self.split_opts.entropy_penalty)

    # perform the best n splits
    def do_splits(self, n):
        for i in range(n):
            if self.root.rcost == 0:
                break
            try:
                _, _, s = heapq.heappop(self.split_queue)
                before = s.node.rcost
                s.node.split(s)
                after = s.node.rcost
                assert math.isclose(s.benefit, before - after)
            except Exception as e:
                print("error splitting tree", e)
                print(traceback.format_exc())
                break
        return self

    def save(self, path):
        with open(path, "w") as f:
            f.write(self.root.json())

def calls_cost_1(calls, weights, entropy_penalty):
    if calls.shape[0] == 0:
        return 0

    col_sums = calls.sum(axis=0).astype(np.float64)
    p = np.clip(col_sums / calls.shape[0], 1e-15, 1 - 1e-15)

    if entropy_penalty > 0:
        entropy = -(p * np.log2(p) + (1 - p) * np.log2(1 - p))
        adjusted_weights = weights * (1 + entropy_penalty * entropy)
    else:
        adjusted_weights = weights

    return np.dot(col_sums, adjusted_weights)


class Node:
    def __init__(self, tree: Tree, parent, direct, packages, calls,
                 depth=0, benefit=0):  # packages: set of package indices(package are sorted)
        self.tree = tree
        self.parent = parent

        self.direct = direct  # the direct pkg chosen by the split
        self.packages = packages
        self.calls = calls
        self.depth = depth
        self.benefit = benefit

        if parent is None:
            self.name = "ROOT"
            self.remaining = ...
            assert (len(packages) == 0)

            self.remaining = deepcopy(tree.deps_set)
        else:
            self.remaining = deepcopy(self.parent.remaining)
            for pkg_name in self.remaining:
                for version in self.remaining[pkg_name]:
                    # a list contains many sets, each set represents a set of deps
                    dep_list = self.remaining[pkg_name][version]
                    to_remove = []
                    for dep_set in dep_list:
                        # rule out deps that has same name but different version
                        for pkg_index in self.packages:
                            pkg = self.tree.pkg_names[pkg_index]
                            name = pkg.split("==")[0]
                            ver = pkg.split("==")[1]
                            # if a deps set has same name but different version, it's an invalid deps set
                            dep_dict = {pkg.split("==")[0]: pkg.split("==")[1] for pkg in dep_set}
                            if name in dep_dict and dep_dict[name] != ver:
                                to_remove.append(dep_set)
                                break
                            # rule out the deps that are already imported
                            if pkg in dep_set:
                                dep_set.remove(pkg)
                    # remove the invalid deps set
                    for dep_set in to_remove:
                        dep_list.remove(dep_set)

        # if A==1 is imported in parent, then A==2 shouldn't be imported again in child
        self.children = []
        self.cost = None
        self.rcost = None  # recursive (include cost of descendents)
        self.split_generation = self.tree.split_generation
        self.tree.split_generation += 1

    # update costs, from leaf to root
    def recursively_update_costs(self):
        self.cost = self.tree.calls_cost(self.calls.values)
        self.rcost = self.cost + sum(child.rcost for child in self.children)
        if self.parent:
            self.parent.recursively_update_costs()

    # return name by default, or return set of indices
    def recursive_deps(self, name=True):
        if self.parent is None:
            return set()
        if name:
            parents_pkgs_name = self.parent.recursive_deps(name=True)
            self_pkg_name = {self.tree.pkg_names[i] for i in self.packages}
            pkgs_names = deepcopy(parents_pkgs_name | self_pkg_name)
            return pkgs_names
        else:
            return deepcopy(self.parent.recursive_deps(name=False) | self.packages)

    # this should be called:
    # 1. when a Node is first created
    # 2. when a Node gets a new child due to a split
    def enqueue_best_split(self):
        if self.cost < 0.01:
            return  # no point in splitting further

        max_workers = MAX_TRAINING_THREADS
        executor = ThreadPoolExecutor(max_workers=max_workers)
        futures = []
        total_jobs = 0
        for pkg in self.remaining:
            for ver in self.remaining[pkg]:
                for dep_set in self.remaining[pkg][ver]:
                    rem = len(dep_set)
                    if rem < 1:
                        continue
                    if rem > 1 and self.tree.split_opts.prereq_first:
                        continue
                    total_jobs += 1
        batch_size = max(total_jobs // max_workers, 30)  # if the batch is too small, the overhead of creating a proc is large
        current_batch = []
        best_split = None

        for pkg in self.remaining:
            for ver in self.remaining[pkg]:
                for dep_set in self.remaining[pkg][ver]:
                    rem = len(dep_set)
                    if rem < 1:
                        continue

                    if rem > 1 and self.tree.split_opts.prereq_first:
                        continue

                    # when prereq_first off, one possible incompatibility probably will be caused,
                    # if A==4 <- B==5 (B dep on A), tree is root -> A==1
                    # B==5 probably will be chosen, and A==4 is required,
                    # but A==1 is imported in parent and cannot be removed,
                    # this issue ia solved by `self.remaining`:
                    #   if A==1 is imported in parent, then B wouldn't even in `self.remaining` keys
                    pkg_indices = []
                    for dep in dep_set:
                        pkg_indices.append(self.tree.calls.columns.get_loc(dep))

                    direct_pkg_index = self.tree.calls.columns.get_loc(pkg + "==" + ver)
                    # same pkg already imported in parent, as implied by self.remaining
                    if direct_pkg_index not in pkg_indices:
                        continue

                    # set the pkg==ver to the first position, represent it's the direct one
                    index = pkg_indices.index(self.tree.calls.columns.get_loc(pkg + "==" + ver))
                    pkg_indices.pop(index)
                    pkg_indices.insert(0, self.tree.calls.columns.get_loc(pkg + "==" + ver))

                    current_batch.append(pkg_indices)
                    if len(current_batch) >= batch_size:
                        future = executor.submit(create_split_batch, self, current_batch)
                        futures.append(future)
                        current_batch = []
                    # split = Split(self, pkg_indices)
                    # # first choose best_split on single package, then when to really split, we choose the most popular deps_set
                    # if best_split == None or split.benefit > best_split.benefit:
                    #     best_split = split
        if len(current_batch) > 0:
            future = executor.submit(create_split_batch, self, current_batch)
            futures.append(future)

        # print("benefit", best_split.benefit)
        # assert (best_split.benefit >= 0), when use entropy, (best_split.benefit >= 0) can't be guaranteed
        best_split_cols = None
        best_benefit = None
        best_first_col = None

        for future in as_completed(futures):
            results = future.result()
            for benefit, cols in results:
                if (best_split_cols is None
                        or (benefit > best_benefit) or (benefit == best_benefit and cols[0] < best_first_col)):
                    best_split_cols = cols
                    best_benefit = benefit
                    best_first_col = cols[0]
        executor.shutdown(wait=True)
        if best_split_cols is not None:
            best_split = Split(self, best_split_cols)
            # element 0: for actually scoring, element 1: to break ties(keep the tree deterministic), element 2: actual split
            tup = (-best_split.benefit, self.split_generation, best_split)
            heapq.heappush(self.tree.split_queue, tup)

    def split(self, split):
        child = Node(self.tree, self, {split.cols[0]}, split.cols, split.child_calls, self.depth + 1, split.benefit)
        self.calls = split.parent_calls
        self.children.append(child)
        child.recursively_update_costs()
        # pid_ns Maximum Nested Values is 32.
        # depth start from 0, so 31 is the max depth and I need reserve 1 for handler fork from parent zygote
        if child.depth < 30:
            child.enqueue_best_split()
        self.enqueue_best_split()

    def print(self, *args, **kwargs):
        print("- " * self.depth, end="")
        print(*args, **kwargs)

    def dump(self):
        self.print("%s [%.1f total cost]" % (self.name, self.rcost))
        self.print("indirect pkgs: %s" % ",".join(self.packagesIndirect))
        self.print("%d calls with sub cost %.1f" % (len(self.calls), self.cost))
        self.print(len(self.children), "children with sub cost %.1f" % (self.rcost - self.cost))
        for c in self.children:
            c.dump()

    def to_dict(self):
        funcs = self.calls.index.tolist()
        count = sum(self.tree.workloads.invoke_freq[func] for func in funcs)
        return {
            "direct": [self.tree.pkg_names[p] for p in self.direct],
            "packages": [self.tree.pkg_names[p] for p in self.packages],
            "split_generation": self.split_generation,
            "count": count,
            "depth": self.depth,
            "benefit": self.benefit,
            "parent": self.parent.split_generation if self.parent else None,
            "children": [node.to_dict() for node in self.children],
        }

    def json(self, path=None):
        if path == None:
            return json.dumps(self.to_dict(), indent=2)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)


def create_split_batch(node: Node, cols_batch):
    results = []

    calls_array = node.calls.values
    for cols in cols_batch:
        if isinstance(cols, int):
            cols = [cols]

        col_idxs = cols

        child_mask = np.all(calls_array[:, col_idxs] != 0, axis=1)
        child_calls_array = calls_array[child_mask, :].copy()
        parent_calls_array = calls_array[~child_mask, :].copy()

        child_calls_array[:, col_idxs] = 0

        parent_cost = node.tree.calls_cost(parent_calls_array)
        child_cost = node.tree.calls_cost(child_calls_array)
        benefit = node.cost - (parent_cost + child_cost)

        results.append((benefit, cols))
        child_calls_array[:, col_idxs] = 1

    return results


# col represents a versioned package, col should also be versioned
# todo: when there is a lot of data this is too slow
class Split:
    def __init__(self, node: Node, cols):
        if isinstance(cols, int):
            cols = [cols]
        self.node = node
        self.cols = cols

        column_names = [node.calls.columns[col] for col in cols]

        condition = (node.calls[column_names] != 0).all(axis=1)
        self.child_calls = node.calls[condition].copy()
        self.parent_calls = node.calls.drop(self.child_calls.index).copy()

        for column_name in column_names:
            self.child_calls.loc[:, column_name] = 0

        self.parent_cost = self.node.tree.calls_cost(self.parent_calls.values)
        self.child_cost = self.node.tree.calls_cost(self.child_calls.values)
        # benefit is reduction in tree cost
        self.benefit = node.cost - (self.parent_cost + self.child_cost)

    def __str__(self):
        return "[on col %d of node %s to save %.1f]" % (self.cols[0], self.node.split_generation, self.benefit)


def find_reqs_with_deps(reqs):
    reqs = copy.deepcopy(reqs)
    if len(reqs) == 0:
        return set()
    reqs_with_deps = set()
    for req in reqs:
        reqs_with_deps |= set(trace.packagesDict[req]["deps"])
    reqs |= find_reqs_with_deps(reqs_with_deps)
    return reqs


# import nothing, but only install the packages and fetch some metadata(like deps, toplevel mods)
# pkgs_with_version is {name1: {v1, v2, v3}, name2: {v1, v2, v3}}
def gen_workload_each_once(package_with_versions, concurrent=False):
    if concurrent:
        # clean old factory
        Package.packages_factory = {}
    w = Workload()
    Package.add_version(package_with_versions)

    for p in package_with_versions:
        for v in package_with_versions[p]:
            name = w.addFunc({p: ["==", v]}, [])
            pkg = Package.packages_factory[p]
            # set the requirements.txt
            pkg.available_versions[v] = versionMeta(
                top_level=None,
                requirements_dict=w.find_func(name).meta.pkg_with_version)
            w.addCall(name)
            print("version %s of %s added" % (v, p))
        print("added %s" % p)

    # # get rid of package with no version, that means some err happened when pip-compile the designated version
    # new_workload = Workload()
    # for f in w.funcs:
    #     for pkg in f.meta.pkg_with_version:
    #         if f.meta.pkg_with_version[pkg][1] is not None:
    #             new_workload.funcs.append(f)
    #             break
    return w, Package.get_from_factory(next(iter(package_with_versions.keys())))


# packages should be a set of packages without version, we choose a random version for each package
# pick_from is a list of names
# todo: test this function under versioned packages
def gen_workload_pairs(pick_from, packages: Dict[str, Package] = None, calls=500):
    if packages is None:
        packages = {}
    w = Workload()
    for i in range(calls):
        # randomly choose 2 packages, then decide their version
        pkg_with_versions = random.sample(list(pick_from), 2)
        packages_with_version = {}
        top_levels = set()
        for pkg in pkg_with_versions:
            v, v_meta = packages[pkg].choose_version()
            packages_with_version[pkg] = ["==", v]
            top_levels |= v_meta.top_level

        # takes long time cuz running pip-compile, need to parallelize
        name = w.addFunc(packages_with_version, top_levels)
        w.addCall(name)
    return w


def gen_workload_pairs_skewed(packages=[], calls=500):
    w = Workload()
    # duplicate each package 1, 2, or 3 times (to simulate different popularity)
    dups = []
    for p in packages:
        r = random.randint(1, 3)
        for i in range(r):
            dups.append(p)
    packages = dups

    for i in range(calls):
        pkgs = []
        while len(set(pkg["name"] for pkg in pkgs)) != 2:
            pkgs = random.sample(packages, 2)
        name = w.addFunc([pkgs[0]["name"], pkgs[1]["name"]], pkgs[0]["top"] + pkgs[1]["top"])
        w.addCall(name)
    return w


# pick_from = {pkg_name: {v1, v2, v3}, pkg_name2: {v1, v2, v3}, ...}
def gen_workload_pairs_concurrently(pick_from, packages: Dict[str, Package] = None, calls=500, num_processes=6):
    # tasks should be a list of list, sublist contains 2 package name
    res = gen_workload_pairs(pick_from, packages, calls % num_processes)
    calls -= calls % num_processes
    avg_calls = calls // num_processes

    with multiprocessing.Pool(processes=num_processes) as pool:
        results = []
        unfinished_calls = calls
        while calls > 0:
            if calls < avg_calls:
                avg_calls = calls
            result = pool.apply_async(gen_workload_pairs, args=(pick_from, packages, avg_calls))
            results.append(result)
            calls -= avg_calls

        while results:
            completed_results = [result for result in results if result.ready()]
            for result in completed_results:
                wl = result.get()
                unfinished_calls -= len(wl.calls)
                # print("unfinished calls: %d" % unfinished_calls)
                res.add(wl)
                results.remove(result)
        pool.close()
        pool.join()
    return res

