#!/usr/bin/env python3
import multiprocessing
import sys

from bench import *
from util import *
from config import *

# use not import but importlib to import modules to avoid syntax error
measure_ms = """
import time, sys, importlib, os
# scipy will use all cores by default, which cause errors
os.environ['OPENBLAS_NUM_THREADS'] = '2'

def is_module_imported(module_name):
    return module_name in sys.modules
    
def f(event):
    try:
        dep_mods = {dep_mods}
        for mod in dep_mods:
            importlib.import_module(mod) 
        t0 = time.time()
        mods = {mods}
        for mod in mods:
            importlib.import_module(mod) 
        t1 = time.time()
    except Exception as e:
        t0 = -1
    if t0 == -1:
        return -1
    return (t1 - t0) * 1000
"""

measure_mb = """
import tracemalloc, gc, sys, importlib, os

import os
os.environ['OPENBLAS_NUM_THREADS'] = '2'

def f(event):
    try:
        dep_mods = {dep_mods}
        for mod in dep_mods:
            importlib.import_module(mod) 
        gc.collect()
        tracemalloc.start()
        mods = {mods}
        for mod in mods:
            importlib.import_module(mod)  
        mb = (tracemalloc.get_traced_memory()[0] -  tracemalloc.get_tracemalloc_memory()) / 1024/1024
        return mb
    except Exception as e:
        return -1
"""


def post(path, data):
    return requests.post('http://localhost:5000/' + path, json.dumps(data))


def measure(meta=None, stat='mb OR ms', include_indirect=False):
    if meta is None:
        meta = Meta()
    mods = meta.direct_import_mods
    dep_mods = meta.import_mods - meta.direct_import_mods
    pkg_name = list(meta.direct_pkg_with_version.keys())[0]
    # cannot use '==' when requesting lambda, says "can only contain letters, numbers, period, dash, and underscore"
    pkg_with_version = pkg_name + "_" + meta.direct_pkg_with_version[pkg_name][1]
    print(pkg_with_version)
    dep_pkgs = set(meta.pkg_with_version.keys()) - set(meta.direct_pkg_with_version.keys())

    if len(mods) == 0:
        return 0  # no imports means no cost
    dep_mods = json.dumps(list(dep_mods))
    mods = json.dumps(list(mods))

    code = {
        'ms': measure_ms,
        'mb': measure_mb,
    }[stat]

    # dep_pkgs no longer needed, still, let's keep it for debugging
    dep_pkgs.add(pkg_with_version)
    code = code.format(dep_pkgs=",".join(dep_pkgs),
                       dep_mods=dep_mods,
                       pkg=pkg_name, mods=mods)

    if include_indirect:
        name = "i_" + stat + "-" + pkg_with_version
    else:
        name = stat + "-" + pkg_with_version
    path = os.path.join(ol_dir, "default-ol", "registry", name)
    if not os.path.exists(path):
        os.mkdir(path)
    with open(os.path.join(path, "f.py"), "w") as f:
        f.write(code)
    with open(os.path.join(path, "requirements.in"), "w") as f:
        f.write(meta.requirements_in)
    with open(os.path.join(path, "requirements.txt"), "w") as f:
        f.write(meta.requirements_txt)

    target = "run/" + name
    r = post(target, None)
    r.raise_for_status()
    return max(float(r.text), 0)

# deps and costs are saved in packages.json
def find_deps_and_costs(deps_dict):
    metas = gen_meta_each_pkg(deps_dict)
    # step 2: try importing each discovered pkg, measuring import
    # latency and mem usage, beyond that of the deps
    print("measuring import costs")
    remove_dirs_with_pattern(os.path.join(ol_dir, "default-ol/registry"), r'^measure-*')
    pid = start_worker({"limits.mem_mb": 1000, "import_cache_tree": ""})
    for p in Package.packages_factory:
        if p not in wl.pkg_with_version:
            continue
        pkg = Package.get_from_factory(p)
        for v in pkg.available_versions:
            if not p+"=="+v in metas:
                continue
            meta = deepcopy(metas[p+"=="+v])

            pkg_versions = pkg.available_versions
            pkg_versions[v].cost = {
                "i-ms": measure(meta, "ms", True),
                "i-mb": measure(meta, "mb", True),
                "ms": measure(meta, "ms"),
                "mb": measure(meta, "mb"),
            }
    Package.save(os.path.join(experiment_dir, "packages.json"))
    kill_worker(pid)


# this time we create meta for each pkg from deps(a dict), those deps info are
# collected from pip-compile file, also fill the top-level mods for each pkg
def gen_meta_each_pkg(deps):
    metas = {}
    for pkg in Package.packages_factory:
        for version in Package.packages_factory[pkg].available_versions:
            # construct a meta for each pkg
            # collect requirements.txt for each pkg
            req_set = next(iter(deps[pkg][version]))
            req_txt = ""
            for req in req_set:
                req_txt += req + "\n"
            dir_top_mods = Package.packages_factory[pkg].available_versions[version].top_level
            indir_pkgs, _ = parse_requirements(req_txt)
            indir_top_mods = []
            for indir_pkg in indir_pkgs:
                indir_versions = indir_pkgs[indir_pkg][1]
                indir_top_mods += Package.packages_factory[indir_pkg].available_versions[indir_versions].top_level
            meta = Meta({pkg: ["==", version]}, indir_pkgs, req_txt, req_txt, dir_top_mods, indir_top_mods)
            metas[pkg + "==" + version] = meta
    return metas


# {pkg1: [v1,v2...], pkg2: [v3,v4...]} -> [{pkg1: [v1,v2...]}, {pkg2: [v3,v4...]}]
def dict_to_list(input_dict):
    result_list = []
    for key, value in input_dict.items():
        small_dict = {key: value}
        result_list.append(small_dict)
    return result_list

wl = Workload(os.path.join(experiment_dir, "filtered_workloads.json"))
def main():
    # load deps_dict from deps.json
    deps_dict = load_all_deps(os.path.join(experiment_dir, "deps.json"))
    Package.from_json(os.path.join(experiment_dir, "packages.json"))

    find_deps_and_costs(deps_dict)
    json.dump(Package.cost_dict(), open(os.path.join(experiment_dir, "costs.json"), "w"), indent=2)


if __name__ == '__main__':
    main()
