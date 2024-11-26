import json
import random
import threading
import time
import traceback
from collections import Counter
from subprocess import check_output
from typing import List

import pandas as pd
import numpy as np
import requests
import send_req
import concurrent.futures
import json

from config import *
from util import *
from version import *

packages_size = {}
packages_lock = threading.Lock()


def generate_non_measure_code_lines(modules, return_val):
    return [
        "import time, importlib, os",
        "os.environ['OPENBLAS_NUM_THREADS'] = '2'",
        f"for mod in {modules}:",
        "    try:",
        "        importlib.import_module(mod)",
        "    except Exception as e:",
        "        pass",
        f"def f(event):",
        f"    return \"{return_val}\""
    ]


def gen_measure_code(modules, measure_latency=False, measure_mem=False):
    code_lines = [
        "import time, importlib, os",
        "os.environ['OPENBLAS_NUM_THREADS'] = '2'",
        "called = False",  # record if the function is called for the first time
        "split_gen=-1"  # this is for openlambda, to record the split generation
    ]
    if measure_mem:
        code_lines += ["import tracemalloc, gc, sys, json",
                       "gc.collect()",
                       "tracemalloc.start()"]
    if measure_latency:
        code_lines += ["t_StartImport = time.time()*1000"]
    code_lines += [
        "failed = []",
        f"for mod in {modules}:",
        "    try:",
        "        importlib.import_module(mod)",
        "    except Exception as e:",
        "        failed.append(mod)",
        "        pass"
    ]
    if measure_latency:
        code_lines += ["t_EndImport = time.time()*1000"]

    code_lines += [
        "def f(event):",
        "    global t_StartImport, t_EndImport, t_EndExecute, failed",
        "    time_start = time.time()*1000",
        # "    time.sleep(0.1)",
    ]
    if measure_latency:
        code_lines += [
            "    t_EndExecute = time.time()*1000",
            "    event['start_import'] = t_StartImport",
            "    event['end_import'] = t_EndImport",
            "    event['start_execute'] = time_start",
            "    event['end_execute'] = t_EndExecute",
            "    event['failed'] = failed",
        ]
        code_lines += [
            "    global called, split_gen, sb_id",
            "    if called:",
            "        event['split_gen'] = split_gen",
            "        event['start_import'] = 0",
            "        event['end_import'] = 0",
            "        event['sb_id'] = sb_id",
            "    called = True",
            "    split_gen = event['split_gen']",
            "    sb_id = event['sb_id']"
        ]

        code_lines += ["    called = True"]
    if measure_mem:
        code_lines += [
            "    mb = (tracemalloc.get_traced_memory()[0] - tracemalloc.get_tracemalloc_memory()) / 1024 / 1024"]
        code_lines += ["    event['memory_usage_mb'] = mb"]
    code_lines.append("    return event")
    return code_lines


def fetch_package_size(pkg_name, version):
    if "github.com" in pkg_name:
        return None
    with packages_lock:
        if packages_size.get(pkg_name) is not None and packages_size.get(pkg_name).get(version) is not None:
            return pkg_name, version, packages_size[pkg_name][version]

    size = get_whl_size(pkg_name, version)
    if size is None:
        print(f"cannot find size for {pkg_name} {version} in pypi")
        return None
    with packages_lock:
        if pkg_name not in packages_size:
            packages_size[pkg_name] = {}
        packages_size[pkg_name][version] = size
    return pkg_name, version, size


def get_whl_size(pkg_name, version):
    url = f"https://pypi.org/pypi/{pkg_name}/{version}/json"
    response = requests.get(url)
    if response.status_code != 200:
        return None

    try:
        data = response.json()
    except json.JSONDecodeError:
        print("Invalid JSON received.")
        print(url)
        return None

    whl_size = None
    linux_whl_size = None
    tar_gz_size = None
    for release in data["urls"]:
        if release["packagetype"] == "bdist_wheel":
            if "linux" in release["filename"]:
                linux_whl_size = release["size"]
            else:
                whl_size = release["size"]
        elif release["packagetype"] == "sdist":
            tar_gz_size = release["size"]

    if linux_whl_size is not None:
        return linux_whl_size * 7
    elif whl_size is not None:
        return whl_size * 7
    elif tar_gz_size is not None:
        return tar_gz_size * 10


# use this function to get all the pkgs size and store them in json.
def get_whl(pkgs):
    tasks = []
    finished = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            for pkg in pkgs:
                pkg_name = pkg.split("==")[0]
                version = pkg.split("==")[1]
                tasks.append((pkg_name, version))
            print("need to find", len(tasks), "in total")
            futures = [executor.submit(fetch_package_size, pkg_name, version) for pkg_name, version in tasks]

            for future in concurrent.futures.as_completed(futures):
                future.result()
                finished += 1
                if finished % 100 == 0:
                    print("find", finished, "pkg size")
    except Exception as e:
        print(e)
    # Save to JSON file anyway
    with open('packages_disk_size.json', 'w') as f:
        json.dump(packages_size, f, indent=2)


def get_top_n_packages(filtered_df, n=500):
    packages_appear_times = {}

    for col in filtered_df["compiled"]:
        requirements, _ = parse_requirements(col)
        if any([x in requirements.keys() for x in blacklist]):
            continue
        for pkg_name, op_version in requirements.items():
            pkg_name = pkg_name.split("[")[0]
            version = op_version[1]
            key = f"{pkg_name}=={version}"
            packages_appear_times[key] = packages_appear_times.get(key, 0) + 1

    print(f"there are {len(packages_appear_times)} unique packages in total")
    if n == -1:
        return _, packages_appear_times

    sorted_packages = sorted(packages_appear_times.items(), key=lambda x: x[1], reverse=True)
    # if n=-1, return all pkgs
    top_n_packages = sorted_packages[:n]
    return dict(top_n_packages), packages_appear_times


def generate_workloads_from_txts(txts):
    if isinstance(txts, str):
        txts = json.load(open(os.path.join(bench_file_dir, txts)))
    elif isinstance(txts, list):
        txts = txts
    wl = Workload()
    for txt in txts:
        meta_dict = {"requirements_in": txt, "requirements_txt": txt,
                     "direct_import_mods": [], "import_mods": []}
        meta = Meta.from_dict(meta_dict)
        name = wl.addFunc(meta=meta)
        wl.addCall(name)
    return wl


# we dump requirements.txt and requirements.in to json file, and reparse them to get the versioned packages
# direct_pkg_with_version: {pkg_name: (operator, version)}
# first step is to generate requirements.in and txt. unless these 2 args are provided
# then parse direct_pkg_with_version, package_with_version.
class Meta:
    # direct_pkg_with_version is a dict of {pkg_name: (operator, version)}
    def __init__(self, direct_pkg_with_version=None, pkg_with_version=None,
                 requirements_in=None, requirements_txt=None,
                 direct_import_mods=None, import_mods=None):
        self.direct_pkg_with_version = {} if direct_pkg_with_version is None else direct_pkg_with_version
        self.direct_import_mods = set() if direct_import_mods is None else set(direct_import_mods)

        if requirements_in is None:
            self.requirements_in = self.gen_requirements_in()
        else:
            self.requirements_in = requirements_in

        # todo: direct_pkg_with_version need to change version, for now I use whatever version pip choose
        # when you set requirements_txt, make sure you have requirements_in first
        if requirements_txt is None:
            self.try_gen_requirements_txt()
        else:
            self.requirements_txt = requirements_txt
        assert self.requirements_txt is not None

        # always re-parse requirements.in because it might be changed
        if direct_pkg_with_version is not None:
            self.direct_pkg_with_version = direct_pkg_with_version
        else:
            self.direct_pkg_with_version, _ = parse_requirements(self.requirements_in)

        if pkg_with_version is not None:
            self.pkg_with_version = pkg_with_version
        else:
            self.pkg_with_version, _ = parse_requirements(self.requirements_txt)

        self.import_mods = set() if import_mods is None else set(import_mods)

    # return true means we can generate requirements.txt from current requirements.in
    # return false means we generate requirements.txt after throw away some versions,
    # the worst case is all versions are ignored
    def try_gen_requirements_txt(self):
        self.requirements_txt = self.gen_requirements_txt()
        if self.requirements_txt is None:
            for pkg_name in self.direct_pkg_with_version:
                self.direct_pkg_with_version[pkg_name][0] = ""
                self.direct_pkg_with_version[pkg_name][1] = ""
                self.requirements_in = self.gen_requirements_in()
                self.requirements_txt = self.gen_requirements_txt()
                if self.requirements_txt is not None:
                    break
            return False
        return True

    def gen_requirements_in(self):
        requirements_in_str = ""
        for pkg in self.direct_pkg_with_version:
            op, version = self.direct_pkg_with_version[pkg][0], self.direct_pkg_with_version[pkg][1]
            if op is None:
                requirements_in_str += f"{pkg}\n"
            else:
                requirements_in_str += f"{pkg}{op}{version}\n"
        return requirements_in_str

    # return None means we cannot generate requirements.txt from current requirements.in
    def gen_requirements_txt(self, print_err=False):
        process = subprocess.Popen(
            ["pip-compile", "--output-file=-", "-"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate(input=self.requirements_in)
        # todo: you should probably delete this line, as it print a lot of annoyed message to console
        if process.returncode != 0:
            if print_err:
                print("err requirements in: \n", self.requirements_in)
                print("err in pip compile: \n", stderr)
            return None
            # raise Exception(f"pip-compile failed with error: {stderr}")
        return stdout

    def to_dict(self):
        return {
            "requirements_in": self.requirements_in,
            "requirements_txt": self.requirements_txt,
            "direct_import_mods": list(self.direct_import_mods),
            "import_mods": list(self.import_mods)
        }

    def __str__(self):
        return "install_pkgs: %s, import_mods: %s" % (
            self.pkg_with_version, self.import_mods)

    @staticmethod
    def from_dict(meta_dict):
        requirement_txt = meta_dict['requirements_txt']
        requirement_in = meta_dict['requirements_in']

        direct_pkg_with_version, _ = parse_requirements(requirement_in)
        pkg_with_version, _ = parse_requirements(requirement_txt)
        return Meta(direct_pkg_with_version, pkg_with_version,
                    requirements_in=requirement_in, requirements_txt=requirement_txt,
                    direct_import_mods=meta_dict["direct_import_mods"],
                    import_mods=meta_dict["import_mods"])


class Func:
    # direct_pkg_with_version is a dict of {pkg_name: (operator, version)}
    # if meta_dict is not None, then other args are ignored
    def __init__(self, name, code, direct_pkg_with_version=None, direct_import_mods=None, meta=None):
        self.name = name

        if meta is not None:
            self.meta = meta
        else:
            self.meta = Meta(direct_pkg_with_version=direct_pkg_with_version,
                             direct_import_mods=direct_import_mods)
        self.code = code

    def to_dict(self):
        return {"name": self.name, "meta": self.meta.to_dict(), "code": self.code}

    @staticmethod
    def from_dict(d):
        meta = Meta.from_dict(d['meta'])
        f = Func(name=d['name'], meta=meta, code=d['code'])
        return f

    def __str__(self):
        return "name: %s, meta: %s, code: %s" % (self.name, self.meta.to_dict(), self.code)


class Workload:
    def __init__(self, workload_path=None):
        self.funcs = []
        self.calls = []
        self.pkg_with_version = {}  # {pkg_name: (v1, v2, ...), ...}
        self.name = 1
        self.empty_pkgs_funcs = []
        self.invoke_freq = {}
        if workload_path:
            with open(workload_path) as f:
                j = json.load(f)
                self.funcs = [Func.from_dict(d) for d in j['funcs']]
                self.calls = j['calls']
                self.invoke_freq = Counter(call['name'] for call in self.calls)
                self.name = max([int(f.name[2:]) for f in self.funcs]) + 1
                self.pkg_with_version = j['pkg_with_version']
                for pkg, versions in self.pkg_with_version.items():
                    self.pkg_with_version[pkg] = set(versions)
                self.empty_pkgs_funcs = j['empty_pkgs_funcs'] if 'empty_pkgs_funcs' in j else []

    # if deps' name exist in one txt, then they can serve a compatible deps
    # deps= {pkg_name: {v1: {dep:ver, dep:ver, ...}, v2: {dep:ver, dep:ver, ...}}, ...}
    def parse_deps(self, deps):
        # deps_dict = {name: {v1:{deps_str: #used, deps_str: #used, ...}, v2: ...}
        # deps_set = {name: {v1: [dep_set, dep_set, ...], v2: ...}
        # '#used' is the number of times this deps_set is used
        deps_dict = {}  # deps_dict shows frequency
        deps_set = {}  # deps_set shows the deps as a set
        dep_matrix_dict = {}

        # add info from original deps dict
        for pkg_name, versions in deps.items():
            for version, deps_set_list in versions.items():
                if deps_set_list is None or len(deps_set_list) == 0:
                    continue
                dep_set = set()
                for name, ver in deps_set_list.items():
                    dep_set.add("%s==%s" % (name, ver))
                if pkg_name not in deps_dict:
                    deps_dict[pkg_name] = {}
                if version not in deps_dict[pkg_name]:
                    deps_dict[pkg_name][version] = {}
                deps_key = ",".join(sorted(dep_set))  # Using frozenset as a key for a dict
                if deps_key not in deps_dict[pkg_name][version]:
                    deps_dict[pkg_name][version][deps_key] = 0  # Initialize with 0 uses
                deps_dict[pkg_name][version][deps_key] += 1  # Increment the number of uses

                if pkg_name not in deps_set:
                    deps_set[pkg_name] = {}
                if version not in deps_set[pkg_name]:
                    deps_set[pkg_name][version] = []
                deps_set[pkg_name][version].append(dep_set)

        # learn from workload
        for func in self.funcs:
            _, deps_from_func = parse_requirements(func.meta.requirements_txt)
            matrix = construct_dependency_matrix(deps_from_func)
            full_deps = get_package_dependencies(matrix)

            for pkg_name, dependencies in full_deps.items():
                if pkg_name == 'direct_req':
                    continue
                if '==' in pkg_name:
                    name, version = pkg_name.split('==')
                    dependencies_str = ",".join(sorted(dependencies))
                    deps_key = dependencies_str

                    # update deps_dict
                    if name not in deps_dict:
                        deps_dict[name] = {}
                    if version not in deps_dict[name]:
                        deps_dict[name][version] = {}
                    if name in deps_dict and version in deps_dict[name]:
                        if deps_key in deps_dict[name][version]:
                            deps_dict[name][version][deps_key] += 1  # Increment the number of uses
                        else:
                            deps_dict[name][version][deps_key] = 1

                    if name not in deps_set:
                        deps_set[name] = {}
                    if version not in deps_set[name]:
                        deps_set[name][version] = []
                    if set(dependencies) not in deps_set[name][version]:
                        deps_set[name][version].append(set(dependencies))

                    for dep in dependencies:
                        if dep not in dep_matrix_dict:
                            dep_matrix_dict[dep] = {}
                        if pkg_name not in dep_matrix_dict[dep]:
                            dep_matrix_dict[dep][pkg_name] = 0
                        dep_matrix_dict[dep][pkg_name] += 1

        dep_matrix = pd.DataFrame.from_dict(dep_matrix_dict, orient='index')
        dep_matrix = dep_matrix.sort_index(axis=0).sort_index(axis=1).fillna(0).astype(int)
        return deps_dict, deps_set, dep_matrix

    # packages_with_version is {pkg1: (v1, v2, ...), ...}
    # import should be a set of strings, also accept list, but will convert to set
    def addFunc(self, packages_with_version=None, imports=None, meta=None):
        if packages_with_version is None:
            packages_with_version = {}

        if imports is None:
            imports = set()
        if type(imports) == list:
            imports = set(imports)

        name = 'fn%d' % self.name
        self.name += 1

        code = []
        import_arr_str = json.dumps(list(imports))
        if imports:
            code = generate_non_measure_code_lines(import_arr_str, name)
        else:
            code = generate_non_measure_code_lines("[]", name)

        f = Func(name=name, code=code,
                 direct_pkg_with_version=packages_with_version,
                 direct_import_mods=imports, meta=meta)

        # add all deps versioned pkgs' to the workload's pkgs dict
        for pkg, op_version in f.meta.pkg_with_version.items():
            if pkg not in self.pkg_with_version:
                self.pkg_with_version[pkg] = set()
            self.pkg_with_version[pkg].add(op_version[1])  # only add version instead of operator

        if len(f.meta.pkg_with_version) == 0:
            self.empty_pkgs_funcs.append(f.name)
        self.funcs.append(f)
        return name

    def addCall(self, name):
        self.calls.append({"name": name})
        self.invoke_freq[name] = self.invoke_freq.get(name, 0) + 1

    # return a df
    def call_matrix(self, compressed=True):
        df_rows = []
        index = []

        if compressed:
            for func in self.funcs:
                assert func is not None
                df_row = {}
                name = func.name
                for pkg, op_version in func.meta.pkg_with_version.items():
                    df_row[pkg + op_version[0] + op_version[1]] = self.invoke_freq.get(name, 0)
                df_rows.append(df_row)
                index.append(name)

            df = pd.DataFrame(df_rows, index=index).fillna(0).astype(int)
        else:
            for call in self.calls:
                func = self.find_func(call['name'])
                assert func is not None
                df_row = {}
                for pkg, op_version in func.meta.pkg_with_version.items():
                    df_row[pkg + op_version[0] + op_version[1]] = 1
                df_rows.append(df_row)
            df = pd.DataFrame(df_rows).fillna(0).astype(int)
        return df[sorted(df.columns)]

    # get all versioned packages used in workload, return a df
    # in previous experiments, we use "deps" in trace, jus name but no version is provided
    # however, since now we came up with pip-compile, such info can be easily obtained
    """
    you will get a matrix like this:
      A B C D
    A 1 0 0 0
    B 1 1 0 0
    C 1 1 1 0
    D 1 1 1 1
    [B,A] = 1 means A requires B
    """

    def dep_matrix(self, pkg_factory: List[Package]):
        pnames = []
        for pkg in self.pkg_with_version:
            for v in self.pkg_with_version[pkg]:
                pnames.append(pkg + "==" + v)
        pnames = sorted(pnames)
        df = pd.DataFrame(index=pnames, columns=pnames).fillna(0)

        # get deps info from our pkg_factory
        for name_op_version in df.columns:
            name = name_op_version.split("==")[0]
            version = name_op_version.split("==")[1]
            df.loc[name_op_version, name + "==" + version] = 1
            pkg = pkg_factory[name]
            # todo: this should not happen, and if it happens, we should pip-compile it
            if pkg is None:
                continue
            if version not in pkg.available_versions:
                print("Warning: %s not in %s's version" % (version, pkg))
            version_meta = pkg.available_versions[version]
            deps = version_meta.requirements_dict
            for dep_name, op_version in deps.items():
                df.loc[dep_name + "==" + op_version[1], name_op_version] = 1

        return df

    def add_metrics(self, metrics=[]):
        for func in self.funcs:
            mods_arr = json.dumps(list(func.meta.import_mods))
            new_code = gen_measure_code(mods_arr,
                                        measure_latency='latency' in metrics,
                                        measure_mem='memory' in metrics
                                        )
            func.code = new_code

    def shuffleCalls(self):
        random.shuffle(self.calls)

    # return 2 workloads, one for training, one for testing
    def random_split(self, ratio):
        wl_train = Workload()
        wl_test = Workload()
        wl_train_added = {}
        wl_test_added = {}

        self.shuffleCalls()

        train_size = int(len(self.calls) * ratio)
        train_calls = self.calls[:train_size]
        test_calls = self.calls[train_size:]

        for call in train_calls:
            func = self.find_func(call['name'])
            if func.name not in wl_train_added:
                name = wl_train.addFunc(None, func.meta.import_mods, func.meta)
                wl_train_added[func.name] = name
            wl_train.addCall(wl_train_added[func.name])

        for call in test_calls:
            func = self.find_func(call['name'])
            if func.name not in wl_test_added:
                name = wl_test.addFunc(None, func.meta.import_mods, func.meta)
                wl_test_added[func.name] = name
            wl_test.addCall(wl_test_added[func.name])

        return wl_train, wl_test

    def to_dict(self):
        funcs_dict = [f.to_dict() for f in self.funcs]
        return {'funcs': funcs_dict, 'calls': self.calls,
                'pkg_with_version': handle_sets(self.pkg_with_version), 'empty_pkgs_funcs': self.empty_pkgs_funcs}

    def save(self, path, workload_dict=None):
        with open(path, 'w') as f:
            if workload_dict is not None:
                json.dump(workload_dict, f, indent=2)
            else:
                json.dump(self.to_dict(), f, indent=2)
        return

    def play(self, options={}, tasks=5, total_time=None):
        # compile ReqBench_go
        reqbench_go_path = os.path.join(experiment_dir, "platform_adapter_go")

        mod_cmd = ["go", "mod", "tidy"]
        build_cmd = ["go", "build", "-o", "rb_go"]
        try:
            check_output(mod_cmd, cwd=reqbench_go_path)
            check_output(build_cmd, cwd=reqbench_go_path)
        except Exception as e:
            print(e)
            print("Failed to compile the ReqBench_go.")
            return {}

        # start with config
        temp_config_path = os.path.join(experiment_dir, "temp_config.json")
        config_json_str = json.dumps(options)
        with open(temp_config_path, "w") as f:
            f.write(config_json_str)

        temp_wl_path = os.path.join(experiment_dir, "temp_wl.json")
        self.save(temp_wl_path)
        # platform, workload, config, tasks, timeout, test duration(if any)
        runBench = [os.path.join(reqbench_go_path, "rb_go"), "openlambda",
                    temp_wl_path, temp_config_path,
                    str(tasks), "30", str(0 if total_time is None else total_time)]

        # parse the output
        stats = ""
        try:
            output_bytes = subprocess.check_output(runBench, stderr=subprocess.STDOUT)
            output_str = output_bytes.decode('utf-8')
            parts = output_str.split("stats:")
            if len(parts) > 1:
                stats = parts[1]
            else:
                print("No stats received from ReqBench_go.")
                return {}
        except subprocess.CalledProcessError as e:
            print("Run ReqBench_go failed", e.output.decode('utf-8'))

        # parse the dict
        try:
            stat_dict = json.loads(stats)
        except json.JSONDecodeError:
            print("Invalid JSON:", stats)
            return {}

        os.remove(temp_config_path)
        os.remove(temp_wl_path)
        return stat_dict

    def find_func(self, name):
        for f in self.funcs:  # todo: use a dict could be faster
            if f.name == name:
                return f
        return None

    # traverse all the meta.direct_pkg_with_version to find the matching func
    # pkg should be {pkg_name: versioned_package}
    def find_funcs_by_pkg(self, pkg):
        pkg = {key.lower(): value for key, value in pkg.items()}
        funcs = []
        for f in self.funcs:
            meta = f.meta
            # PEP 426: All comparisons of distribution names MUST be case insensitive
            f_dir_pkgs = {key.lower(): value for key, value in meta.direct_pkg_with_version.items()}
            if meta and pkg == f_dir_pkgs:
                funcs.append(f)
        return funcs

    # assume the name is like fn1, fn2, fn3 ...
    # and the call is in the same order (no repeated call)
    def add(self, workload):
        func_name_map = {}  # map from old name to new name
        for f in workload.funcs:
            old_name = f.name
            f.name = 'fn%d' % self.name
            f.code[-1] = "    return '%s'\n" % f.name
            func_name_map[old_name] = f.name
            self.funcs.append(f)
            self.name += 1

        for c in workload.calls:
            # rename calls
            c['name'] = func_name_map[c['name']]
            self.calls.append(c)

        # add pkg_with_version
        for pkg, versions_set in workload.pkg_with_version.items():
            if pkg not in self.pkg_with_version:
                self.pkg_with_version[pkg] = set()
            self.pkg_with_version[pkg] = self.pkg_with_version[pkg].union(versions_set)

    # randomly select some functions, add them to the workload with new name
    def expand(self, target):
        if target < len(self.calls):
            return
        for i in range(target - len(self.calls)):
            func = random.choice(self.funcs)
            name = self.addFunc(None, func.meta.import_mods, func.meta)
            self.addCall(name)

    # repeat the calls in the workload
    def gen_trace(self, target, skew=False, weights=None, s=1.5):
        self.calls = []

        function_names = [f.name for f in self.funcs]

        if not skew:
            if target <= len(function_names):
                names = random.sample(function_names, target)
            else:
                names = random.choices(function_names, k=target)
            for name in names:
                self.addCall(name)
            return

        if weights is not None:
            weights /= np.sum(weights)
            weighted_calls = random.choices(function_names, weights=weights, k=target)
            for name in weighted_calls:
                self.addCall(name)
        else:
            num_funcs = len(function_names)
            zipf_samples = np.random.zipf(s, target) - 1
            zipf_samples = np.mod(zipf_samples, num_funcs)
            for idx in zipf_samples:
                self.addCall(function_names[idx])


# load the deps from deps.json, parse the deps_str to a frozenset of deps
# now it looks like: {name: {version: {deps_set: count}}}, count is the number of times this deps_set appears
def load_all_deps(path):
    with open(path, 'r') as f:
        deps = json.load(f)
    new_deps = {}
    for name in deps:
        for version in deps[name]:
            for deps_str in deps[name][version]:
                if name not in new_deps:
                    new_deps[name] = {}
                if version not in new_deps[name]:
                    new_deps[name][version] = {}

                new_deps[name][version][frozenset(deps_str.split(","))] = deps[name][version][deps_str]
    return new_deps


blacklist = ["https://", "http://"]


def main():
    requirements_csv = os.path.join(experiment_dir, "requirements.csv")
    df = pd.read_csv(requirements_csv)
    filtered_df = df[(df['compiled'] != "") & (df['compiled'].notnull())]
    pkgs, _ = get_top_n_packages(filtered_df, 500)
    # wl = generate_workloads_from_txts(filtered_df["compiled"].tolist())
    # deps_dict, _, _ = wl.parse_deps({})
    # # the top-n packages might have dependencies that are not in them,
    # # so we need to expand the top-n packages list
    # for pkg in pkgs:
    #     name = pkg.split("==")[0]
    #     version = pkg.split("==")[1]
    #     deps = deps_dict[name][version]
    #     pkgs[pkg] = deps

    get_whl(pkgs)

    valid_cols = []
    # rule out the packages that are too big, not in the top 500, in the blacklist
    pkgs = packages_size
    for col in filtered_df["compiled"]:
        valid = 1
        requirements, _ = parse_requirements(col)
        for pkg_name, op_version in requirements.items():
            pkg_name = pkg_name.split("[")[0]
            version = op_version[1]
            if pkg_name not in pkgs or version not in pkgs[pkg_name]:
                valid = 0
        if valid:
            valid_cols.append(col)
    print(len(valid_cols))
    with open(os.path.join(experiment_dir, "valid_txt.json"), 'w') as f:
        json.dump(valid_cols, f, indent=2)

    wl = generate_workloads_from_txts(valid_cols)
    wl.save(os.path.join(experiment_dir, "workload.json"))
    wl.play({
        "import_cache_tree": os.path.join(experiment_dir, "valid_txt.json"),
        "limits.mem_mb": 900,
        "import_cache_tree": ""
    },
        tasks=5)

    for pkg, versions in wl.pkg_with_version.items():
        Package.add_version({pkg: versions})
        for version in versions:
            path = os.path.join(ol_dir, "default-ol", "lambda", "packages", pkg + "==" + version, "files")
            top_mods = []
            if os.path.exists(path):
                top_mods = get_top_modules(path)
            if Package.packages_factory[pkg].available_versions[version] is None:
                Package.packages_factory[pkg].available_versions[version] = versionMeta(top_mods, None, None)
            else:
                Package.packages_factory[pkg].available_versions[version].top_level = top_mods
    Package.save(os.path.join(experiment_dir, "packages.json"))

    with open(os.path.join(experiment_dir, "deps.json"), 'w') as file:
        deps_dict, _, _ = wl.parse_deps(Package.deps_dict())
        json.dump(deps_dict, file, indent=2)

    wl_with_top_mods = Workload()
    # add top mods to workload and save
    for f in wl.funcs:
        for pkg in f.meta.pkg_with_version:
            version = f.meta.direct_pkg_with_version[pkg][1]
            if Package.packages_factory[pkg].available_versions[version] is not None:
                f.meta.import_mods.update(Package.packages_factory[pkg].available_versions[version].top_level)
        name = wl_with_top_mods.addFunc(None, f.meta.import_mods, f.meta)
        wl_with_top_mods.addCall(name)
    wl_with_top_mods.save(os.path.join(experiment_dir, "workloads.json"))


if __name__ == '__main__':
    main()

"""
# find total size of top n packages on disk
total_size = sum(sum(versions.values()) for versions in pkg_size.values())
print(total_size/1024/1024)
"""

# # generate_workloads_from_csv(requirements_csv, 3000)
# df = pd.read_csv(requirements_csv)
# # randomly pick count of compiled columns
# filtered_df = df[(df['compiled'] != "") & (df['compiled'].notnull())]
# cols = filtered_df["compiled"]
# cols = filtered_df.sample(n=5)


# after workload generated, run the empty ones to install them(pkgs are about 3 GB in total),
#   then search through packages dir to find top_mods.
# after top_mods are found, measure the top pkgs importing cost, generate the tree
# then test the dataset

# some questions:
# pip-compile give a version not existed, e.g jaxlib==0.1.76, but jaxlib doesn't have 0.1.76 metadata on pypi
# some using: parcon @ git+https://github.com/javawizard/parcon, also cannot fetch metadata, and doesn't used often
