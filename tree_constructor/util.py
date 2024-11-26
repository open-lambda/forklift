import os
import pkgutil
import re
import shutil
import signal
import subprocess
import time
from collections import deque

import pandas as pd

try:
    from azure.identity import ManagedIdentityCredential
    from azure.mgmt.compute import ComputeManagementClient
except Exception as e:
    pass

from config import *

dep_pattern = re.compile(r'#\s+via\s+(.+)')

pattern = re.compile(
    r'([^<>=!;]+)'  # pkg name or url
    r'([<>!=]=?)?'  # version operator
    r'([^<>=!;]+)?'  # version
)

end_string = "The following packages are considered to be unsafe in a requirements file"


def normalize_pkg(pkg: str) -> str:
    return pkg.lower().replace("_", "-")


def parse_requirements(line_str, direct=False):
    """
    Parse the given pip-compile generated requirements string.

    Args:
        line_str (str): The string content of the requirements file.

    Returns:
        requirements: {'cython': ['==', '3.0.2'], 'numpy': ['==', '1.25.2'], 'packaging': ['==', '23.1'], 'scipy': ['==', '1.11.2']}
        versioned_dependencies: {'cython==3.0.2': ['cython==3.0.2', 'direct_req'], 'numpy==1.25.2': ['numpy==1.25.2', 'direct_req', 'scipy==1.11.2'], ...}
        (dependencies = {'cython': ['direct_req'], 'numpy': ['direct_req', 'scipy'], ...} )
    Raises:
        Exception: If the input string is None.

    Notes:
        dependencies(A)=[B, C] means B,C depends on A
    """

    if line_str is None:
        raise Exception("requirement.in or txt is None")
    lines = line_str.splitlines()

    requirements = {}
    dependencies = {}

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if end_string in line:
            break
        # match = pattern.match(line)

        parts = line.split(';', 1)
        package_part = parts[0]
        condition_part = parts[1].strip() if len(parts) > 1 else None
        match = pattern.match(package_part)

        if match and not lines[i].startswith('#'):
            # todo: handle condition operator and value
            package, operator, version = match.groups()
            if version is None:
                version = "-1"
            if operator is None:
                operator = "=="
            current_package = package.strip().split("[")[0]
            requirements[current_package] = [operator.strip(), version.strip()] if version else None

            i += 1
            isFirstComment = True
            # there could be a few comments followed <pkg>==<ver>, specifying who requires this pkg
            while i < len(lines) and lines[i].strip().startswith("#"):
                line = lines[i].strip()
                if isFirstComment:
                    isFirstComment = False
                    dependency = line.removeprefix(
                        "# via").strip()  # get rid of the '# via', the rest is the package name
                else:
                    dependency = line.removeprefix("#").strip()

                if '-r' in dependency:  # Replace with shorthand
                    dependency = "direct_req"

                if dependency is None or dependency == "":
                    pass
                elif current_package in dependencies:
                    dependencies[current_package].append(dependency)
                else:
                    dependencies[current_package] = [dependency]
                i += 1
        else:
            i += 1

    # add the keys and values of the dependencies dict with version
    versioned_dependencies = {}
    for pkg, deps in dependencies.items():
        pkg_key = f"{pkg}=={requirements[pkg][1]}"
        versioned_dependencies[pkg_key] = [pkg_key]
        for dep in deps:
            if dep in requirements:
                if dep is None or requirements.get(dep) is None:
                    print(dep)
                dep_key = f"{dep}=={requirements[dep][1]}"
                versioned_dependencies[pkg_key].append(dep_key)
            else:
                versioned_dependencies[pkg_key].append(dep)
    if direct:
        requirements = {}
        for pkg in versioned_dependencies.keys():
            if "direct_req" in versioned_dependencies[pkg]:
                requirements[pkg.split("==")[0]] = ['==', pkg.split("==")[1]]
    return requirements, versioned_dependencies


def normalize_pkg(pkg: str) -> str:
    return pkg.lower().replace("_", "-")


def handle_sets(obj):
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: handle_sets(v) for k, v in obj.items()}
    return obj


def start_worker(options={}):
    optstr = ",".join(["%s=%s" % (k, v) for k, v in options.items()])

    cmd = ['./ol', 'worker', 'up', '-d']
    if optstr:
        cmd.extend(['-o', optstr])
    print(cmd)
    out = subprocess.check_output(cmd, cwd=ol_dir)
    print(str(out, 'utf-8'))

    match = re.search(r"PID: (\d+)", str(out, 'utf-8'))
    if match:
        pid = match.group(1)
        print(f"The PID is {pid}")
        return pid
    else:
        print("No PID found in the text.")
        return -1


def kill_worker(pid, options={}):
    try:
        cmd = ['./ol', 'worker', 'down']
        out = subprocess.check_output(cmd, cwd=ol_dir)
        print(str(out, 'utf-8'))
    except Exception as e:
        print(e)
        print("force kill")

        print(f"Killing process {pid} on port 5000")
        subprocess.run(['kill', '-9', pid], cwd=ol_dir)

        cmd = ['./ol', 'worker', 'force-cleanup']
        subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ol_dir)

        process = subprocess.Popen(['./ol', 'worker', 'up'])
        os.kill(process.pid, signal.SIGINT)

        cmd = ['./ol', 'worker', 'force-cleanup']
        subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ol_dir)


def remove_dirs_with_pattern(path, pattern):
    for dir_name in os.listdir(path):
        if re.match(pattern, dir_name):
            dir_path = os.path.join(path, dir_name)
            if os.path.isdir(dir_path):
                shutil.rmtree(dir_path)


def clean_registry():
    registry_dir = os.path.join(ol_dir, "default-ol", "registry")

    for item in os.listdir(registry_dir):
        item_path = os.path.join(registry_dir, item)

        if os.path.isfile(item_path):
            os.remove(item_path)
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)


def construct_dependency_matrix(dependencies):
    all_packages = list(set(dependencies.keys()).union(*dependencies.values()))

    matrix = pd.DataFrame(0, index=all_packages, columns=all_packages)
    for pkg, deps in dependencies.items():
        for dep in deps:
            matrix.loc[pkg, dep] = 1

    return matrix


def get_package_dependencies(matrix):
    """Get dependencies for each package based on the matrix."""
    package_deps = {}
    for pkg in matrix.columns:
        package_deps[pkg] = get_recursive_dependencies(pkg, matrix)
    return package_deps


def get_recursive_dependencies(pkg, matrix):
    visited = set()
    all_deps = []
    queue = deque([pkg])

    while queue:
        current_pkg = queue.popleft()

        if current_pkg in visited:
            continue

        visited.add(current_pkg)

        immediate_deps = matrix.index[matrix[current_pkg] == 1].tolist()
        all_deps.extend(immediate_deps)

        for dep in immediate_deps:
            if dep not in visited:
                queue.append(dep)

    return list(set(all_deps))  # Removing duplicates


def get_top_modules(path):
    return [name for _, name, _ in pkgutil.iter_modules([path])]


def copy_worker_out(dst_dir, name="worker.out"):
    worker_out_path = os.path.join(ol_dir, "default-ol", "worker.out")
    shutil.copy(worker_out_path, dst_dir)
    os.rename(os.path.join(dst_dir, "worker.out"), os.path.join(dst_dir, name))


def shutdown_vm(credential, subscription_id, resource_group, vm_name):
    compute_client = ComputeManagementClient(credential, subscription_id)
    compute_client.virtual_machines.begin_deallocate(resource_group, vm_name)