# install all packages to /packages dir, then control the sys.path to import packages
import fnmatch
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

# find_compressed_files and normalize_pkg are copied from util.py
def find_compressed_files(dir_path, pattern):
    case_insensitive_pattern = pattern.lower()
    matched_files = []

    for root, dirs, files in os.walk(dir_path):
        for file in files:
            parts = file.split('-', 1)
            if len(parts) > 1:
                normalized_file = parts[0].lower().replace('.', '_') + '-' + parts[1].lower()
            else:
                normalized_file = file.lower()

            if fnmatch.fnmatch(normalized_file, case_insensitive_pattern):
                matched_files.append(os.path.join(root, file))

    return matched_files

def normalize_pkg(pkg: str) -> str:
    return pkg.lower().replace("-", "_")

# it can match most of the compressed file, but not all.
# e.g. zope-event==5.0's compressed file is zope.event-5.0...('-' is replaced by '.')
# simply ignore this case, as this func is used to save time, not for accuracy
def downloaded_packages(name, version):
    files = find_compressed_files("/tmp/.cache/", f"{normalize_pkg(name)}-{version}*")
    return len(files) > 0

def install_package(pkg, install_dir):
    name = pkg.split("==")[0]
    version = pkg.split("==")[1]

    try:
        install_dir = os.path.join(install_dir, pkg)
        # download, then install. by doing this, we could eliminate the time of downloading affected by network
        if not downloaded_packages(name, version):
            print(f"downloading {pkg}")
            subprocess.check_output(
                ['pip3', 'download', '--no-deps', pkg, '--dest', '/tmp/.cache'],
                stderr=subprocess.STDOUT
            )
        subprocess.check_output(
            ['pip3', 'install', '--no-deps', pkg, '--cache-dir', '/tmp/.cache', '-t', install_dir],
            stderr=subprocess.STDOUT
        )
    except Exception as e:
        print(f"failed to install {pkg}: {e}")

def main(pkgs):
    with ThreadPoolExecutor(max_workers=4) as executor:
        for pkg in pkgs:
            executor.submit(install_package, pkg, "/packages")

if __name__ == "__main__":
    pkg_list = []
    with open(sys.argv[1], 'r') as f:
        for line in f.readlines():
            pkg_list.append(line.strip())
    main(pkg_list)