import copy
import json
from config import *
from mock_run import estimate_cost
from workload import Workload

packages_file_path = os.path.join(bench_file_dir, 'packages.json')
with open(packages_file_path, 'r') as file:
    packages = json.load(file)

def get_package_cost(package):
    name = package.split("==")[0]
    version = package.split("==")[1]
    if name not in packages:
        print(f"package {name} not found")
        return 0
    if version not in packages[name]["versions"]:
        print(f"package {name} not found")
        return 0
    return packages[name]["versions"][version]["cost"]["ms"]


def traverse_tree(node, gen_range, ancestors_packages=[]):
    ancestors_packages = copy.deepcopy(ancestors_packages)
    total_cost = 0
    ancestors_packages += node['packages']
    if gen_range[0] <= node['split_generation'] <= gen_range[1]:
        total_cost += node["count"]
        # for package in node['packages']:
        #     pkg_cost = get_package_cost(package)
        #     total_cost += pkg_cost
        # for package in ancestors_packages:
        #     pkg_cost = get_package_cost(package)
        #     total_cost += pkg_cost*node["count"]
    for child in node['children']:
        total_cost += traverse_tree(child, gen_range, ancestors_packages)
    return total_cost


generation_range = (0, 319)
tree_path = os.path.join(bench_file_dir, "trials", "4", 'tree-v2.node-160.json')
tree_path = os.path.join(bench_file_dir, "trials", "skew", "random_wl", 'tree-v2.node-1.json')
tree = json.load(open(tree_path))
w2 = Workload(os.path.join(bench_file_dir, "trials", "0", 'w2.json'))
total_cost = traverse_tree(tree, generation_range)
print(f'Total cost for split generations {generation_range}: {total_cost}')

tree_path = os.path.join(bench_file_dir, "trials", "4", 'tree-v3.node-160.json')
tree = json.load(open(tree_path))
total_cost = traverse_tree(tree, generation_range)
print(f'Total cost for split generations {generation_range}: {total_cost}')

# looks pretty unstable, 0-10 shows v1 better(as it should), but 10-20 shows v0 better
# for 10-20, costs are lower, but fit more requests
# later on, things changed a lot, take (50, 60) for example,
# v0 wins in both cost and hit rate
