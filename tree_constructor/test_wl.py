import os.path
from config import *
from workload import *
from platform_adapter.openlambda import ol

# ol = ol.OL()
# wl = Workload(platform=ol,
#                     workload_path=os.path.join(bench_file_dir, "trials", "0", 'w2.json'))
# wl.add_metrics(["latency"])
# wl.play(
#     {
#         "start_options": {
#             "import_cache_tree": os.path.join(experiment_dir,"trials/1/tree-v3.node-320.json"),
#             "limits.mem_mb": 600,
#             "features.warmup": False,
#         },
#         "kill_options": {
#             "save_metrics": True,
#             "csv_name": "metrics.csv",
#         },
#     }
# )
# wl = Workload(platform=ol,
#                     workload_path=os.path.join(experiment_dir, "filtered_workloads.json")
#                     )
# wl2=Workload()
# for func in wl.funcs:
#     if len(func.meta.import_mods) == 0:
#         print(func.name)
#         name = wl2.addFunc(None, func.meta.import_mods, func.meta)
#         wl2.addCall(name)
# print(wl2.call_matrix())



import numpy as np
from collections import Counter

def generate_zipf_calls(num_items, num_calls, s=1.5):
    return np.random.zipf(s, num_calls) % num_items

def calculate_top_percentage(calls, top_percentage):
    call_counts = Counter(calls)
    total_calls = sum(call_counts.values())
    top_calls = sum(count for item, count in call_counts.most_common(int(len(call_counts) * top_percentage / 100)))
    return (top_calls / total_calls) * 100

num_functions = 1800
num_calls = 100000
s = 1.5

calls = generate_zipf_calls(num_functions, num_calls, s)
top_percentage = 10

percentage_of_calls = calculate_top_percentage(calls, top_percentage)
print(f"top {top_percentage}% func account for {percentage_of_calls:.2f}% calls")

print(len(set(calls)))