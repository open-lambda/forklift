import socket
import time
import traceback
import shutil
from bench import *
from config import *
from platform_adapter.openlambda.ol import get_total_mem,get_pss,get_rss
from mock_run import estimate_cost
import os
import glob
import psutil
from workload import Workload, Meta

base_path = "/sys/fs/cgroup/default-ol-sandboxes"


def block_on(port):
    HOST = 'localhost'

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, port))
    server_socket.listen(1)

    print(f"Listening for connections on {HOST}:{port}...")

    try:
        client_socket, address = server_socket.accept()
        print(f"Connection from {address} has been established.")
        while True:
            data = client_socket.recv(1024).decode('utf-8')
            if data:
                client_socket.close()
                break
    finally:
        server_socket.close()
        print(f"Server socket on {HOST}:{port} has been closed.")


# deprecated
# construct a warmup workload from a tree
def construct_wl(wl: Workload, node, parent_pkgs=[]):
    current_packages = parent_pkgs + node.get("packages", [])
    m = Meta(None, None, "\n".join(current_packages), "\n".join(current_packages))
    wl.addFunc(packages_with_version=None, imports=None, meta=m)

    for child in node.get("children", []):
        construct_wl(wl, child, current_packages)



def main():
    index_columns = ['nodes', 'trial', 'cow']
    df = pd.DataFrame(columns=index_columns)
    os.chdir(experiment_dir)
    for subdir in os.listdir('trials'):
        subdir_path = os.path.join('trials', subdir)
        if os.path.isdir(subdir_path) and subdir.isdigit():
            for tree_file in os.listdir(subdir_path):
                if tree_file.endswith('.json'):
                    match = re.match(r"tree-v(\d+).node-(\d+).json", tree_file)
                    if match:
                        tree_path = os.path.join(subdir_path, tree_file)
                        tree = json.load(open(tree_path, 'r'))

                        v_num = int(match.group(1))
                        nodes = int(match.group(2))
                        for cow in [True, False]:
                            options = {
                                "import_cache_tree": os.path.join(experiment_dir, tree_path),
                                "limits.mem_mb": 600,
                                "features.warmup": True,
                                "features.COW": cow,
                            }
                            # block on port 4997 waiting
                            t1 = threading.Thread(target=block_on, args=(4997,))
                            t1.start()
                            pid = start_worker(options)
                            t1.join()

                            mem_cost = get_total_mem(base_path)
                            kill_worker(pid)
                            if f"v{v_num}" not in df.columns:
                                df[f"v{v_num}"] = None

                            row_index = df[(df['nodes'] == nodes) & (df['trial'] == subdir) & (df['cow'] == cow)].index
                            if row_index.empty:
                                new_row = {'nodes': nodes, 'trial': subdir, 'cow': cow, f"v{v_num}": mem_cost}
                                df = df._append(new_row, ignore_index=True)
                            else:
                                df.at[row_index[0], f"v{v_num}"] = mem_cost
    # reorder columns
    all_columns = list(df.columns)
    columns_to_sort = [col for col in all_columns if col not in index_columns]
    sorted_columns = sorted(columns_to_sort)
    new_column_order = index_columns + sorted_columns
    df = df[new_column_order]

    df.set_index(index_columns, inplace=True, drop=True)
    df.sort_index(ascending=[True, True, True], inplace=True)
    df.to_csv(os.path.join(experiment_dir, "mem.csv"))
    print(df)


if __name__ == "__main__":
    main()
