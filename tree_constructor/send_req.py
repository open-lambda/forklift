import os
import shutil
import random

import requests
import json
import threading
import time
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

seen = {}
seen_lock = threading.Lock()


def get_curr_time():
    return time.time() * 1000


def get_id(name):
    global seen
    with seen_lock:
        if name not in seen:
            seen[name] = 1
        id = str(seen[name])
        seen[name] += 1
        return name + "_" + id

# deploy concurrently
def deploy_funcs(workload, platform):
    funcs = workload["funcs"]
    call_set = {call["name"] for call in workload["calls"]}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for fn in funcs:
            # only deploy functions that are called to save time
            if fn["name"] not in call_set:
                continue
            meta = fn["meta"]
            code = "\n".join(fn["code"])
            func_config = {
                "name": fn["name"],
                "code": code,
                "requirements_txt": meta["requirements_txt"]
            }
            future = executor.submit(platform.deploy_func, func_config)
            futures.append(future)
        for future in futures:
            future.result()
    return workload


def run(workload, num_tasks, platform, timeout=None, total_time=None):
    deploy_funcs(workload, platform)
    calls = workload['calls']
    finished = 0
    start_time = time.time()

    stamp = time.time()
    with ThreadPoolExecutor(max_workers=num_tasks) as executor:
        futures = [executor.submit(task, call, platform) for call in calls]
        for future in futures:
            try:
                if timeout is None:
                    future.result()
                else:
                    future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                print("Task timed out")
            except Exception as e:
                print(f"Task resulted in an exception: {e}")
            finished += 1
            current_time = time.time()
            if finished % 1000 == 0:
                print("current throughput: ", 1000 / (current_time - stamp))
                stamp = time.time()

        # if after running the calls, there is still time remaining, then pick some calls randomly
        while total_time and time.time() - start_time < total_time:
            remaining_futures = [executor.submit(task, call, platform) for call in calls]
            for future in remaining_futures:
                try:
                    future.result()
                except concurrent.futures.TimeoutError:
                    print("Task timed out")
                except Exception as e:
                    print(f"Task resulted in an exception: {e}")

                finished += 1
                if finished % 1000 == 0:
                    print("current throughput: ", 1000 / (time.time() - stamp))
                    stamp = time.time()
                if time.time() - start_time >= total_time:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
    # clean up
    global seen
    with seen_lock:
        seen={}
    end_time = time.time()
    seconds = end_time - start_time
    return seconds, finished / seconds


def task(call, platform):
    req_body = {
        "invoke_id": get_id(call['name']),
        "req": get_curr_time()
    }
    options = {
        "req_body": req_body
    }
    resp_body,err = platform.invoke_func(call['name'], options=options)

    if resp_body is None or resp_body == "":
        raise Exception(f"Error: {err}")
