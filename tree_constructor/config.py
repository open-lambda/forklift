import os

# open-lambda base dir, change it according to your own setup
ol_dir = "/root/open-lambda/"
worker_out = os.path.join(ol_dir, "default-ol", "worker.out")
# current dir
experiment_dir = os.path.dirname(os.path.abspath(__file__))

# bench dir is used to estimate the cost, it is set to be the same with experiment_dir.
# most of the estimating features are not used
bench_dir = experiment_dir
bench_file_dir = bench_dir

# max number of threads for training a tree
MAX_TRAINING_THREADS = 1

# azure vm config, used to shut down the vm automatically
# you should forget about them and comment the lines using them if you are not using azure
subscription_id = ''
resource_group = ''
vm_name = ''