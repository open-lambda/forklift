# ReqBench

ReqBench has a readme file in https://github.com/open-lambda/ReqBench/tree/main. To collect the dataset from scratch, 
you can follow it from the first step. However, skipping the big-query and pip-compile steps is reasonable as they take a long time.
You may start from "Collect packages' info" step to get information about the packages and then generate the workload.

For some reason, zygote crashes while importing `netifaces`. Therefore, "filtered_workloads.json" is provided, where functions that use netifaces are removed.

A brief explanation of the files in current directory produced by ReqBench:

> workloads.json: the workload where each function is called once.
> 
> filtered_workloads.json: the workload where functions that use netifaces are removed.
>
> packages.json: the overhead of importing each top-level module in a Docker container, also disk usage, etc.

Running the evaluation will require **filtered_workloads.json** and **packages.json** existed in current directory.

# Re-measure the import overhead

packages.json, produced by Reqbench, contains the overhead of importing each top-level module in a Docker container. 

For more accurate import overhead measurements, it is better to run "python3 deps_and_costs.py". It will re-measure the importing time in zygote environment.
Another important reason for this re-run is to pre-install the packages in the workload, which will save a significant amount of time during the evaluation.

# Run the evaluation

**ReqBench requirements on Go version**
>Our Python caller will call [ReqBench](tree_constructor/platform_adapter_go) implemented in Go to run workload. 
[ReqBench](tree_constructor/platform_adapter_go) requires **go version 1.20 or higher**. 
While OpenLambda requires version 1.18. Please **just install version 1.20 or higher**.

**OpenLambda branch**
>I use **[metrics](https://github.com/open-lambda/open-lambda/tree/metrics)** branch of OpenLambda to run the evaluation. The branch contains some experimental features, 
e.g. renaming tornado(to avoid version conflict), zygotes warmup, non-COW, no unshare, etc. It is not merged into the master branch yet.

Before running python scripts, **change the configurations (`ol_dir` and azure vm settings) in config.py**.

To run the evaluation， execute `python3 run.py`. The results, including trees and timestamps (CSV), will be saved (overwritten) in `trials/tree_gen`. 

The plots can be found in `trees.ipynb` and `perf.ipynb`. To locate corresponding figures, search "# fig n" in the jupyter notebooks, where n is the figure number.

# Packages
You may want to install pandas==2.1.3 as some of the features are not available in the latest version.
Also, it contains a few azure packages that is used to shut my VM down.

Except that, it's just matplotlib, seaborn, and jupyter. The full list is below:
```
anyio==4.4.0
apturl==0.5.2
argon2-cffi==23.1.0
argon2-cffi-bindings==21.2.0
arrow==1.3.0
asttokens==2.4.1
async-lru==2.0.4
attrs==23.2.0
azure-common==1.1.28
azure-core==1.30.2
azure-identity==1.17.1
azure-mgmt-compute==31.0.0
azure-mgmt-core==1.4.0
Babel==2.15.0
bcrypt==3.2.0
beautifulsoup4==4.12.3
bleach==6.1.0
blinker==1.4
Brlapi==0.8.3
certifi==2020.6.20
cffi==1.16.0
chardet==4.0.0
charset-normalizer==3.3.2
click==8.0.3
colorama==0.4.4
comm==0.2.2
command-not-found==0.3
contourpy==1.2.1
cryptography==3.4.8
cupshelpers==1.0
cycler==0.12.1
dbus-python==1.2.18
debugpy==1.8.2
decorator==5.1.1
defer==1.0.6
defusedxml==0.7.1
distro==1.7.0
distro-info==1.1+ubuntu0.2
duplicity==0.8.21
exceptiongroup==1.2.1
executing==2.0.1
fasteners==0.14.1
fastjsonschema==2.20.0
fonttools==4.53.0
fqdn==1.5.1
future==0.18.2
h11==0.14.0
httpcore==1.0.5
httplib2==0.20.2
httpx==0.27.0
idna==3.3
importlib-metadata==4.6.4
ipykernel==6.29.5
ipython==8.26.0
ipywidgets==8.1.3
isodate==0.6.1
isoduration==20.11.0
jedi==0.19.1
jeepney==0.7.1
Jinja2==3.1.4
json5==0.9.25
jsonpointer==3.0.0
jsonschema==4.22.0
jsonschema-specifications==2023.12.1
jupyter==1.0.0
jupyter-console==6.6.3
jupyter-events==0.10.0
jupyter-lsp==2.2.5
jupyter_client==8.6.2
jupyter_core==5.7.2
jupyter_server==2.14.1
jupyter_server_terminals==0.5.3
jupyterlab==4.2.3
jupyterlab_pygments==0.3.0
jupyterlab_server==2.27.2
jupyterlab_widgets==3.0.11
keyring==23.5.0
kiwisolver==1.4.5
language-selector==0.1
launchpadlib==1.10.16
lazr.restfulclient==0.14.4
lazr.uri==1.0.6
lockfile==0.12.2
louis==3.20.0
macaroonbakery==1.3.1
Mako==1.1.3
MarkupSafe==2.0.1
matplotlib==3.9.1
matplotlib-inline==0.1.7
mistune==3.0.2
monotonic==1.6
more-itertools==8.10.0
msal==1.29.0
msal-extensions==1.2.0
nbclient==0.10.0
nbconvert==7.16.4
nbformat==5.10.4
nest-asyncio==1.6.0
netifaces==0.11.0
notebook==7.2.1
notebook_shim==0.2.4
numpy==1.26.4
oauthlib==3.2.0
olefile==0.46
overrides==7.7.0
packaging==24.1
pandas==2.1.3
pandocfilters==1.5.1
paramiko==2.9.3
parso==0.8.4
pexpect==4.8.0
Pillow==9.0.1
platformdirs==4.2.2
portalocker==2.10.0
prometheus_client==0.20.0
prompt_toolkit==3.0.47
protobuf==3.12.4
psutil==6.0.0
ptyprocess==0.7.0
pure-eval==0.2.2
pycairo==1.20.1
pycparser==2.22
pycups==2.0.1
Pygments==2.11.2
PyGObject==3.42.1
PyJWT==2.3.0
pymacaroons==0.13.0
PyNaCl==1.5.0
pyparsing==2.4.7
pyRFC3339==1.1
python-apt==2.4.0+ubuntu3
python-dateutil==2.9.0.post0
python-debian==0.1.43+ubuntu1.1
python-json-logger==2.0.7
pytz==2022.1
pyxdg==0.27
PyYAML==5.4.1
pyzmq==26.0.3
qtconsole==5.5.2
QtPy==2.4.1
referencing==0.35.1
reportlab==3.6.8
requests==2.32.3
rfc3339-validator==0.1.4
rfc3986-validator==0.1.1
rpds-py==0.18.1
seaborn==0.13.2
SecretStorage==3.3.1
Send2Trash==1.8.3
six==1.16.0
sniffio==1.3.1
soupsieve==2.5
ssh-import-id==5.11
stack-data==0.6.3
systemd-python==234
terminado==0.18.1
tinycss2==1.3.0
tomli==2.0.1
tornado==6.4.1
traitlets==5.14.3
types-python-dateutil==2.9.0.20240316
typing_extensions==4.12.2
tzdata==2024.1
ubuntu-drivers-common==0.0.0
ubuntu-pro-client==8001
ufw==0.36.1
unattended-upgrades==0.1
uri-template==1.3.0
urllib3==1.26.5
usb-creator==0.3.7
wadllib==1.3.6
wcwidth==0.2.13
webcolors==24.6.0
webencodings==0.5.1
websocket-client==1.8.0
widgetsnbextension==4.0.11
xdg==5
xkit==0.0.0
zipp==1.0.0
```

