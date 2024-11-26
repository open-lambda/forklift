import copy
import json
import random
from typing import Dict, Optional


import requests
from config import *
from util import *

from packaging.version import Version
from packaging.specifiers import SpecifierSet

def is_python_version_compatible(specifier):
    # current_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    current_version = "3.10.6"
    return Version(current_version) in SpecifierSet(specifier)


# only include versions that are compatible with ubuntu 22.04
def is_ubuntu_version_compatible(release_date):
    ubuntu_2204_release_date = "2022-04-21"
    return release_date > ubuntu_2204_release_date


def is_compatible(version_meta):
    if version_meta["requires_python"] is None:
        return False  # no requires_python field, that usually means version released in python 2
    else:
        python_compatible = is_python_version_compatible(version_meta["requires_python"])
        ubuntu_compatible = is_ubuntu_version_compatible(version_meta["upload_time"].split("T")[0])
        return python_compatible and ubuntu_compatible


class versionMeta:
    # top_level: a set of top level packages
    # requirements_dict: {package: (operator, version), ...}
    def __init__(self, top_level=None, requirements_dict=None, cost=None):
        if top_level is None:
            top_level = set()
        if requirements_dict is None:
            requirements_dict = {}
        if cost is None:
            self.cost = {
                "ms": 0,
                "mb": 0,
                "i-ms": 0,
                "i-mb": 0
            }

        self.top_level = top_level
        self.requirements_dict = requirements_dict
        self.cost = cost

    # I use "top" to represent the top level packages, it is consistent with dep-trace.json
    def to_dict(self):
        return {"top": list(self.top_level), "requirements": self.requirements_dict,
                "cost": self.cost}

# In real case, there is no meaning to fetch all compatible versions, but we should
# just get a self-contained package with version specified.
# Here we just fetch all versions for generating the workload
class Package:
    # packages_factory: {name: Package, ...}
    packages_factory = {}

    def __init__(self, name, versions: Optional[Dict[str, Optional[versionMeta]]] = None, popularity=None):
        self.name = name
        # if no version provided, get compatible versions from pypi.org
        if versions is None:
            self.available_versions = {version: None for version in self.get_versions()}
        else:
            self.available_versions = versions

        if popularity is None:
            self.popularity = self.get_popularity()  # a number between 0 and 1
        else:
            self.popularity = []

    # b or beta: beta release; c or rc: release candidate; post: post release;
    # get rid of the versions with beta/b, rc/c in them, but not post, as post will be chose
    # compatible_only: only include versions that are compatible with python 3.10 and ubuntu 22.04
    # this function fetches versions from pypi.org
    def get_versions(self, stable_only=True, compatible_only=True):
        response = requests.get(f"https://pypi.org/pypi/{self.name}/json")
        data = response.json()
        versions = list(data["releases"].keys())

        ban_list = ["b", "beta", "c", "rc"]
        if stable_only:
            versions = [v for v in versions if not any(b in v for b in ban_list)]

        compatible_versions = []
        if not compatible_only:
            return versions

        for v in versions:
            try:
                # I call "range" as data["releases"][v][0] contains required python versions range
                # it also contains other information (like upload time)
                version_range = data["releases"][v][0]
            except Exception as e:
                # no compatible version range is provided, remove this version by default
                continue
            if is_compatible(version_range):
                compatible_versions.append(v)
        if len(compatible_versions) == 0:
            compatible_versions.append(versions[-1])
            # print(f"no compatible version for {self.name}, choose the latest version {versions[-1]}")
        return compatible_versions

    @classmethod
    # add a specific version to the package, not fetching the version from pypi.org
    # pkg_with_versions: {pkg1: {v1,v2,...}, pkg2: {v1,v2,...}, ...}, only versions specified in the list will be added
    def add_version(cls, pkg_with_versions):
        for pkg in pkg_with_versions:
            versions_to_add = pkg_with_versions[pkg]
            for version in versions_to_add:
                if pkg not in cls.packages_factory:
                    cls.packages_factory[pkg] = Package(pkg, {version: None}, None)
                cls.packages_factory[pkg].available_versions[version] = None
        return None

    # return a version and its versionMeta
    # todo: choose the version based on popularity
    def choose_version(self) -> (str, versionMeta):
        version = random.choice(list(self.available_versions.keys()))
        return version, self.available_versions[version]

    # todo: get popularity from pypi? maybe should get download count?
    def get_popularity(self):
        return []

    @classmethod
    def get_from_factory(cls, name):
        name = normalize_pkg(name)
        if name not in cls.packages_factory:
            cls.packages_factory[name] = Package(name)
        return cls.packages_factory[name]

    def to_dict(self):
        versions_dict = {}
        for version in self.available_versions:
            if self.available_versions[version]:
                versions_dict[version] = self.available_versions[version].to_dict()
            else:
                versions_dict[version] = None

        return {"name": self.name, "versions": versions_dict,
                "popularity": self.popularity}

    @classmethod
    def save(cls, path):
        pkgs_dict = {}
        for name, pkg in cls.packages_factory.items():
            pkgs_dict[name] = pkg.to_dict()
        with open(path, "w") as f:
            json.dump(pkgs_dict, f, indent=2)

    @classmethod
    def from_json(cls, path):
        with open(path, "r") as f:
            json_str = f.read()
        pkgs_dict = json.loads(json_str)
        for name, pkg_dict in pkgs_dict.items():
            versions = {}
            for v, v_meta_dict in pkg_dict["versions"].items():
                if v_meta_dict is not None:
                    versions[v] = versionMeta(top_level=set(v_meta_dict["top"]),
                                              requirements_dict=v_meta_dict["requirements"],
                                                cost=v_meta_dict["cost"])
            pkg = Package(name, versions, pkg_dict["popularity"])
            cls.packages_factory[name] = pkg

    @classmethod
    def get_top_mods(cls, name, version, include_indirect=False):
        version_meta = cls.packages_factory[name].available_versions[version]
        top_mods = version_meta.top_level
        top_mods = copy.deepcopy(top_mods)
        if not include_indirect:
            return top_mods
        else:
            indirect_pkgs_dict = version_meta.requirements_dict
            for pkg_name in indirect_pkgs_dict:
                version = indirect_pkgs_dict[pkg_name][1]
                top_mods |= cls.get_top_mods(pkg_name, version, include_indirect=False)
            return top_mods

    # before calling this function, make sure every package you desired is in the factory
    # todo: temporarily assume self-contained
    @classmethod
    def dep_matrix(cls):
        names_with_version = []
        for name, pkg in cls.packages_factory.items():
            for version in pkg.available_versions:
                names_with_version.append(name + "==" + version)
        names_with_version = sorted(names_with_version)
        df = pd.DataFrame(columns=names_with_version, index=names_with_version).fillna(0)
        for name, pkg in cls.packages_factory.items():
            for version, version_meta in pkg.available_versions.items():
                assert version_meta is not None
                for req, op_version in version_meta.requirements_dict.items():
                    req_str = req + op_version[0] + op_version[1]
                    df.loc[req_str, name + "==" + version] = 1

    @classmethod
    def cost_dict(cls):
        costs = {}
        for name, pkg in cls.packages_factory.items():
            for version, version_meta in pkg.available_versions.items():
                costs[name + "==" + version] = version_meta.cost
        return costs

    @classmethod
    def deps_dict(cls):
        deps = {}
        for name, pkg in cls.packages_factory.items():
            deps[name] = {}
            for version, version_meta in pkg.available_versions.items():
                deps[name][version] = {}
                for req, op_version in version_meta.requirements_dict.items():
                    deps[name][version][req] = op_version[1]
        return deps

trace = None
costs = None
