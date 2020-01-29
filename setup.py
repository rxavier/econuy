import os
from setuptools import setup, find_packages
import json


def get_requirements_from_pipfile_lock(pipfile_lock=None):
    if pipfile_lock is None:
        pipfile_lock = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'Pipfile.lock'
        )
    lock_data = json.load(open(pipfile_lock))
    return [package_name for package_name in
            lock_data.get('default', {}).keys()]


packages = find_packages('.', exclude=['*.test', '*.test.*'])
pipfile_lock_requirements = get_requirements_from_pipfile_lock()

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="econuy",
    version="0.1.1",
    description="Download and process Uruguayan economic data.",
    long_description=long_description,
    author="Rafael Xavier",
    license="GPL-3.0",
    url="https://github.com/rxavier/uy-econ",
    keywords=["uruguay", "economy", "economic", "statistics", "data"],
    install_requires=pipfile_lock_requirements,
    packages=packages,
    python_requires=">=3.7"
)
