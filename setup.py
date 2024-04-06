import os
from setuptools import setup, find_packages
from distutils.util import convert_path


main_ns = {}
ver_path = convert_path("econuy/_version.py")
with open(ver_path) as ver_file:
    exec(ver_file.read(), main_ns)


packages = find_packages(".", exclude=["*.test", "*.test.*"])
with open("requirements.in", "r") as f:
    requirements = [line.strip() for line in f.readlines() if not line.startswith("#")]
with open("requirements-dev.in", "r") as f:
    dev_requirements = [
        line.strip()
        for line in f.readlines()
        if not line.startswith("-c") and not line.startswith("#")
    ]

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="econuy",
    version=main_ns["__version__"],
    description="Wrangling Uruguayan economic data so you don't have to.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Rafael Xavier",
    license="GPL-3.0",
    url="https://github.com/rxavier/econuy",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    keywords=["uruguay", "economy", "economic", "statistics", "data"],
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
    },
    packages=packages,
    include_package_data=True,
    python_requires=">=3.10",
)
