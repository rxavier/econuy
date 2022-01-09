import os
from setuptools import setup, find_packages
from distutils.util import convert_path


main_ns = {}
ver_path = convert_path("econuy/_version.py")
with open(ver_path) as ver_file:
    exec(ver_file.read(), main_ns)


packages = find_packages(".", exclude=["*.test", "*.test.*"])

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
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    keywords=["uruguay", "economy", "economic", "statistics", "data"],
    install_requires=[
        "pandas",
        "openpyxl",
        "statsmodels",
        "requests",
        "beautifulsoup4",
        "lxml",
        "opnieuw",
        "sqlalchemy",
        "selenium",
        "chromedriver-autoinstaller",
        "python-dotenv",
        "patool",
        "xlrd",
    ],
    extras_require={
        "pgsql": ["psycopg2"],
        "dev": [
            "psycopg2",
            "sphinx",
            "sphinx-autobuild",
            "sphinx-autodoc-typehints",
            "recommonmark",
            "sphinx-rtd-theme",
            "pytest",
            "coverage",
            "autopep8",
            "pre-commit",
            "jupyter",
            "matplotlib",
        ],
    },
    packages=packages,
    include_package_data=True,
    python_requires=">=3.7",
)
