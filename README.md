![GitHub Pipenv locked Python version](https://img.shields.io/github/pipenv/locked/python-version/rxavier/uy-econ)
[![PyPI version](https://badge.fury.io/py/econuy.svg)](https://badge.fury.io/py/econuy)

# ECON-UY

This project aims at simplifying gathering, processing and visualization (in the future) of Uruguay economic statistics. Data is retrieved from (mostly) government sources and can be transformed in several ways (converting to dollars, calculating rolling averages, resampling to other frequencies, etc.).

## Installation

* PyPI:

```
pip install econuy
```


* Git:

```
git clone https://github.com/rxavier/uy-econ.git
python setup.py install
```

## Usage

### The `Session()` class

#### Basics

This is the main entry point for the package. It allows setting up the common behavior for downloads, and holds the current working dataset.

```
from econuy.session import Session

session = Session(loc_dir="econuy-data",
                  revise_rows="nodup",
                  force_update=False)
```

The `Session()` object is initialized with the `loc_dir`, `revise_rows` and `force_update` attributes, plus the `dataset` attribute, which initially holds an empty Pandas dataframe. After each download and transformation method, `dataset` will hold the current working dataset.

* `loc_dir` controls where data will be saved and where it will be looked for when updating. It defaults to "econuy-data", and will create the directory if it doesn't exist.
* `revise_rows` controls the updating mechanism. It can be an integer, denoting how many rows from the data held on disk to replace with new data, or a string. In the latter case, `auto` indicates that the amount of rows to be replaced will be determined from the inferred data frequency, while `nodup` replaces existing data with new data for each time period found in both.
* `force_update` controls whether whether to redownload data even if existing data in disk was modified recently.

#### Methods

**get()** downloads the basic datasets.
```
session.get(self, dataset: str, update: bool = True, save: bool = True, 
            override: Optional[str] = None, final: bool = False, **kwargs)
```
Available options for the `dataset` argument are "cpi", "fiscal", "nxr", "nacciounts", "labor", "rxr_custom", "rxr_official", "commodity_index", "reserves" and "fx_spot_ff". Most are self explanatory.

`override` allows setting the CSV's filename to a different one than default (each dataset has a default, for example, "cpi.csv"). `final` controls whether to return the `Session()` object (if False), or to return the dataframe held by the object (if True). In any case, the following are equivalent, and will return a dataframe with consumer price index data:

```
df = session.get(dataset=cpi, final=True)

df = session.get(dataset=cpi).dataset
```

**get_tfm()** gives access to predefined data pipelines that output frequently used data.
```
session.get_tfm(self, dataset: str, update: bool = True, save: bool = True,
                override: Optional[str] = None, final: bool = False, **kwargs)
```
For example, `session.get_tfm(dataset="inflation")` downloads CPI data, calculates annual inflation (pct change from a year ago), monthly inflation, and seasonally adjusted and trend monthly inflation.

**Transformation methods** take a `Session()` object with a valid dataset and allow performing preset transformation pipelines. For example:
```
df = session.get(dataset="nxr").decompose(flavor="trend", outlier=True, trading=False, final=True)
```
will return a dataframe holding the trend component of nominal exchange rate.

Available transformation methods are 
* `freq_resample()` - resample data to a different frequency, taking into account whether data is of stock or flow type.
* `chg_diff()` - calculate percent changes or differences for same period last year, last period or at annual rate.
* `decompose()` - use X13-ARIMA to decompose series into trend and seasonally adjusted components.
* `unit_conv()` - convert to US dollars, constant prices or percent of GDP.
* `base_index()` - set a period or window as 100, scale rest accordingly
* `rollwindow()` - calculate rolling windows, either average or sum.

#### X13 ARIMA binary

If you want to use the `decompose()` method  you will need to supply the X13 binary (or place it somewhere reasonable and set `x13_binary="search"`). You can get it [from here](https://www.census.gov/srd/www/x13as/x13down_pc.html) for Windows or [from here](https://www.census.gov/srd/www/x13as/x13down_unix.html) for UNIX systems. For macOS you can compile it using the instructions found [here](https://github.com/christophsax/seasonal/wiki/Compiling-X-13ARIMA-SEATS-from-Source-for-OS-X) (choose the non-html version) or use my version (working under macOS Catalina) from [here](https://drive.google.com/open?id=1HxFoi57TWaBMV90NoOAbM8hWdZS9uoz_).

#### Dataframe/CSV headers

Metadata for each dataset is held in Pandas MultiIndexes with the following:

1) Indicator name
2) Topic or area
3) Frequency
4) Unit/currency
5) Current or inflation adjusted
6) Base index period(s) (if applicable)
7) Seasonal adjustment
8) Type (stock or flow)
9) Cumulative periods

## Word of warning

This project is heavily based on getting data from online sources that could change without notice, causing methods that download data to fail. While I try to stay on my toes and fix these quickly, it helps if you create an issue when you find one of these (or even submit a fix!).

## What next

* ~~I now realize this project would greatly benefit from OOP and plan to implement it next.~~
* Tests.
* CLI.
* Handling everything with column multi-indexes really doesn't seem like the best way to go around this.
* Automating data updates.
