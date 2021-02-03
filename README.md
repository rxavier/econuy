<img src="https://i.imgur.com/o6cxmaP.jpg" width=400 style="margin-bottom:60px;">

  <a href="https://www.python.org/downloads/release/python-360/"><img src="https://img.shields.io/pypi/pyversions/econuy"></a>
  <a href="https://img.shields.io/pypi/l/econuy"><img src="https://img.shields.io/pypi/l/econuy"></a>
  <a href="https://pypi.org/project/econuy/"><img src="https://img.shields.io/pypi/v/econuy"></a>
  <a href="https://travis-ci.com/rxavier/econuy"><img src="https://travis-ci.com/rxavier/econuy.svg?branch=master"></a>
  <a href="https://econuy.readthedocs.io/en/latest/?badge=latest"><img src="https://readthedocs.org/projects/econuy/badge/?version=latest"></a>
  <a href="https://codecov.io/gh/rxavier/econuy"><img src="https://codecov.io/gh/rxavier/econuy/branch/master/graph/badge.svg"></a>


This project simplifies gathering and processing of Uruguayan economic statistics. Data is retrieved from (mostly) government sources and can be transformed in several ways (converting to dollars, calculating rolling averages, resampling to other frequencies, etc.).

If [this screenshot](https://i.imgur.com/Ku5OR0y.jpg) gives you anxiety, this package should be of interest.

# Webapp

A webapp with a limited but interactive version of econuy is available at https://econ.uy. Check out the [repo](https://github.com/rxavier/econuy-web) as well.

# Installation

* PyPI:

```bash
pip install econuy
```

* Git:

```bash
git clone https://github.com/rxavier/econuy.git
cd econuy
python setup.py install
```

# Usage

**[Read the documentation](https://econuy.readthedocs.io/)**

## The `Session()` class

This is the recommended entry point for the package. It allows setting up the common behavior for downloads, and holds the current working dataset.

```python
from econuy.session import Session

sess = Session(location="your/directory", revise_rows="nodup", only_get=False, log=1, inplace=False)
```

The `Session()` object is initialized with the `location`, `revise_rows`,  `only_get`, `dataset`, `log`, `logger` and `inplace` attributes.

* `location` controls where data will be saved and where it will be looked for when updating. It defaults to "econuy-data", and will create the directory if it doesn't exist. It can also be a SQLAlchemy Connection or Engine object.
* `revise_rows` controls the updating mechanism. It can be an integer, denoting how many rows from the data held on disk to replace with new data, or a string. In the latter case, `auto` indicates that the amount of rows to be replaced will be determined from the inferred data frequency, while `nodup` replaces existing data with new data for each time period found in both.
* `only_get` controls whether to get data from local sources or attempt to download it.
* `dataset` holds the current working dataset(s) and by default is initialized with an empty Pandas dataframe.
* `log` controls how logging works. `0`, don't log; `1`, log to console; `2`, log to console and file with default file; ``str``, log to console and file with filename=str.
* `logger` holds the current logger object from the logging module. Generally, the end user shouldn't set this manually.
* `inplace` controls whether transformation methods modify the current Session object inplace or whether they create a new instance with the same attributes (except `dataset`, of course).

### Session retrieval methods

#### `get()`

Downloads the basic datasets. These are generally as provided by official sources, except various Pandas transformations are performed to render nice looking dataframes with appropiate column names, time indexes and properly defined values. In select cases, I drop columns that I feel don't add relevant information for the target audience of this package, or that are inconsistent with other datasets.

Available options for the `dataset` argument can be found in [the datasets file](econuy/utils/datasets.py). English descriptions for these will be added in the future.

If you wanted CPI data:
```python
from econuy.session import Session

sess = Session(location="your/directory")
df = sess.get(dataset="cpi").dataset
```
Note that the previous code block accessed the `dataset` attribute in order to get a dataframe.

#### `get_custom()`

Gives access to predefined data pipelines that output frequently used data not provided officially or require the combination of available official sources. These are based on the datasets provided by `get()`, but are transformed to render data that you might find more immediately useful. As with `get()`, available options for the `dataset` argument can be found in [the datasets file](econuy/utils/datasets.py).

For example, the following calculates tradable CPI, non-tradable CPI, core CPI, residual CPI and Winsorized CPI. Also, it uses a SQL database for data updating and saving.
```python
from sqlalchemy import create_engine

from econuy.session import Session

eng = create_engine("dialect+driver://user:pwd@host:port/database")

sess = Session(location=eng)
df = sess.get_custom(dataset="cpi_measures")
```

### Session transformation methods

These class methods take a `Session()` object with a valid dataset and allow performing preset transformation pipelines. For example:

```python
from econuy.session import Session

sess = Session(location="your/directory")
df = sess.get(dataset="nxr_monthly").decompose(component="trend", method="x13", fallback="loess")
```
will return a the Session object, with the dataset attribute holding the trend component of the monthly nominal exchange rate.

Available transformation methods are 
* `resample()` - resample data to a different frequency, taking into account whether data is of stock or flow type.
* `chg_diff()` - calculate percent changes or differences for same period last year, last period or at annual rate.
* `decompose()` - seasonally decompose series into trend and seasonally adjusted components.
* `convert()` - convert to US dollars, constant prices or percent of GDP.
* `base_index()` - set a period or window as 100, scale rest accordingly
* `rolling()` - calculate rolling windows, either average or sum.

## Retrieval functions

If you don't want to go the `Session()` way, you can simply get your data from the functions under `econuy.retrieval`, for example `econuy.retrieval.fiscal_accounts.balance()`. While creating a Session object is recommended, this can be easier if you only plan on retrieving a single dataset.

## Dataframe/CSV headers

Metadata for each dataset is held in Pandas MultiIndexes with the following:

1) Indicator name
2) Topic or area
3) Frequency
4) Currency
5) Inflation adjustment
6) Unit
7) Seasonal adjustment
8) Type (stock or flow)
9) Cumulative periods

## External binaries and libraries

### unrar libraries	

The [patool](https://github.com/wummel/patool) library is used in order to access data provided in `.rar` format. This library requires that you have the unrar binaries in your system, which you can get from [here](https://www.rarlab.com/rar_add.htm).

### X13 ARIMA binary

If you want to use the `decompose()` functions with ``method="x13"``  you will need to supply the X13 binary (or place it somewhere reasonable and set `x13_binary="search"`). You can get it [from here](https://www.census.gov/srd/www/x13as/x13down_pc.html) for Windows or [from here](https://www.census.gov/srd/www/x13as/x13down_unix.html) for UNIX systems. For macOS you can compile it using the instructions found [here](https://github.com/christophsax/seasonal/wiki/Compiling-X-13ARIMA-SEATS-from-Source-for-OS-X) (choose the non-html version) or use my version (working under macOS Catalina) from [here](https://drive.google.com/open?id=1HxFoi57TWaBMV90NoOAbM8hWdZS9uoz_).

### Selenium webdrivers

Some retrieval functions need Selenium to be configured in order to scrape data. These functions include a `driver` parameter in which a Selenium Webdriver can be passed, or they will attempt to configure a Chrome webdriver, even downloading the chromedriver binary if needed (which still needs a Chrome installation).

### Ghostscript and Tkinter

This project uses [Camelot](https://github.com/camelot-dev/camelot) to extract data from PDF tables, which relies on these two dependencies. Installation instructions for these can be found [here](https://camelot-py.readthedocs.io/en/master/user/install-deps.html).

----

# Problems and plans

## Problems

This project is heavily based on getting data from online sources that could change without notice, causing methods that download data to fail. While I try to stay on my toes and fix these quickly, it helps if you create an issue when you find one of these (or even submit a fix!).

## Plans

* ~~I now realize this project would greatly benefit from OOP and plan to implement it next.~~
* ~~Tests.~~
* CLI.
* ~~Website.~~
* Automating data updates.
* ~~Visualization.~~ (I have decided that visualization should be up to the end-user. However, the [webapp](https://econ.uy) is available for this purpose).
* Translations for dataset descriptions and metadata.
