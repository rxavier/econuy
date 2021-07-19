<img src="https://i.imgur.com/o6cxmaP.jpg" width=400 style="margin-bottom:60px;">

  <a href="https://www.python.org/downloads/release/python-360/"><img src="https://img.shields.io/pypi/pyversions/econuy"></a>
  <a href="https://img.shields.io/pypi/l/econuy"><img src="https://img.shields.io/pypi/l/econuy"></a>
  <a href="https://pypi.org/project/econuy/"><img src="https://img.shields.io/pypi/v/econuy"></a>
  <a href="https://travis-ci.com/rxavier/econuy"><img src="https://travis-ci.com/rxavier/econuy.svg?branch=master"></a>
  <a href="https://econuy.readthedocs.io/en/latest/?badge=latest"><img src="https://readthedocs.org/projects/econuy/badge/?version=latest"></a>
  <a href="https://codecov.io/gh/rxavier/econuy"><img src="https://codecov.io/gh/rxavier/econuy/branch/master/graph/badge.svg"></a>

# Overview

This project simplifies gathering and processing of Uruguayan economic statistics. Data is retrieved from (mostly) government sources, processed into a familiar tabular format, tagged with useful metadata and can be transformed in several ways (converting to dollars, calculating rolling averages, resampling to other frequencies, etc.).

If [this screenshot](https://i.imgur.com/Ku5OR0y.jpg) gives you anxiety, this package should be of interest.

A webapp with a limited but interactive version of econuy is available at [econ.uy](https://econ.uy). Check out the [repo](https://github.com/rxavier/econuy-web) as well.

The most basic econuy workflow goes like this:

```python
from econuy.core import Pipeline

p = Pipeline()
p.get("labor_rates")
```

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

**[Full API documentation available at RTD](https://econuy.readthedocs.io/en/latest/api.html)**

## The `Pipeline()` class

This is the recommended entry point for the package. It allows setting up the common behavior for downloads, and holds the current working dataset.

```python
from econuy.core import Pipeline

p = Pipeline(location="your_directory")
```

### The `Pipeline.get()` method

Retrieves datasets (generally downloads them, unless the `download` attribute is `False` and the requested dataset exists at the `location`) and loads them into the `dataset` attribute as a Pandas DataFrame.

The `Pipeline.available_datasets()` method returns a `dict` with the available options.

```python
from econuy.core import Pipeline
from sqlalchemy import create_engine

eng = create_engine("dialect+driver://user:pwd@host:port/database")

p = Pipeline(location=eng)
p.get("industrial_production")
```

Which also shows that econuy supports SQLAlchemy `Engine` or `Connection` objects.

Note that every time a dataset is retrieved, `Pipeline` will
1. Check if a previous version exists at `location`. If it does, it will read it and combine it with the new data (unless `download=False`, in which case only existing data will be retrieved)
2. Save the dataset to `location`, unless the `always_save` attribute is set to `False` or no new data is available.

Data can be written and read to and from CSV or Excel files (controlled by the `read_fmt` and `save_fmt` attributes) or SQL (automatically determined from `location`).

### Dataset metadata

Metadata for each dataset is held in Pandas MultiIndexes with the following:

1. Indicator name
2. Topic or area
3. Frequency
4. Currency
5. Inflation adjustment
6. Unit
7. Seasonal adjustment
8. Type (stock or flow)
9. Cumulative periods

When writing, metadata can be included as dataset headers (Pandas MultiIndex columns), placed on another sheet if writing to Excel, or dropped. This is controlled by `read_header` and `save_header`.

### Pipeline transformation methods

`Pipeline` objects with a valid dataset can access 6 transformation methods that modify the held dataset.

* `resample()` - resample data to a different frequency, taking into account whether data is of stock or flow type.
* `chg_diff()` - calculate percent changes or differences for same period last year, last period or at annual rate.
* `decompose()` - seasonally decompose series into trend or seasonally adjusted components.
* `convert()` - convert to US dollars, constant prices or percent of GDP.
* `rebase()` - set a period or window as 100, scale rest accordingly
* `rolling()` - calculate rolling windows, either average or sum.

```python
from econuy.core import Pipeline

p = Pipeline()
p.get("balance_nfps")
p.convert(flavor="usd")
p.resample(rule="A-DEC", operation="sum")
```

### Saving the current dataset

While `Pipeline.get()` will generally save the retrieved dataset to `location`, transformation methods won't automatically write data.

However, `Pipeline.save()` can be used, which will overwrite the file on disk (or SQL table) with the contents in `dataset`.

## The `Session()` class

Like a `Pipeline`, except it can hold several datasets.

The `datasets` attribute is a `dict` of name-DataFrame pairs. Additionally, `Session.get()` accepts a sequence of strings representing several datasets.

Transformation and saving methods support a `select` parameter that determines which held datasets are considered.

```python
from econuy.session import Session

s = Session(location="your/directory")
s.get(["cpi", "nxr_monthly"])
s.get("commodity_index")
s.rolling(window=12, operation="mean", select=["nxr_monthly", "commodity_index"])
```

`Session.get_bulk()` makes it easy to get several datasets in one line.

```python
from econuy.session import Session

s = Session()
s.get_bulk("all")
```

```python
from econuy.session import Session

s = Session()
s.get_bulk("fiscal_accounts")
```

`Session.concat()` combines selected datasets into a single DataFrame with a common frequency, and adds it as a new key-pair in `datasets`.

## External binaries and libraries

### unrar libraries

The [patool](https://github.com/wummel/patool) package is used in order to access data provided in `.rar` format. This package requires that you have the `unrar` binaries in your system, which in most cases you should already have. You can can get them from [here](https://www.rarlab.com/rar_add.htm) if you don't.

### Selenium webdrivers

Some retrieval functions need Selenium to be configured in order to scrape data. These functions include a `driver` parameter in which a Selenium Webdriver can be passed, or they will attempt to configure a Chrome webdriver, even downloading the chromedriver binary if needed. This still requires an existing Chrome installation.

----

# Caveats and plans

## Caveats

This project is heavily based on getting data from online sources that could change without notice, causing methods that download data to fail. While I try to stay on my toes and fix these quickly, it helps if you create an issue when you find one of these (or even submit a fix!).

## Plans

* Implement a CLI.
* ~~Provide methods to make keeping an updated database easy~~. `Session.get_bulk()` mostly covers this.
* ~~Visualization.~~ (I have decided that visualization should be up to the end-user. However, the [webapp](https://econ.uy) is available for this purpose).
* Translations for dataset descriptions and metadata.
