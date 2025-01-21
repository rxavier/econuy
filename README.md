<img src="https://i.imgur.com/o6cxmaP.jpg" width=400 style="margin-bottom:60px;">

  <a href="https://www.python.org/downloads/release/python-310/"><img src="https://img.shields.io/pypi/pyversions/econuy"></a>
  <a href="https://img.shields.io/pypi/l/econuy"><img src="https://img.shields.io/pypi/l/econuy"></a>
  <a href="https://pypi.org/project/econuy/"><img src="https://img.shields.io/pypi/v/econuy"></a>
  <a href="https://econuy.readthedocs.io/en/latest/?badge=latest"><img src="https://readthedocs.org/projects/econuy/badge/?version=latest"></a>

# Overview

This project simplifies gathering and processing of Uruguayan economic statistics. Data is retrieved from (mostly) government sources, processed into a familiar tabular format, tagged with useful metadata and can be transformed in several ways (converting to dollars, calculating rolling averages, resampling to other frequencies, etc.).

If [this screenshot](https://i.imgur.com/Ku5OR0y.jpg) gives you anxiety, this package should be of interest.

A webapp with a limited but interactive version of econuy is available at [econ.uy](https://econ.uy). Check out the [repo](https://github.com/rxavier/econuy-web) as well.

The most basic econuy workflow goes like this:

```python
from econuy import load_dataset, load_datasets_parallel

data1 = load_dataset("cpi")
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

### Cache directory

econuy saves and reads data to a directory which by default is at the system `home / .cache / econuy`. This can be modified for all data loading by setting `ECONUY_DATA_DIR` or directly in `load_dataset(data_dir=...)`.

### Dataset load branching

1. Check that the dataset exists in the `REGISTRY`.
2. Cache check:
  - If `skip_cache=True`, **download dataset**
  - If `skip_cache=False` (default):
    - Check whether the dataset exists in the cache.
      - If it exists:
        - Recency check:
          - If it was created in the last day, **return existing dataset**.
          - If it was created prior to the last day and `skip_update=False`, **download dataset**.
          - If it was created prior to the last day and `skip_update=True`, **return existing dataset**.
      - If it does not exist, **download dataset**
3. If the dataset was downloaded, try to update the cache:
- Validation:
  - If `force_overwrite=True`, **overwrite dataset**.
  - If `force_overwrite=False` (default):
    - If the new dataset is similar to the cached dataset, **overwrite dataset**.
    - If the new dataset is not similar to the cached dataset, **do not overwrite dataset**.

### Loading and transforming data

```python
from econuy import load_dataset, load_datasets_parallel


# load a single dataset
data1 = load_dataset("cpi")

# load a single dataset and chain transformations
data2 = (
    load_dataset("fiscal_balance_nonfinancial_public_sector")
    .select(names="Ingresos: SPNF")
    .resample("QE-DEC", "sum")
    .decompose(method="x13", component="t-c")
    .filter(start_date="2014-01-01")
    )
```
This returns a `Dataset` object, which contains a `Metadata` object.

You can also load multiple datasets fast:
```python
# load multiple datasets using threads or processes
data3 = load_datasets_parallel(["nxr_monthly", "ppi"])
```

### Finding datasets

```python
from econuy.utils.operations import REGISTRY


REGISTRY.list_available()
REGISTRY.list_by_area("activity")
```
### Dataset metadata

Datasets include the following metadata per indicator:

1. Indicator name
2. Area
3. Frequency
4. Currency
5. Inflation adjustment
6. Unit
7. Seasonal adjustment
8. Type (stock or flow)
9. Cumulative periods

### Transformation methods

`Dataset` objects have multiple methods to transform their underlying data and update their metadata.

* `resample()` - resample data to a different frequency, taking into account whether data is of stock or flow type.
* `chg_diff()` - calculate percent changes or differences for same period last year, last period or at annual rate.
* `decompose()` - seasonally decompose series into trend or seasonally adjusted components.
* `convert()` - convert to US dollars, constant prices or percent of GDP.
* `rebase()` - set a period or window as 100, scale rest accordingly
* `rolling()` - calculate rolling windows, either average or sum.

## External binaries and libraries

### unrar libraries

The [patool](https://github.com/wummel/patool) package is used in order to access data provided in `.rar` format. This package requires that you have the `unrar` binaries in your system, which in most cases you should already have. You can can get them from [here](https://www.rarlab.com/rar_add.htm) if you don't.

----

# Caveats

This project is heavily based on getting data from online sources that could change without notice, causing methods that download data to fail. While I try to stay on my toes and fix these quickly, it helps if you create an issue when you find one of these (or even submit a fix!).
