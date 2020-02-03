# ECON-UY

This project aims at simplifying gathering, processing and visualization (in the future) of Uruguay economic statistics. Data is retrieved from (mostly) government sources and can be transformed in several ways (converting to dollars, calculating rolling averages, resampling to other frequencies, etc.).

## Installation

* PyPI:

```
pip install econuy
```


* Git:

```
git clone https://github.com/rxavier/uy-econ.git your-directory
cd your-directory
python setup.py install
```

## Usage

### Retrieval

Getting data as-is is as simple as `from econuy.retrieval import [x]`, where [x] can be
* `cpi` | available methods: `get()`
* `nxr` | available methods: `get()`
* `rxr` | available methods: `get_official()` and `get_custom()`
* `fiscal_accounts` | available methods: `get()`
* `national_accounts` | available methods: `get()`
* `labor` | available methods: `get()`
* `reserves_chg` | available methods: `get()`
* `fx_spot_ff` | available methods: `get()`
* `commodity_index` | available methods: `get()`

So if you want to download CPI data, load it into a Pandas dataframe and save it on disk as a CSV, you would do

```
from econuy,retrevial import cpi

data = cpi.get(update=False, save=True)
```

#### Update and save parameters

All the `get()` functions under `retrieval` take these parameters (in the case of `fiscal_accounts` and `national_accounts`, these are actually `update_dir` and `save_dir`, but their behavior is analogous).

They can be a path-like string, a PathLike object from the `pathlib` package or bool. The first two simply indicate a path where to find files for updating or where to save output files. If `False`, no updating/saving will take place, i.e., data will be downloaded and that's it. If `True`, paths are set to default locations, specifically a `econuy-data` directory will be created within the working directory and files will be saved with preset names (nominal exchange will be saved as `econuy-data/nxr.csv`)

`update` is used for two reasons: 
1) To avoid downloading data if a file on disk has been modified within some set amount of time. For example, 25 days in the case of CPI data. Can be overriden if `force_update=True` is passed to these functions.
2) If there is new data to download, allow the user to "revise" only some rows of the old data, replacing it with newly downloaded data. This is controlled by the `revise_rows` parameter. So if `revise_rows=6`, existing CPI data on disk will have its last 6 months removed, which will be replaced with newly downloaded data.

`save` is more straightforward, in that it simply indicates where to put the CSV.

### Processing

Once data has been loaded it can be transformed. These functions are all under `processing` and allow the following:

* `convert` | available methods: `usd()`, `real()` and `pcgdp()`
* `seasonal` | available methods: `decompose()`
* `index` | available methods: `base_index()`
* `variations` | available methods: `chg_diff()`
* `freqs` | available methods: `freq_resample()` and `rolling()`

#### X13 ARIMA binary

If you want to use the seasonal `decompose()` function under `seasonal`  you will need to supply the X13 binary (or place it somewhere reasonable and set `x13_binary="search"`). You can get it [from here](https://www.census.gov/srd/www/x13as/x13down_pc.html) for Windows or [from here](https://www.census.gov/srd/www/x13as/x13down_unix.html) for UNIX systems. For macOS you can compile it using the instructions found [here](https://github.com/christophsax/seasonal/wiki/Compiling-X-13ARIMA-SEATS-from-Source-for-OS-X) or use my version (working under macOS Catalina) from [here](https://drive.google.com/open?id=1HxFoi57TWaBMV90NoOAbM8hWdZS9uoz_).

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

### Frequently used

`from econuy.frequent import frequent` gives access to a number of functions that combine retrieval and processing pipelines, and output frequently used datasets. For example, `inflation()` will download CPI data, calculate interannual inflation, monthly inflation, seasonally adjusted monthly inflation and trend monthly inflation.

## What next

* I now realize this project would greatly benefit from OOP and plan to implement it next.
* Tests.
* CLI.
* Handling everything with column multi-indexes really doesn't seem like the best way to go around this.
* Automating data updates.
