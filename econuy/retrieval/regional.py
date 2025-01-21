import re
import os
import tempfile
import time
import zipfile
import datetime as dt
import ssl
from io import BytesIO
from os import path, listdir

import pandas as pd
import numpy as np
import httpx
from pandas.tseries.offsets import MonthEnd
from dotenv import load_dotenv

from econuy import load_dataset
from econuy.base import Dataset, DatasetMetadata
from econuy.utils.chromedriver import _build
from econuy.utils.operations import get_download_sources, get_name_from_function
from econuy.utils.retrieval import get_certs_path


load_dotenv()
FRED_API_KEY = os.environ.get("FRED_API_KEY")


def regional_gdp() -> Dataset:
    """Get seasonally adjusted real GDP for Argentina and Brazil.

    Returns
    -------
    Quarterly real GDP : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    driver = _build()
    driver.get(sources["arg_new"])
    time.sleep(5)
    source = driver.page_source
    driver.quit()
    url = re.findall(r"/ftp/cuadros/economia/.+desest.+\.xls", source)[0]
    full_url = f"https://www.indec.gob.ar{url}"
    arg = pd.read_excel(full_url, skiprows=3, usecols="C").dropna(how="all")
    arg.index = pd.date_range(start="2004-03-31", freq="QE-DEC", periods=len(arg))
    arg_old = pd.read_excel(sources["arg_old"], skiprows=7, usecols="D").dropna(
        how="all"
    )
    arg_old.index = pd.date_range(
        start="1993-03-31", freq="QE-DEC", periods=len(arg_old)
    )
    arg = pd.concat([arg, arg_old], axis=1)
    for row in reversed(range(len(arg))):
        if pd.isna(arg.iloc[row, 0]):
            arg.iloc[row, 0] = (
                arg.iloc[row, 1] / arg.iloc[row + 1, 1] * arg.iloc[row + 1, 0]
            )
    arg = arg.iloc[:, [0]]

    r = httpx.get(sources["bra"])
    temp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(BytesIO(r.content), "r") as f:
        f.extractall(path=temp_dir.name)
    path_temp = path.join(temp_dir.name, listdir(temp_dir.name)[0])
    bra = pd.read_excel(
        path_temp, usecols="Q", skiprows=3, sheet_name="Val encad preços 95 com ajuste"
    )
    bra.index = pd.date_range(start="1996-03-31", freq="QE-DEC", periods=len(bra))

    output = pd.concat([arg, bra], axis=1).div(1000)
    output.columns = ["Argentina", "Brasil"]
    output = output.rename_axis(None)

    spanish_names = output.columns
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Regional",
        "currency": "-",
        "inflation_adjustment": "Const.",
        "unit": "Billions",
        "seasonal_adjustment": "Seasonally adjusted",
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, currency in zip(ids, ["ARS", "BRL"]):
        metadata.update_indicator_metadata_value(indicator, "currency", currency)
    dataset = Dataset(name, output, metadata)

    return dataset


def regional_monthly_gdp() -> Dataset:
    """Get monthly GDP data.

    Countries/aggregates selected are Argentina and Brazil.

    Returns
    -------
    Monthly GDP : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    arg = pd.read_excel(sources["arg"], usecols="E", skiprows=3).dropna(how="all")
    arg.index = pd.date_range(start="2004-01-31", freq="ME", periods=len(arg))

    bra = pd.read_csv(sources["bra"], sep=";", index_col=0, decimal=",")
    bra.index = pd.date_range(start="2003-01-31", freq="ME", periods=len(bra))

    output = pd.concat([arg, bra], axis=1)
    output.columns = ["Argentina", "Brasil"]
    output = output.rename_axis(None)

    spanish_names = output.columns
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Regional",
        "currency": "-",
        "inflation_adjustment": "Const.",
        "unit": "-",
        "seasonal_adjustment": "Seasonally adjusted",
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, currency in zip(ids, ["ARS", "BRL"]):
        metadata.update_indicator_metadata_value(indicator, "currency", currency)
    dataset = Dataset(name, output, metadata).rebase("2010-01-01", "2010-12-31")
    dataset.metadata.update_dataset_metadata({"unit": "2010=100"})
    dataset.transformed = False

    return dataset


def regional_cpi() -> pd.DataFrame:
    """Get consumer price index for Argentina and Brazil.

    Returns
    -------
    Monthly CPI : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    certs = get_certs_path("bcra")
    ssl_context = ssl.create_default_context(cafile=str(certs))
    arg = httpx.get(
        sources["ar"].format(
            end_date=dt.datetime.now().strftime("%Y-%m-%d"),
        ),
        verify=ssl_context,
    )
    arg = pd.read_html(arg.content)[0]
    arg.set_index("Fecha", drop=True, inplace=True)
    arg.index = pd.to_datetime(arg.index, format="%d/%m/%Y")
    arg.columns = ["nivel"]
    arg = arg.divide(10)

    arg_unoff = pd.read_excel(sources["ar_unofficial"])
    arg_unoff.set_index("date", drop=True, inplace=True)
    arg_unoff.index = arg_unoff.index + MonthEnd(0)
    arg_unoff = arg_unoff.loc[
        (arg_unoff.index >= "2006-12-01") & (arg_unoff.index <= "2016-12-01"), "index"
    ]
    arg_unoff = arg_unoff.to_frame().pct_change(periods=1).multiply(100).dropna()
    arg_unoff.columns = ["nivel"]
    arg = (
        pd.concat([arg, arg_unoff])
        .reset_index()
        .drop_duplicates(subset="index", keep="last")
        .set_index("index", drop=True)
        .sort_index()
    )
    arg = arg.divide(100).add(1).cumprod()

    bra_r = httpx.get(sources["bra"].format(date=dt.datetime.now().strftime("%Y%m")))
    bra = pd.DataFrame(bra_r.json())[["v"]]
    bra.index = pd.date_range(start="1979-12-31", freq="ME", periods=len(bra))
    bra = bra.apply(pd.to_numeric, errors="coerce")
    bra = bra.divide(100).add(1).cumprod()

    output = pd.concat([arg, bra], axis=1)
    output.columns = ["Argentina", "Brasil"]
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Regional",
        "currency": "-",
        "inflation_adjustment": None,
        "unit": "-",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, currency in zip(ids, ["ARS", "BRL"]):
        metadata.update_indicator_metadata_value(indicator, "currency", currency)
    dataset = Dataset(name, output, metadata).rebase("2010-01-01", "2010-12-31")
    dataset.metadata.update_dataset_metadata({"unit": "2010=100"})
    dataset.transformed = False

    return dataset


def regional_embi_spreads() -> Dataset:
    """Get EMBI spread for Argentina, Brazil and the EMBI Global.

    Returns
    -------
    Daily 10-year government bond spreads : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], usecols="A:B,E,G", skiprows=1, index_col=0)
    output = (
        raw.loc[~pd.isna(raw.index)]
        .mul(100)
        .rename(columns={"Global": "EMBI Global"})[
            ["Argentina", "Brasil", "EMBI Global"]
        ]
    )
    output.index = pd.to_datetime(output.index)
    output = output.apply(pd.to_numeric, errors="coerce").sort_index()
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Regional",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Bps",
        "seasonal_adjustment": None,
        "frequency": "D",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def regional_embi_yields(*args, **kwargs) -> Dataset:
    """Get EMBI yields for Argentina, Brazil and the EMBI Global.

    Yields are calculated by adding EMBI spreads to the 10-year US Treasury
    bond rate.

    Returns
    -------
    Daily 10-year government bonds interest rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    if FRED_API_KEY is None:
        raise ValueError(
            "FRED_API_KEY not found. Get one at https://fredaccount.stlouisfed.org/apikeys and set it as an environment variable."
        )

    r = httpx.get(sources["treasury"].format(FRED_API_KEY))
    treasuries = pd.DataFrame.from_records(r.json()["observations"]).set_index("date")[
        ["value"]
    ]
    treasuries.index = pd.to_datetime(treasuries.index)
    treasuries["value"] = pd.to_numeric(
        treasuries["value"], errors="coerce"
    ).interpolate()

    spreads = load_dataset("regional_embi_spreads", *args, **kwargs).to_named()

    treasuries = treasuries.reindex(spreads.index).interpolate(
        method="linear", limit_direction="forward"
    )
    output = spreads.div(100).add(treasuries.squeeze(), axis=0)
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Regional",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Rate",
        "seasonal_adjustment": None,
        "frequency": "D",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def regional_nxr() -> Dataset:
    """Get USDARS and USDBRL.

    Returns
    -------
    Daily exchange rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    arg = []
    for dollar in ["ar", "ar_unofficial"]:
        r = httpx.get(
            sources[dollar].format(date=dt.datetime.now().strftime("%d-%m-%Y")),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            },
        )
        aux = pd.DataFrame(r.json())[[0, 2]]
        aux = aux.set_index(0, drop=True)
        aux = aux.drop("Fecha")
        aux = aux.replace(",", ".", regex=True).apply(pd.to_numeric)
        aux.index = pd.to_datetime(aux.index, format="%d/%m/%Y")
        aux = aux.sort_index()
        aux.columns = [dollar]
        arg.append(aux)
    arg = arg[0].join(arg[1], how="left")
    arg.columns = ["Argentina - oficial", "Argentina - informal"]

    r = httpx.get(sources["bra"])
    bra = pd.DataFrame(r.json())
    bra = [(x["VALDATA"], x["VALVALOR"]) for x in bra["value"]]
    bra = pd.DataFrame.from_records(bra).dropna(how="any")
    bra = bra.set_index(0)
    bra.index = pd.to_datetime(bra.index.str[:-4]).tz_localize(None)
    bra.columns = ["Brasil"]

    output = arg.join(bra, how="left").interpolate(method="linear", limit_area="inside")
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Regional",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "-",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def regional_policy_rates() -> Dataset:
    """Get central bank policy interest rates data.

    Countries/aggregates selected are Argentina and Brazil.

    Returns
    -------
    Daily policy interest rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    r = httpx.get(sources["main"])
    temp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(BytesIO(r.content), "r") as f:
        f.extractall(path=temp_dir.name)
        path_temp = path.join(temp_dir.name, "WS_CBPOL_csv_row.csv")
        raw = pd.read_csv(path_temp, index_col=0)
    output = raw.loc[:, lambda x: x.columns.str.contains("D:Daily")]
    output.columns = output.iloc[0]
    output = output.loc[:, ["AR:Argentina", "BR:Brazil"]].iloc[8:].dropna(how="all")
    output.columns = ["Argentina", "Brasil"]
    output = output.apply(pd.to_numeric, errors="coerce").interpolate(
        method="linear", limit_area="inside"
    )
    output.index = pd.to_datetime(output.index)
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Regional",
        "currency": "-",
        "inflation_adjustment": None,
        "unit": "Rate",
        "seasonal_adjustment": None,
        "frequency": "D",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, currency in zip(ids, ["ARS", "BRL"]):
        metadata.update_indicator_metadata_value(indicator, "currency", currency)
    dataset = Dataset(name, output, metadata)

    return dataset


# def regional_stock_markets() -> pd.DataFrame:
#     """Get stock market index data in USD terms.

#     Indexes selected are MERVAL and BOVESPA.

#     Parameters
#     ----------
#     pipeline : econuy.core.Pipeline or None, default None
#         An instance of the econuy Pipeline class.

#     Returns
#     -------
#     Daily stock market index in USD terms: pd.DataFrame

#     """
#     name = get_name_from_function()
#     sources = get_download_sources(name)

#     yahoo = []
#     for series in ["arg", "bra"]:
#         aux = pd.read_csv(
#             sources[series].format(timestamp=dt.datetime.now().timestamp().__round__()),
#             index_col=0,
#             usecols=[0, 4],
#             parse_dates=True,
#         )
#         aux.columns = [series]
#         yahoo.append(aux)
#     output = pd.concat(yahoo, axis=1).interpolate(method="linear", limit_area="inside")
#     output.columns = ["MERVAL", "BOVESPA"]
#     output.rename_axis(None, inplace=True)
#     metadata._set(
#         output,
#         area="Global",
#         currency="USD",
#         inf_adj="No",
#         seas_adj="NSA",
#         ts_type="-",
#         cumperiods=1,
#     )
#     metadata._modify_multiindex(output, levels=[3], new_arrays=[["ARS", "BRL"]])
#     output = rebase(output, start_date="2019-01-02")

#     return output


def regional_rxr(*args, **kwargs) -> Dataset:
    """Get real exchange rates vis-á-vis the US dollar for Argentina and Brasil .

    Returns
    -------
    Monthly real exchange rate : Dataset

    """
    name = get_name_from_function()
    proc = _ifs(*args, **kwargs)

    output = pd.DataFrame()
    output["Argentina"] = (
        proc["Argentina - oficial"] * proc["US.PCPI_IX"] / proc["ARG CPI"]
    )
    output["Brasil"] = proc["Brasil"] * proc["US.PCPI_IX"] / proc["BRA CPI"]
    output = output.rename_axis(None).dropna(how="all")

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Regional",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "-",
        "seasonal_adjustment": None,
        "frequency": "D",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, unit in zip(ids, ["ARS/USD", "BRL/USD"]):
        metadata.update_indicator_metadata_value(indicator, "unit", unit)
    dataset = Dataset(name, output, metadata).rebase("2019-01-01", "2019-01-31")
    dataset.metadata.update_dataset_metadata({"unit": "2019-01=100"})
    dataset.transformed = False

    return dataset


def _ifs(*args, **kwargs) -> pd.DataFrame:
    """Get extra CPI and exchange rate data from the IMF IFS.

    Returns
    -------
    IMF data : pd.DataFrame
        CPI and XR for the US, Brazil and Argentina.

    """
    url_ = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M"
    url_extra = "?startPeriod=1970&endPeriod="
    ifs = []
    for country in ["US", "BR", "AR"]:
        for indicator in ["PCPI_IX", "ENDA_XDC_USD_RATE"]:
            base_url = (
                f"{url_}.{country}.{indicator}.{url_extra}{dt.datetime.now().year}"
            )
            r_json = httpx.get(base_url, timeout=30).json()
            data = r_json["CompactData"]["DataSet"]["Series"]["Obs"]
            try:
                data = pd.DataFrame(data)
                data.set_index("@TIME_PERIOD", drop=True, inplace=True)
            except ValueError:
                data = pd.DataFrame(
                    np.nan,
                    index=pd.date_range(
                        start="1970-01-01", end=dt.datetime.now(), freq="ME"
                    ),
                    columns=[f"{country}.{indicator}"],
                )
            if "@OBS_STATUS" in data.columns:
                data.drop("@OBS_STATUS", inplace=True, axis=1)
            data.index = pd.to_datetime(data.index, format="%Y-%m") + MonthEnd(1)
            data.columns = [f"{country}.{indicator}"]
            ifs.append(data)
    ifs = pd.concat(ifs, axis=1, sort=True).apply(pd.to_numeric)

    xr = load_dataset("regional_nxr", *args, **kwargs).resample("ME", "mean").to_named()
    prices = load_dataset("regional_cpi", *args, **kwargs).to_named()
    prices.columns = ["ARG CPI", "BRA CPI"]

    proc = pd.concat([xr, prices, ifs], axis=1)
    proc["Argentina - oficial"] = np.where(
        pd.isna(proc["Argentina - oficial"]),
        proc["AR.ENDA_XDC_USD_RATE"],
        proc["Argentina - oficial"],
    )
    proc["Argentina - informal"] = np.where(
        pd.isna(proc["Argentina - informal"]),
        proc["AR.ENDA_XDC_USD_RATE"],
        proc["Argentina - informal"],
    )
    proc["Brasil"] = np.where(
        pd.isna(proc["Brasil"]), proc["BR.ENDA_XDC_USD_RATE"], proc["Brasil"]
    )
    proc = proc[
        [
            "Argentina - oficial",
            "Argentina - informal",
            "Brasil",
            "ARG CPI",
            "BRA CPI",
            "US.PCPI_IX",
        ]
    ]

    return proc
