import re
import tempfile
import time
import zipfile
import datetime as dt
from random import randint
from io import BytesIO
from os import path, listdir
from typing import Optional
from urllib.error import HTTPError, URLError

import pandas as pd
import numpy as np
import requests
from pandas.tseries.offsets import MonthEnd
from bs4 import BeautifulSoup
from opnieuw import retry
from selenium.webdriver.remote.webdriver import WebDriver

from econuy.core import Pipeline
from econuy.transform import rebase, resample
from econuy.utils import metadata
from econuy.utils.chromedriver import _build
from econuy.utils.sources import urls
from econuy.utils.extras import investing_headers


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def gdp(driver: WebDriver = None) -> pd.DataFrame:
    """Get seasonally adjusted real GDP for Argentina and Brazil.

    This function requires a Selenium webdriver. It can be provided in the
    driver parameter, or it will attempt to configure a Chrome webdriver.

    Parameters
    ----------
    driver : selenium.webdriver.chrome.webdriver.WebDriver, default None
        Selenium webdriver for scraping. If None, build a Chrome webdriver.

    Returns
    -------
    Quarterly real GDP : pd.DataFrame

    """
    name = "regional_gdp"

    if driver is None:
        driver = _build()
    driver.get(urls[name]["dl"]["arg_new"])
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "lxml")
    driver.quit()
    url = soup.find_all(href=re.compile("desest"))[0]["href"]
    full_url = f"https://www.indec.gob.ar{url}"
    arg = pd.read_excel(full_url, skiprows=3, usecols="D").dropna(how="all")
    arg.index = pd.date_range(start="2004-03-31", freq="Q-DEC", periods=len(arg))
    arg_old = pd.read_excel(urls[name]["dl"]["arg_old"], skiprows=7, usecols="D").dropna(how="all")
    arg_old.index = pd.date_range(start="1993-03-31", freq="Q-DEC", periods=len(arg_old))
    arg = pd.concat([arg, arg_old], axis=1)
    for row in reversed(range(len(arg))):
        if pd.isna(arg.iloc[row, 0]):
            arg.iloc[row, 0] = arg.iloc[row, 1] / arg.iloc[row + 1, 1] * arg.iloc[row + 1, 0]
    arg = arg.iloc[:, [0]]

    r = requests.get(urls[name]["dl"]["bra"])
    temp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(BytesIO(r.content), "r") as f:
        f.extractall(path=temp_dir.name)
    path_temp = path.join(temp_dir.name, listdir(temp_dir.name)[0])
    bra = pd.read_excel(
        path_temp, usecols="Q", skiprows=3, sheet_name="Val encad preços 95 com ajuste"
    )
    bra.index = pd.date_range(start="1996-03-31", freq="Q-DEC", periods=len(bra))

    output = pd.concat([arg, bra], axis=1).div(1000)
    output.columns = ["Argentina", "Brasil"]
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Regional",
        currency="-",
        inf_adj="Const.",
        seas_adj="SA",
        unit="Miles de millones",
        ts_type="Flujo",
        cumperiods=1,
    )
    metadata._modify_multiindex(output, levels=[3], new_arrays=[["ARS", "BRL"]])

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def monthly_gdp() -> pd.DataFrame:
    """Get monthly GDP data.

    Countries/aggregates selected are Argentina and Brazil.

    Returns
    -------
    Daily policy interest rates : pd.DataFrame

    """
    name = "regional_monthly_gdp"

    arg = pd.read_excel(urls[name]["dl"]["arg"], usecols="C", skiprows=4).dropna(how="all")
    arg.index = pd.date_range(start="2004-01-31", freq="M", periods=len(arg))

    bra = pd.read_csv(urls[name]["dl"]["bra"], sep=";", index_col=0, decimal=",")
    bra.index = pd.date_range(start="2003-01-31", freq="M", periods=len(bra))

    output = pd.concat([arg, bra], axis=1)
    output.columns = ["Argentina", "Brasil"]
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Regional",
        currency="-",
        inf_adj="Const.",
        seas_adj="SA",
        ts_type="Flujo",
        cumperiods=1,
    )
    metadata._modify_multiindex(output, levels=[3], new_arrays=[["ARS", "BRL"]])
    output = rebase(output, start_date="2010-01-01", end_date="2010-12-31")

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def cpi() -> pd.DataFrame:
    """Get consumer price index for Argentina and Brazil.

    Returns
    -------
    Monthly CPI : pd.DataFrame

    """
    name = "regional_cpi"

    arg = requests.get(urls[name]["dl"]["ar"], params=urls[name]["dl"]["ar_payload"])
    arg = pd.read_html(arg.content)[0]
    arg.set_index("Fecha", drop=True, inplace=True)
    arg.index = pd.to_datetime(arg.index, format="%d/%m/%Y")
    arg.columns = ["nivel"]
    arg = arg.divide(10)

    arg_unoff = pd.read_excel(urls[name]["dl"]["ar_unofficial"])
    arg_unoff.set_index("date", drop=True, inplace=True)
    arg_unoff.index = arg_unoff.index + MonthEnd(0)
    arg_unoff = arg_unoff.loc[
        (arg_unoff.index >= "2006-12-01") & (arg_unoff.index <= "2016-12-01"), "index"
    ]
    arg_unoff = arg_unoff.to_frame().pct_change(periods=1).multiply(100).dropna()
    arg_unoff.columns = ["nivel"]
    arg = (
        arg.append(arg_unoff)
        .reset_index()
        .drop_duplicates(subset="index", keep="last")
        .set_index("index", drop=True)
        .sort_index()
    )
    arg = arg.divide(100).add(1).cumprod()

    bra_r = requests.get(urls[name]["dl"]["bra"])
    bra = pd.DataFrame(bra_r.json())[["v"]]
    bra.index = pd.date_range(start="1979-12-31", freq="M", periods=len(bra))
    bra = bra.apply(pd.to_numeric, errors="coerce")
    bra = bra.divide(100).add(1).cumprod()

    output = pd.concat([arg, bra], axis=1)
    output.columns = ["Argentina", "Brasil"]
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Regional",
        currency="-",
        inf_adj="No",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )
    metadata._modify_multiindex(output, levels=[3], new_arrays=[["ARS", "BRL"]])
    output = rebase(output, start_date="2010-10-01", end_date="2010-10-31")

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def embi_spreads() -> pd.DataFrame:
    """Get EMBI spread for Argentina, Brazil and the EMBI Global.

    Returns
    -------
    Daily 10-year government bond spreads : pd.DataFrame

    """
    name = "regional_embi_spreads"

    global_ = pd.read_excel(
        urls[name]["dl"]["global"], usecols="A:B", skiprows=1, index_col=0, parse_dates=True
    )
    global_ = global_.loc[~pd.isna(global_.index)].mul(100)
    region = []
    for cnt in ["argentina", "brasil"]:
        r = requests.get(urls[name]["dl"][cnt])
        aux = pd.DataFrame(r.json())
        aux.set_index(0, drop=True, inplace=True)
        aux.drop("Fecha", inplace=True)
        aux = aux.replace(",", ".", regex=True).apply(pd.to_numeric)
        aux.index = pd.to_datetime(aux.index, format="%d-%m-%Y")
        aux.sort_index(inplace=True)
        aux.columns = [cnt]
        region.append(aux)
    region = region[0].join(region[1]).interpolate(limit_area="inside")
    output = region.join(global_, how="left").interpolate(method="linear", limit_area="inside")
    output.columns = ["Argentina", "Brasil", "EMBI Global"]
    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Regional",
        currency="USD",
        inf_adj="No",
        seas_adj="NSA",
        unit="PBS",
        ts_type="-",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def embi_yields(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """Get EMBI yields for Argentina, Brazil and the EMBI Global.

    Yields are calculated by adding EMBI spreads to the 10-year US Treasury
    bond rate.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Daily 10-year government bonds interest rates : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get("global_long_rates")
    treasuries = pipeline.dataset["Estados Unidos"]
    pipeline.get("regional_embi_spreads")
    spreads = pipeline.dataset

    treasuries = treasuries.reindex(spreads.index).interpolate(
        method="linear", limit_direction="forward"
    )
    output = spreads.div(100).add(treasuries.squeeze(), axis=0)
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Regional",
        currency="USD",
        inf_adj="No",
        seas_adj="NSA",
        unit="Tasa",
        ts_type="-",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def nxr() -> pd.DataFrame:
    """Get USDARS and USDBRL.

    Returns
    -------
    Daily exchange rates : pd.DataFrame

    """
    name = "regional_nxr"

    arg = []
    for dollar in ["ar", "ar_unofficial"]:
        r = requests.get(urls[name]["dl"][dollar])
        aux = pd.DataFrame(r.json())[[0, 2]]
        aux.set_index(0, drop=True, inplace=True)
        aux.drop("Fecha", inplace=True)
        aux = aux.replace(",", ".", regex=True).apply(pd.to_numeric)
        aux.index = pd.to_datetime(aux.index, format="%d-%m-%Y")
        aux.sort_index(inplace=True)
        aux.columns = [dollar]
        arg.append(aux)
    arg = arg[0].join(arg[1], how="left")
    arg.columns = ["Argentina - oficial", "Argentina - informal"]

    r = requests.get(urls[name]["dl"]["bra"])
    bra = pd.DataFrame(r.json())
    bra = [(x["VALDATA"], x["VALVALOR"]) for x in bra["value"]]
    bra = pd.DataFrame.from_records(bra).dropna(how="any")
    bra.set_index(0, inplace=True)
    bra.index = pd.to_datetime(bra.index.str[:-4], format="%Y-%m-%dT%H:%M:%S").tz_localize(None)
    bra.columns = ["Brasil"]

    output = arg.join(bra, how="left").interpolate(method="linear", limit_area="inside")
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Regional",
        currency="USD",
        inf_adj="No",
        seas_adj="NSA",
        unit="Tasa",
        ts_type="-",
        cumperiods=1,
    )
    metadata._modify_multiindex(
        output,
        levels=[3, 5],
        new_arrays=[["ARS", "ARS", "BRL"], ["ARS/USD", "ARS/USD", "BRL/USD"]],
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def policy_rates() -> pd.DataFrame:
    """Get central bank policy interest rates data.

    Countries/aggregates selected are Argentina and Brazil.

    Returns
    -------
    Daily policy interest rates : pd.DataFrame

    """
    name = "regional_policy_rates"

    r = requests.get(urls[name]["dl"]["main"])
    temp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(BytesIO(r.content), "r") as f:
        f.extractall(path=temp_dir.name)
        path_temp = path.join(temp_dir.name, "WEBSTATS_CBPOL_D_DATAFLOW_csv_row.csv")
        raw = pd.read_csv(
            path_temp, usecols=[0, 1, 3], index_col=0, header=2, parse_dates=True
        ).dropna(how="all")
    output = raw.apply(pd.to_numeric, errors="coerce").interpolate(
        method="linear", limit_area="inside"
    )
    output.columns = ["Argentina", "Brasil"]
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Regional",
        currency="-",
        inf_adj="No",
        seas_adj="NSA",
        unit="Tasa",
        ts_type="-",
        cumperiods=1,
    )
    metadata._modify_multiindex(output, levels=[3], new_arrays=[["ARS", "BRL"]])

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def stocks(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """Get stock market index data in USD terms.

    Indexes selected are MERVAL and BOVESPA.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Daily stock market index in USD terms: pd.DataFrame

    """
    name = "regional_stocks"

    end_date_dt = dt.datetime(2000, 1, 1)
    start_date_dt = dt.datetime(2000, 1, 1)
    aux = []
    while end_date_dt < dt.datetime.now():
        end_date_dt = start_date_dt + dt.timedelta(days=5000)
        params = {
            "curr_id": "13376",
            "smlID": str(randint(1000000, 99999999)),
            "header": "S&amp;P Merval Historical Data",
            "st_date": start_date_dt.strftime("%m/%d/%Y"),
            "end_date": end_date_dt.strftime("%m/%d/%Y"),
            "interval_sec": "Daily",
            "sort_col": "date",
            "sort_ord": "DESC",
            "action": "historical_data",
        }
        r = requests.post(urls[name]["dl"]["arg"], headers=investing_headers, data=params)
        aux.append(pd.read_html(r.content, match="Price", index_col=0, parse_dates=True)[0])
        start_date_dt = end_date_dt + dt.timedelta(days=1)
    arg = pd.concat(aux, axis=0)[["Price"]].sort_index()

    bra = pd.read_csv(urls[name]["dl"]["bra"], index_col=0, parse_dates=True)[["Close"]]
    bra = bra.loc[bra.index >= "2000-01-01"]

    pipeline.get("regional_nxr")
    converters = pipeline.dataset
    converters.columns = converters.columns.get_level_values(0)
    arg = pd.merge_asof(
        arg, converters[["Argentina - informal"]], left_index=True, right_index=True
    )
    arg = (arg.iloc[:, 0] / arg.iloc[:, 1]).to_frame()
    arg.columns = ["Argentina"]
    bra = pd.merge_asof(bra, converters[["Brasil"]], left_index=True, right_index=True)
    bra = (bra.iloc[:, 0] / bra.iloc[:, 1]).to_frame()
    bra.columns = ["Brasil"]

    output = arg.join(bra, how="left").interpolate(method="linear", limit_area="inside")
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Regional",
        currency="USD",
        inf_adj="No",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )
    output = rebase(output, start_date="2019-01-02").dropna(how="all")

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def rxr(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """Get real exchange rates vis-á-vis the US dollar for Argentina and Brasil .

    Returns
    -------
    Monthly real exchange rate : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()

    proc = _ifs(pipeline=pipeline)

    output = pd.DataFrame()
    output["Argentina"] = proc["Argentina - oficial"] * proc["US.PCPI_IX"] / proc["ARG CPI"]
    output["Brasil"] = proc["Brasil"] * proc["US.PCPI_IX"] / proc["BRA CPI"]
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Regional",
        currency="-",
        inf_adj="-",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )
    metadata._modify_multiindex(output, levels=[3], new_arrays=[["ARS/USD", "BRL/USD"]])
    output = rebase(output, start_date="2019-01-01", end_date="2019-01-31").dropna(how="all")

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=30,
)
def _ifs(pipeline: Pipeline = None) -> pd.DataFrame:
    """Get extra CPI and exchange rate data from the IMF IFS.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    IMF data : pd.DataFrame
        CPI and XR for the US, Brazil and Argentina.

    """
    if pipeline is None:
        pipeline = Pipeline()

    url_ = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M."
    url_extra = ".?startPeriod=1970&endPeriod="
    ifs = []
    for country in ["US", "BR", "AR"]:
        for indicator in ["PCPI_IX", "ENDA_XDC_USD_RATE"]:
            base_url = f"{url_}{country}.{indicator}{url_extra}" f"{dt.datetime.now().year}"
            r_json = requests.get(base_url).json()
            data = r_json["CompactData"]["DataSet"]["Series"]["Obs"]
            try:
                data = pd.DataFrame(data)
                data.set_index("@TIME_PERIOD", drop=True, inplace=True)
            except ValueError:
                data = pd.DataFrame(
                    np.nan,
                    index=pd.date_range(start="1970-01-01", end=dt.datetime.now(), freq="M"),
                    columns=[f"{country}.{indicator}"],
                )
            if "@OBS_STATUS" in data.columns:
                data.drop("@OBS_STATUS", inplace=True, axis=1)
            data.index = pd.to_datetime(data.index, format="%Y-%m") + MonthEnd(1)
            data.columns = [f"{country}.{indicator}"]
            ifs.append(data)
    ifs = pd.concat(ifs, axis=1, sort=True).apply(pd.to_numeric)

    pipeline.get("regional_nxr")
    xr = pipeline.dataset
    xr = resample(xr, rule="M", operation="mean")
    xr.columns = xr.columns.get_level_values(0)
    pipeline.get("regional_cpi")
    prices = pipeline.dataset
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
