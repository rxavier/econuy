import re
import tempfile
import time
import zipfile
import datetime as dt
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
from econuy.utils.operations import get_download_sources, get_name_from_function


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def regional_gdp(driver: WebDriver = None) -> pd.DataFrame:
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
    name = get_name_from_function()
    sources = get_download_sources(name)

    if driver is None:
        driver = _build()
    driver.get(sources["arg_new"])
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "lxml")
    driver.quit()
    url = soup.find_all(href=re.compile("desest"))[0]["href"]
    full_url = f"https://www.indec.gob.ar{url}"
    arg = pd.read_excel(full_url, skiprows=3, usecols="C").dropna(how="all")
    arg.index = pd.date_range(start="2004-03-31", freq="Q-DEC", periods=len(arg))
    arg_old = pd.read_excel(sources["arg_old"], skiprows=7, usecols="D").dropna(how="all")
    arg_old.index = pd.date_range(start="1993-03-31", freq="Q-DEC", periods=len(arg_old))
    arg = pd.concat([arg, arg_old], axis=1)
    for row in reversed(range(len(arg))):
        if pd.isna(arg.iloc[row, 0]):
            arg.iloc[row, 0] = arg.iloc[row, 1] / arg.iloc[row + 1, 1] * arg.iloc[row + 1, 0]
    arg = arg.iloc[:, [0]]

    r = requests.get(sources["bra"])
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
def regional_monthly_gdp() -> pd.DataFrame:
    """Get monthly GDP data.

    Countries/aggregates selected are Argentina and Brazil.

    Returns
    -------
    Monthly GDP : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    arg = pd.read_excel(sources["arg"], usecols="C", skiprows=3).dropna(how="all")
    arg.index = pd.date_range(start="2004-01-31", freq="M", periods=len(arg))

    bra = pd.read_csv(sources["bra"], sep=";", index_col=0, decimal=",")
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
def regional_cpi() -> pd.DataFrame:
    """Get consumer price index for Argentina and Brazil.

    Returns
    -------
    Monthly CPI : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    arg = requests.get(
        sources["ar"],
        params=sources["ar_payload"].format(
            date1=dt.datetime.now().strftime("%Y-%m-%d"),
            date2=dt.datetime.now().strftime("%Y%m%d"),
        ),
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

    bra_r = requests.get(sources["bra"].format(date=dt.datetime.now().strftime("%Y%m")))
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
def regional_embi_spreads() -> pd.DataFrame:
    """Get EMBI spread for Argentina, Brazil and the EMBI Global.

    Returns
    -------
    Daily 10-year government bond spreads : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], usecols="A:B,E,G", skiprows=1, index_col=0)
    output = (
        raw.loc[~pd.isna(raw.index)]
        .mul(100)
        .rename(columns={"Global": "EMBI Global"})[["Argentina", "Brasil", "EMBI Global"]]
    )
    output.index = pd.to_datetime(output.index)
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
def regional_embi_yields(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
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
    name = get_name_from_function()
    sources = get_download_sources(name)

    if pipeline is None:
        pipeline = Pipeline()

    treasuries = pd.read_csv(
        sources["treasury"].format(timestamp=dt.datetime.now().timestamp().__round__()),
        usecols=[0, 4],
        index_col=0,
    )
    treasuries.index = pd.to_datetime(treasuries.index)
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
def regional_nxr() -> pd.DataFrame:
    """Get USDARS and USDBRL.

    Returns
    -------
    Daily exchange rates : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    arg = []
    for dollar in ["ar", "ar_unofficial"]:
        r = requests.get(sources[dollar].format(date=dt.datetime.now().strftime("%d-%m-%Y")))
        aux = pd.DataFrame(r.json())[[0, 2]]
        aux.set_index(0, drop=True, inplace=True)
        aux.drop("Fecha", inplace=True)
        aux = aux.replace(",", ".", regex=True).apply(pd.to_numeric)
        aux.index = pd.to_datetime(aux.index, format="%d/%m/%Y")
        aux.sort_index(inplace=True)
        aux.columns = [dollar]
        arg.append(aux)
    arg = arg[0].join(arg[1], how="left")
    arg.columns = ["Argentina - oficial", "Argentina - informal"]

    r = requests.get(sources["bra"])
    bra = pd.DataFrame(r.json())
    bra = [(x["VALDATA"], x["VALVALOR"]) for x in bra["value"]]
    bra = pd.DataFrame.from_records(bra).dropna(how="any")
    bra.set_index(0, inplace=True)
    bra.index = pd.to_datetime(bra.index.str[:-4]).tz_localize(None)
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
def regional_policy_rates() -> pd.DataFrame:
    """Get central bank policy interest rates data.

    Countries/aggregates selected are Argentina and Brazil.

    Returns
    -------
    Daily policy interest rates : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    r = requests.get(sources["main"])
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
def regional_stock_markets() -> pd.DataFrame:
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
    name = get_name_from_function()
    sources = get_download_sources(name)

    yahoo = []
    for series in ["arg", "bra"]:
        aux = pd.read_csv(
            sources[series].format(timestamp=dt.datetime.now().timestamp().__round__()),
            index_col=0,
            usecols=[0, 4],
            parse_dates=True,
        )
        aux.columns = [series]
        yahoo.append(aux)
    output = pd.concat(yahoo, axis=1).interpolate(method="linear", limit_area="inside")
    output.columns = ["MERVAL", "BOVESPA"]
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Global",
        currency="USD",
        inf_adj="No",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )
    metadata._modify_multiindex(output, levels=[3], new_arrays=[["ARS", "BRL"]])
    output = rebase(output, start_date="2019-01-02")

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def regional_rxr(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
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

    url_ = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M"
    url_extra = "?startPeriod=1970&endPeriod="
    ifs = []
    for country in ["US", "BR", "AR"]:
        for indicator in ["PCPI_IX", "ENDA_XDC_USD_RATE"]:
            base_url = f"{url_}.{country}.{indicator}.{url_extra}{dt.datetime.now().year}"
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
