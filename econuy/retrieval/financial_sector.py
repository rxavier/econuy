import datetime as dt
import re
import time
from io import BytesIO
from typing import Optional
from urllib.error import HTTPError, URLError
from requests.exceptions import SSLError

import pandas as pd
import requests
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from selenium.webdriver.remote.webdriver import WebDriver

from econuy.utils import metadata
from econuy.utils.chromedriver import _build
from econuy.utils.sources import urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def credit() -> pd.DataFrame:
    """Get bank credit data.

    Returns
    -------
    Monthly credit : pd.DataFrame

    """
    name = "credit"

    raw = pd.read_excel(
        urls[name]["dl"]["main"],
        sheet_name="Total Sist. Banc.",
        skiprows=10,
        usecols="A:P,T:AB,AD:AL",
        index_col=0,
    )
    output = raw.loc[~pd.isna(raw.index)].dropna(how="all", axis=1)
    output.index = output.index + MonthEnd(0)
    output.columns = [
        "Créditos: Resid. privado - vigentes",
        "Créditos: Resid. privado - vencidos",
        "Créditos: Resid. privado- total",
        "Créditos: Resid. público - vigentes",
        "Créditos: Resid. público - vencidos",
        "Créditos: Resid. público - total",
        "Créditos: Resid. total - vigentes",
        "Créditos: Resid. total - vencidos",
        "Créditos: Resid. total - total",
        "Créditos: No residentes - vigentes",
        "Créditos: No residentes - vencidos",
        "Créditos: No residentes - total",
        "Créditos: Total - vigentes",
        "Créditos: Total - vencidos",
        "Créditos: Total - total",
        "Créditos: Resid. MN privado - vigentes",
        "Créditos: Resid. MN privado - vencidos",
        "Créditos: Resid. MN privado- total",
        "Créditos: Resid. MN público - vigentes",
        "Créditos: Resid. MN público - vencidos",
        "Créditos: Resid. MN público- total",
        "Créditos: Resid. MN total - vigentes",
        "Créditos: Resid. MN total - vencidos",
        "Créditos: Resid. MN total- total",
        "Créditos: Resid. ME privado - vigentes",
        "Créditos: Resid. ME privado - vencidos",
        "Créditos: Resid. ME privado- total",
        "Créditos: Resid. ME público - vigentes",
        "Créditos: Resid. ME público - vencidos",
        "Créditos: Resid. ME público- total",
        "Créditos: Resid. ME total - vigentes",
        "Créditos: Resid. ME total - vencidos",
        "Créditos: Resid. ME total- total",
    ]

    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Sector financiero",
        currency="USD",
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Stock",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def deposits() -> pd.DataFrame:
    """Get bank deposits data.

    Returns
    -------
    Monthly deposits : pd.DataFrame

    """
    name = "deposits"

    raw = pd.read_excel(
        urls[name]["dl"]["main"],
        sheet_name="Total Sist. Banc.",
        skiprows=8,
        usecols="A:S",
        index_col=0,
    )
    output = raw.loc[~pd.isna(raw.index)].dropna(how="all", axis=1)
    output.index = output.index + MonthEnd(0)
    output.columns = [
        "Depósitos: S. privado - MN",
        "Depósitos: S. privado - ME",
        "Depósitos: S. privado - total",
        "Depósitos: S. público - MN",
        "Depósitos: S. público - ME",
        "Depósitos: S. público - total",
        "Depósitos: Total - MN",
        "Depósitos: Total - ME",
        "Depósitos: Total - total",
        "Depósitos: S. privado - MN vista",
        "Depósitos: S. privado - MN plazo",
        "Depósitos: S. privado - ME vista",
        "Depósitos: S. privado - ME plazo",
        "Depósitos: S. privado - total vista",
        "Depósitos: S. privado - total plazo",
        "Depósitos: S. privado - residente",
        "Depósitos: S. privado - no residente",
    ]

    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Sector financiero",
        currency="USD",
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Stock",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def interest_rates() -> pd.DataFrame:
    """Get interest rates data.

    Returns
    -------
    Monthly interest rates : pd.DataFrame

    """
    xls = pd.ExcelFile(urls["interest_rates"]["dl"]["main"])
    sheets = ["Activas $", "Activas UI", "Activas U$S", "Pasivas $", "Pasivas UI", "Pasivas U$S"]
    columns = ["B:C,G,K", "B:C,G,K", "B:C,H,L", "B:C,N,T", "B:C,P,W", "B:C,N,T"]
    sheet_data = []
    for sheet, columns in zip(sheets, columns):
        if "Activas" in sheet:
            skip = 11
        else:
            skip = 10
        data = pd.read_excel(xls, sheet_name=sheet, skiprows=skip, usecols=columns, index_col=0)
        data.index = pd.to_datetime(data.index, errors="coerce")
        data = data.loc[~pd.isna(data.index)]
        data.index = data.index + MonthEnd(0)
        sheet_data.append(data)
    output = pd.concat(sheet_data, axis=1)
    output.columns = [
        "Tasas activas: $, promedio",
        "Tasas activas: $, promedio empresas",
        "Tasas activas: $, promedio familias",
        "Tasas activas: UI, promedio",
        "Tasas activas: UI, promedio empresas",
        "Tasas activas: UI, promedio familias",
        "Tasas activas: US$, promedio",
        "Tasas activas: US$, promedio empresas",
        "Tasas activas: US$, promedio familias",
        "Tasas pasivas: $, promedio",
        "Tasas pasivas: $, promedio empresas",
        "Tasas pasivas: $, promedio familias",
        "Tasas pasivas: UI, promedio",
        "Tasas pasivas: UI, promedio empresas",
        "Tasas pasivas: UI, promedio familias",
        "Tasas pasivas: US$, promedio",
        "Tasas pasivas: US$, promedio empresas",
        "Tasas pasivas: US$, promedio familias",
    ]

    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Sector financiero",
        currency="-",
        inf_adj="-",
        unit="Tasa",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )
    metadata._modify_multiindex(
        output,
        levels=[3, 4],
        new_arrays=[
            [
                "UYU",
                "UYU",
                "UYU",
                "UYU",
                "UYU",
                "UYU",
                "USD",
                "USD",
                "USD",
                "UYU",
                "UYU",
                "UYU",
                "UYU",
                "UYU",
                "UYU",
                "USD",
                "USD",
                "USD",
            ],
            [
                "No",
                "No",
                "No",
                "Const.",
                "Const.",
                "Const.",
                "No",
                "No",
                "No",
                "No",
                "No",
                "No",
                "Const.",
                "Const.",
                "Const.",
                "No",
                "No",
                "No",
            ],
        ],
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def sovereign_risk() -> pd.DataFrame:
    """Get Uruguayan Bond Index (sovereign risk spreads) data.

    Returns
    -------
    Uruguayan Bond Index : pd.DataFrame

    """
    name = "sovereign_risk"

    try:
        historical = pd.read_excel(
            urls[name]["dl"]["historical"],
            usecols="B:C",
            skiprows=1,
            index_col=0,
            sheet_name="Valores de Cierre Diarios",
        )
        r_current = requests.get(urls[name]["dl"]["current"])
    except (SSLError, URLError, HTTPError):
        r_historical = requests.get(urls[name]["dl"]["historical"], verify=False)
        historical = pd.read_excel(
            BytesIO(r_historical.content),
            usecols="B:C",
            skiprows=1,
            index_col=0,
            sheet_name="Valores de Cierre Diarios",
        )
        r_current = requests.get(urls[name]["dl"]["current"], verify=False)
    soup = BeautifulSoup(r_current.text, features="lxml")
    raw_string = soup.find_all(type="hidden")[0]["value"]
    raw_list = raw_string.split("],")
    raw_list = [re.sub(r'["\[\]]', "", line) for line in raw_list]
    index = [x.split(",")[0] for x in raw_list]
    values = [x.split(",")[1] for x in raw_list]
    current = pd.DataFrame(data=values, index=index, columns=["UBI"])
    current.index = pd.to_datetime(current.index, format="%d/%m/%y")
    output = pd.concat([historical, current])
    output = output.loc[~output.index.duplicated(keep="last")]
    output.sort_index(inplace=True)
    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Sector financiero",
        currency="USD",
        inf_adj="No",
        unit="PBS",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def call_rate(driver: Optional[WebDriver] = None) -> pd.DataFrame:
    """Get 1-day call interest rate data.

    This function requires a Selenium webdriver. It can be provided in the
    driver parameter, or it will attempt to configure a Chrome webdriver.

    Parameters
    ----------
    driver : selenium.webdriver.chrome.webdriver.WebDriver, default None
        Selenium webdriver for scraping. If None, build a Chrome webdriver.

    Returns
    -------
    Daily call rate : pd.DataFrame

    """
    name = "call"

    if driver is None:
        driver = _build()
    driver.get(urls[name]["dl"]["main"])
    start = driver.find_element(by="name", value="ctl00$ContentPlaceHolder1$dateDesde$dateInput")
    start.clear()
    start.send_keys("01/01/2002")
    end = driver.find_element(by="name", value="ctl00$ContentPlaceHolder1$dateHasta$dateInput")
    end.clear()
    end.send_keys(dt.datetime.now().strftime("%d/%m/%Y"))
    submit = driver.find_element(by="id", value="ContentPlaceHolder1_LinkFiltrar")
    submit.click()
    time.sleep(5)
    tables = pd.read_html(driver.page_source, decimal=",", thousands=".")
    driver.quit()
    raw = tables[8].iloc[:, :-2]
    call = raw.set_index("FECHA")
    call.index = pd.to_datetime(call.index, format="%d/%m/%Y")
    call.sort_index(inplace=True)
    call.columns = [
        "Tasa call a 1 día: Promedio",
        "Tasa call a 1 día: Máximo",
        "Tasa call a 1 día: Mínimo",
    ]
    call = call.apply(pd.to_numeric, errors="coerce")
    call.rename_axis(None, inplace=True)

    metadata._set(
        call,
        area="Sector financiero",
        currency="UYU",
        inf_adj="No",
        unit="Tasa",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return call


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=8,
    retry_window_after_first_call_in_seconds=60,
)
def bonds(driver: Optional[WebDriver] = None) -> pd.DataFrame:
    """Get interest rate yield for Uruguayan US-denominated bonds,
    inflation-linked bonds and peso bonds.

    This function requires a Selenium webdriver. It can be provided in the
    driver parameter, or it will attempt to configure a Chrome webdriver.

    Parameters
    ----------
    driver : selenium.webdriver.chrome.webdriver.WebDriver, default None
        Selenium webdriver for scraping. If None, build a Chrome webdriver.

    Returns
    -------
    Daily bond yields in basis points : pd.DataFrame

    """
    name = "bonds"

    if driver is None:
        driver = _build()
    dfs = []
    for url in urls[name]["dl"].values():
        driver.get(url)
        start = driver.find_element(
            by="name", value="ctl00$ContentPlaceHolder1$dateDesde$dateInput"
        )
        start.clear()
        start.send_keys("01/01/2000")
        end = driver.find_element(by="name", value="ctl00$ContentPlaceHolder1$dateHasta$dateInput")
        end.clear()
        end.send_keys(dt.datetime.now().strftime("%d/%m/%Y"))
        submit = driver.find_element(by="id", value="ContentPlaceHolder1_LinkFiltrar")
        submit.click()
        time.sleep(10)
        tables = pd.read_html(driver.page_source, decimal=",", thousands=".")

        raw = tables[8]
        df = raw.set_index("FECHA")
        df.index = pd.to_datetime(df.index, format="%d/%m/%Y")
        df.sort_index(inplace=True)
        df = df.loc[:, df.columns.isin(["BPS", "RENDIMIENTO"])]
        df.columns = [url]
        dfs.append(df)
    driver.quit()
    output = dfs[0].join(dfs[1], how="outer").join(dfs[2], how="outer")
    output.columns = [
        "Bonos soberanos en dólares",
        "Bonos soberanos en UI",
        "Bonos soberanos en pesos",
    ]
    output = output.loc[~output.index.duplicated()]

    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Sector financiero",
        currency="-",
        inf_adj="No",
        unit="PBS",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )
    metadata._modify_multiindex(
        output, levels=[3, 4], new_arrays=[["USD", "UYU", "UYU"], ["No", "Const.", "No"]]
    )

    return output
