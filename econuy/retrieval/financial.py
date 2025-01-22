import datetime as dt
import time
from io import BytesIO, StringIO
from urllib.error import HTTPError, URLError
from httpx import ConnectError

import pandas as pd
import httpx
from pandas.tseries.offsets import MonthEnd

from econuy.utils.chromedriver import _build
from econuy.utils.operations import get_download_sources, get_name_from_function
from econuy.utils.retrieval import get_with_ssl_context
from econuy.base import Dataset, DatasetMetadata


def bank_credit() -> pd.DataFrame:
    """Get bank credit data.

    Returns
    -------
    Monthly credit : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        xls = pd.ExcelFile(sources["main"])
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            r_bytes = get_with_ssl_context("bcu", sources["main"])
            xls = pd.ExcelFile(r_bytes)
    tc = pd.read_excel(
        xls,
        sheet_name="TC",
        skiprows=1,
        usecols="A:B",
        index_col=0,
        parse_dates=True,
        date_format="%Y%m",
    ).squeeze()
    tc.index = tc.index + MonthEnd(0)
    raw = pd.read_excel(
        xls,
        sheet_name="Total Sist. Banc.",
        skiprows=10,
        usecols="A:P,T:AB,AD:AL",
        index_col=0,
    )
    raw.index = pd.to_datetime(raw.index, errors="coerce")
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
    output.iloc[:, :24] = output.iloc[:, :24].div(tc, axis=0)

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    spanish_names = output.columns
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Financial market",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def bank_deposits() -> pd.DataFrame:
    """Get bank deposits data.

    Returns
    -------
    Monthly deposits : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        xls = pd.ExcelFile(sources["main"])
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            r_bytes = get_with_ssl_context("bcu", sources["main"])
            xls = pd.ExcelFile(r_bytes)
    tc = pd.read_excel(
        xls,
        sheet_name="TC",
        skiprows=1,
        usecols="A:B",
        index_col=0,
        parse_dates=True,
        date_format="%Y%m",
    ).squeeze()
    tc.index = tc.index + MonthEnd(0)
    raw = pd.read_excel(
        xls,
        sheet_name="Total Sist. Banc.",
        skiprows=8,
        usecols="A:S",
        index_col=0,
    )
    raw.index = pd.to_datetime(raw.index, errors="coerce")
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
    output.iloc[:, :15] = output.iloc[:, :15].div(tc, axis=0)
    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    spanish_names = output.columns
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Financial market",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def bank_interest_rates() -> pd.DataFrame:
    """Get interest rates data.

    Returns
    -------
    Monthly interest rates : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        xls = pd.ExcelFile(sources["main"])
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            r_bytes = get_with_ssl_context("bcu", sources["main"])
            xls = pd.ExcelFile(r_bytes)

    sheets = [
        "Activas $",
        "Activas UI",
        "Activas U$S",
        "Pasivas $",
        "Pasivas UI",
        "Pasivas U$S",
    ]
    columns = ["B:C,G,K", "B:C,G,K", "B:C,H,L", "B:C,N,T", "B:C,P,W", "B:C,N,T"]
    sheet_data = []
    for sheet, columns in zip(sheets, columns):
        if "Activas" in sheet:
            skip = 11
        else:
            skip = 10
        data = pd.read_excel(
            xls, sheet_name=sheet, skiprows=skip, usecols=columns, index_col=0
        )
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
    output = output.rename_axis(None)

    spanish_names = output.columns
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Financial market",
        "currency": "-",
        "inflation_adjustment": "-",
        "unit": "Rate",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, currency in zip(
        ids,
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
    ):
        metadata.update_indicator_metadata_value(indicator, "currency", currency)
    for indicator, const in zip(
        ids,
        [
            None,
            None,
            None,
            "Const.",
            "Const.",
            "Const.",
            None,
            None,
            None,
            None,
            None,
            None,
            "Const.",
            "Const.",
            "Const.",
            None,
            None,
            None,
        ],
    ):
        metadata.update_indicator_metadata_value(
            indicator, "inflation_adjustment", const
        )
    dataset = Dataset(name, output, metadata)

    return dataset


def sovereign_risk_index() -> pd.DataFrame:
    """Get Uruguayan Bond Index (sovereign risk spreads) data.

    Returns
    -------
    Uruguayan Bond Index : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    try:
        historical = pd.read_excel(
            sources["historical"],
            usecols="B:C",
            skiprows=1,
            index_col=0,
            sheet_name="Valores de Cierre Diarios",
        )
    except (ConnectError, URLError, HTTPError):
        r_historical = httpx.get(sources["historical"], verify=False)
        historical = pd.read_excel(
            BytesIO(r_historical.content),
            usecols="B:C",
            skiprows=1,
            index_col=0,
            sheet_name="Valores de Cierre Diarios",
        )
    driver = _build()
    driver.get(sources["current"])
    text = driver.page_source
    current = (
        pd.read_html(StringIO(text))[0]
        .set_index("Fecha")
        .rename_axis(None)
        .rename(columns={"Valor": "UBI"})
    )
    current.index = pd.to_datetime(current.index, format="%d/%m/%y")
    output = pd.concat([historical, current])
    output = output.loc[~output.index.duplicated(keep="last")]
    output.sort_index(inplace=True)
    output = output.apply(pd.to_numeric, errors="coerce").interpolate(
        limit_area="inside"
    )
    output = output.rename_axis(None)

    spanish_names = output.columns
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Financial market",
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


def _bypass_bevsa_disclaimer_maybe(driver, url: str):
    driver.get(url)
    if "Disclaimer.aspx" in driver.current_url:
        checkbox = driver.find_element(
            by="id", value="ContentPlaceHolder1_chkAcceptTerms"
        )
        checkbox.click()
        time.sleep(2)

        accept_button = driver.find_element(
            by="id", value="ContentPlaceHolder1_btnContinue"
        )
        accept_button.click()
        time.sleep(2)

        driver.get(url)


def call_rate() -> pd.DataFrame:
    """Get 1-day call interest rate data.

    Returns
    -------
    Daily call rate : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    driver = _build()
    _bypass_bevsa_disclaimer_maybe(driver, sources["main"])

    start = driver.find_element(
        by="name", value="ctl00$ContentPlaceHolder1$dateDesde$dateInput"
    )
    start.clear()
    start.send_keys("01/01/2002")
    end = driver.find_element(
        by="name", value="ctl00$ContentPlaceHolder1$dateHasta$dateInput"
    )
    end.clear()
    end.send_keys(dt.datetime.now().strftime("%d/%m/%Y"))
    submit = driver.find_element(by="id", value="ContentPlaceHolder1_LinkFiltrar")
    submit.click()
    time.sleep(5)
    tables = pd.read_html(StringIO(driver.page_source), decimal=",", thousands=".")
    driver.quit()

    raw = tables[8].iloc[:, :-2]
    output = raw.set_index("FECHA")
    output.index = pd.to_datetime(output.index, format="%d/%m/%Y")
    output.columns = [
        "Tasa call a 1 día: Promedio",
        "Tasa call a 1 día: Máximo",
        "Tasa call a 1 día: Mínimo",
    ]
    output = output.sort_index()
    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    spanish_names = output.columns
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Financial market",
        "currency": "UYU",
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


def sovereign_bond_yields() -> pd.DataFrame:
    """Get interest rate yield for Uruguayan US-denominated bonds,
    inflation-linked bonds and peso bonds.

    Returns
    -------
    Daily bond yields in basis points : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    driver = _build()
    _bypass_bevsa_disclaimer_maybe(driver, sources["usd"])
    dfs = []
    for url in sources.values():
        driver.get(url)
        start = driver.find_element(
            by="name", value="ctl00$ContentPlaceHolder1$dateDesde$dateInput"
        )
        start.clear()
        start.send_keys("01/01/2000")
        end = driver.find_element(
            by="name", value="ctl00$ContentPlaceHolder1$dateHasta$dateInput"
        )
        end.clear()
        end.send_keys(dt.datetime.now().strftime("%d/%m/%Y"))
        submit = driver.find_element(by="id", value="ContentPlaceHolder1_LinkFiltrar")
        submit.click()
        time.sleep(5)
        tables = pd.read_html(StringIO(driver.page_source), decimal=",", thousands=".")

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
    output = output.rename_axis(None)

    spanish_names = output.columns
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Financial market",
        "currency": "UYU",
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
    for indicator, const in zip(ids, [None, "Const.", None]):
        metadata.update_indicator_metadata_value(
            indicator, "inflation_adjustment", const
        )
    for indicator, currency in zip(ids, ["USD", "UYU", "UYU"]):
        metadata.update_indicator_metadata_value(indicator, "currency", currency)
    dataset = Dataset(name, output, metadata)

    return dataset
