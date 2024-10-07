import datetime as dt
import re
import tempfile
import time
from pathlib import Path
from os import listdir, path
from typing import List, Optional
from urllib.error import HTTPError, URLError

import pandas as pd
import numpy as np
import patoolib
import httpx
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from econuy import transform
from econuy.core import Pipeline
from econuy.utils import metadata, get_project_root
from econuy.utils.operations import get_download_sources, get_name_from_function
from econuy.utils.chromedriver import _build
from econuy.base import Dataset, DatasetMetadata


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def monthly_gdp() -> pd.DataFrame:
    """Get the monthly indicator for economic activity.

    Returns
    -------
    Monthly GDP : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    with tempfile.TemporaryDirectory() as tmp_dir:
        driver = _build(tmp_dir)
        driver.get(sources["main"])
        button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "button.dt-button.buttons-excel.buttons-html5")
            )
        )
        driver.execute_script("arguments[0].click()", button)
        time.sleep(3)
        driver.quit()
        filepath = list(Path(tmp_dir).glob("*.xlsx"))[0]
        output = pd.read_excel(filepath, index_col=0)

    output.index = pd.date_range(start="2016-01-31", freq="ME", periods=len(output))
    output = output.rename_axis(None)

    spanish_names = [
        "Indicador mensual de actividad económica",
        "Indicador mensual de actividad económica (desestacionalizado)",
        "Indicador mensual de actividad económica (tendencia-ciclo)",
    ]
    spanish_names = [{"es": x} for x in spanish_names]

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": "Constant prices 2016",
        "unit": "2016=100",
        "seasonal_adjustment": "",
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }

    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, adjustment in zip(
        ids, ["Not seasonally adjusted", "Seasonally adjusted", "Trend-cycle"]
    ):
        metadata = metadata.update_indicator_metadata_value(
            indicator, "seasonal_adjustment", adjustment
        )
    dataset = Dataset(name, output, metadata)

    return dataset


def _national_accounts_retriever(
    url: str,
    nrows: int,
    skiprows: int,
    inf_adj: str,
    unit: str,
    seas_adj: str,
    colnames: List[str],
) -> pd.DataFrame:
    """Helper function. See any of the `natacc_...()` functions."""
    try:
        raw = pd.read_excel(url, skiprows=skiprows, nrows=nrows, index_col=1)
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = httpx.get(url, verify=certs_path)
            raw = pd.read_excel(r.content, skiprows=skiprows, nrows=nrows, index_col=1)
    raw = raw.iloc[:, 1:].dropna(how="all", axis=1).dropna(how="all", axis=0).T
    raw.index = raw.index.str.replace("*", "", regex=False)
    raw.index = raw.index.str.replace(r"\bI \b", "3-", regex=True)
    raw.index = raw.index.str.replace(r"\bII \b", "6-", regex=True)
    raw.index = raw.index.str.replace(r"\bIII \b", "9-", regex=True)
    raw.index = raw.index.str.replace(r"\bIV \b", "12-", regex=True)
    raw.index = raw.index.str.replace(r"([0-9]+)(I)", r"\g<1>1", regex=True)
    raw.index = raw.index.str.strip()
    raw.index = pd.to_datetime(raw.index, format="%m-%Y") + MonthEnd(1)
    raw.columns = colnames
    output = raw.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)

    for col in output.columns:
        if "Importaciones" in col and all(output[col] < 0):
            output[col] = output[col] * -1

    metadata._set(
        output,
        area="Actividad económica",
        currency="UYU",
        inf_adj=inf_adj,
        unit=unit,
        seas_adj=seas_adj,
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def national_accounts_supply_constant_nsa() -> pd.DataFrame:
    """Get supply-side national accounts data in NSA constant prices, 2005-.

    Returns
    -------
    National accounts, supply side, constant prices, NSA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    colnames = [
        "Agropecuario, pesca y minería",
        "Industrias manufactureras",
        "Energía eléctrica, gas y agua",
        "Construcción",
        "Comercio, alojamiento y suministro de comidas y bebidas",
        "Transporte y almacenamiento, información y comunicaciones",
        "Servicios financieros",
        "Actividades profesionales y arrendamiento",
        "Actividades de administración pública",
        "Salud, educación, act. inmobiliarias y otros servicios",
        "Valor agregado a precios básicos",
        "Impuestos menos subvenciones",
        "Producto bruto interno",
    ]
    return _national_accounts_retriever(
        url=sources["main"],
        nrows=13,
        skiprows=7,
        inf_adj="Const. 2016",
        unit="Millones",
        seas_adj="NSA",
        colnames=colnames,
    )


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def national_accounts_demand_constant_nsa() -> pd.DataFrame:
    """Get demand-side national accounts data in NSA constant prices, 2005-.

    Returns
    -------
    National accounts, demand side, constant prices, NSA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    colnames = [
        "Gasto de consumo: total",
        "Gasto de consumo: hogares",
        "Gasto de consumo: gobierno y ISFLH",
        "Formación bruta de capital",
        "Formación bruta de capital: fijo",
        "Variación de existencias",
        "Exportaciones de bienes y servicios",
        "Importaciones de bienes y servicios",
        "Producto bruto interno",
    ]
    return _national_accounts_retriever(
        url=sources["main"],
        nrows=9,
        skiprows=7,
        inf_adj="Const. 2016",
        unit="Millones",
        seas_adj="NSA",
        colnames=colnames,
    )


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def national_accounts_demand_current_nsa() -> pd.DataFrame:
    """Get demand-side national accounts data in NSA current prices.

    Returns
    -------
    National accounts, demand side, current prices, NSA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    colnames = [
        "Gasto de consumo: total",
        "Gasto de consumo: hogares",
        "Gasto de consumo: gobierno y ISFLH",
        "Formación bruta de capital",
        "Formación bruta de capital: fijo",
        "Variación de existencias",
        "Exportaciones de bienes y servicios",
        "Importaciones de bienes y servicios",
        "Producto bruto interno",
    ]
    return _national_accounts_retriever(
        url=sources["main"],
        nrows=9,
        skiprows=7,
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        colnames=colnames,
    )


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def national_accounts_supply_current_nsa() -> pd.DataFrame:
    """Get supply-side national accounts data in NSA current prices, 2005-.

    Returns
    -------
    National accounts, supply side, current prices, NSA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    colnames = [
        "Agropecuario, pesca y minería",
        "Industrias manufactureras",
        "Energía eléctrica, gas y agua",
        "Construcción",
        "Comercio, alojamiento y suministro de comidas y bebidas",
        "Transporte y almacenamiento, información y comunicaciones",
        "Servicios financieros",
        "Actividades profesionales y arrendamiento",
        "Actividades de administración pública",
        "Salud, educación, act. inmobiliarias y otros servicios",
        "Valor agregado a precios básicos",
        "Impuestos menos subvenciones",
        "Producto bruto interno",
    ]
    return _national_accounts_retriever(
        url=sources["main"],
        nrows=13,
        skiprows=7,
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        colnames=colnames,
    )


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def gdp_index_constant_sa() -> pd.DataFrame:
    """Get supply-side national accounts data in SA real index, 1997-.

    Returns
    -------
    National accounts, supply side, real index, SA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    colnames = ["Producto bruto interno"]
    return _national_accounts_retriever(
        url=sources["main"],
        nrows=1,
        skiprows=7,
        inf_adj="Const. 2016",
        unit="2016=100",
        seas_adj="SA",
        colnames=colnames,
    )


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def national_accounts_supply_constant_nsa_extended(
    pipeline: Pipeline = None,
) -> pd.DataFrame:
    """Get supply-side national accounts data in NSA constant prices, 1988-.

    Three datasets with different base years, 1983, 2005 and 2016, are spliced
    in order to get to the result DataFrame.

    Returns
    -------
    National accounts, supply side, constant prices, NSA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("national_accounts_supply_constant_nsa")
    data_16 = pipeline.dataset.copy()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16["Otros servicios"] = (
        data_16["Servicios financieros"]
        + data_16["Actividades profesionales y arrendamiento"]
        + data_16["Actividades de administración pública"]
        + data_16["Salud, educación, act. inmobiliarias y otros servicios"]
    )
    data_16.drop(
        [
            "Valor agregado a precios básicos",
            "Servicios financieros",
            "Actividades profesionales y arrendamiento",
            "Actividades de administración pública",
            "Salud, educación, act. inmobiliarias y otros servicios",
            "Valor agregado a precios básicos",
        ],
        axis=1,
        inplace=True,
    )

    colnames = [
        "Actividades primarias",
        "Act. prim.: Agricultura, ganadería, caza y silvicultura",
        "Industrias manufactureras",
        "Suministro de electricidad, gas y agua",
        "Construcción",
        "Comercio, reparaciones, restaurantes y hoteles",
        "Transporte, almacenamiento y comunicaciones",
        "Otras actividades",
        "Otras actividades: SIFMI",
        "Impuestos menos subvenciones",
        "Producto bruto interno",
    ]
    data_05 = _national_accounts_retriever(
        url=sources["2005"],
        nrows=12,
        skiprows=9,
        inf_adj="Const. 2005",
        unit="Millones",
        seas_adj="NSA",
        colnames=colnames,
    )
    data_05.columns = data_05.columns.get_level_values(0)

    # In the following lines I rename and reorder the Other activities
    # column, and explicitly drop SIFMI because I have no way of adding them.
    # In order for this to be correct, I would have to substract the SIFMI
    # part from each of the other sectors, which I cannot do. Implicitly I'm
    # assuming that the share of SIFMI in each sector over time doesn't change,
    # which shouldn't affect growth rates.
    data_05["Otros servicios"] = data_05["Otras actividades"]
    data_05.drop(
        [
            "Act. prim.: Agricultura, ganadería, caza y silvicultura",
            "Otras actividades",
            "Otras actividades: SIFMI",
        ],
        axis=1,
        inplace=True,
    )
    data_05.columns = data_16.columns
    aux_index = list(dict.fromkeys(list(data_05.index) + list(data_16.index)))
    aux = data_16.reindex(aux_index)
    for quarter in reversed(aux_index):
        if aux.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            aux.loc[quarter, :] = (
                aux.loc[next_quarter, :]
                * data_05.loc[quarter, :]
                / data_05.loc[next_quarter, :]
            )
    aux = aux[
        [
            "Agropecuario, pesca y minería",
            "Industrias manufactureras",
            "Energía eléctrica, gas y agua",
            "Construcción",
            "Comercio, alojamiento y suministro de comidas y bebidas",
            "Transporte y almacenamiento, información y comunicaciones",
            "Otros servicios",
            "Impuestos menos subvenciones",
            "Producto bruto interno",
        ]
    ]
    try:
        data_83 = (
            pd.read_excel(
                sources["1983"],
                skiprows=10,
                nrows=8,
                index_col=1,
            )
            .iloc[:, 1:]
            .T
        )
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = httpx.get(sources["1983"], verify=certs_path)
            data_83 = (
                pd.read_excel(
                    r.content,
                    skiprows=10,
                    nrows=8,
                    index_col=1,
                )
                .iloc[:, 1:]
                .T
            )
    data_83.index = pd.date_range(
        start="1988-03-31", freq="QE-DEC", periods=len(data_83)
    )
    data_83["Impuestos menos subvenciones"] = np.nan
    data_83 = data_83[
        [
            "AGROPECUARIA",
            "INDUSTRIAS MANUFACTURERAS",
            "SUMINISTRO DE ELECTRICIDAD, GAS Y AGUA",
            "CONSTRUCCION",
            "COMERCIO, REPARACIONES, RESTAURANTES Y HOTELES",
            "TRANSPORTE, ALMACENAMIENTO Y COMUNICACIONES",
            "OTRAS (1)",
            "Impuestos menos subvenciones",
            "PRODUCTO INTERNO BRUTO",
        ]
    ]
    data_83.columns = aux.columns

    complete_index = list(dict.fromkeys(list(data_83.index) + list(aux.index)))
    output = aux.reindex(complete_index)
    for quarter in reversed(complete_index):
        if output.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            output.loc[quarter, :] = (
                output.loc[next_quarter, :]
                * data_83.loc[quarter, :]
                / data_83.loc[next_quarter, :]
            )

    metadata._set(
        output,
        area="Actividad económica",
        currency="UYU",
        inf_adj="Const. 2016",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def national_accounts_demand_constant_nsa_extended(
    pipeline: Pipeline = None,
) -> pd.DataFrame:
    """Get demand-side national accounts data in NSA constant prices, 1988-.

    Three datasets with different base years, 1983, 2005 and 2016, are spliced
    in order to get to the result DataFrame.

    Returns
    -------
    National accounts, demand side, constant prices, NSA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("national_accounts_demand_constant_nsa")
    data_16 = pipeline.dataset.copy()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16.drop(["Variación de existencias"], axis=1, inplace=True)

    colnames = [
        "Gasto de consumo final",
        "Gasto de consumo final de hogares",
        "Gasto de consumo final del gobierno general",
        "Formación bruta de capital",
        "Formación bruta de capital fijo",
        "Sector público",
        "Sector privado",
        "Exportaciones",
        "Importaciones",
        "Producto bruto interno",
    ]
    data_05 = _national_accounts_retriever(
        url=sources["2005"],
        nrows=10,
        skiprows=9,
        inf_adj="Const. 2005",
        unit="Millones",
        seas_adj="NSA",
        colnames=colnames,
    )
    data_05.columns = data_05.columns.get_level_values(0)
    data_05.drop(["Sector público", "Sector privado"], axis=1, inplace=True)
    data_05.columns = data_16.columns
    aux_index = list(dict.fromkeys(list(data_05.index) + list(data_16.index)))
    aux = data_16.reindex(aux_index)
    for quarter in reversed(aux_index):
        if aux.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            aux.loc[quarter, :] = (
                aux.loc[next_quarter, :]
                * data_05.loc[quarter, :]
                / data_05.loc[next_quarter, :]
            )
    try:
        data_83 = (
            pd.read_excel(
                sources["1983"],
                skiprows=10,
                nrows=11,
                index_col=1,
            )
            .iloc[:, 1:]
            .T
        )
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = httpx.get(sources["1983"], verify=certs_path)
            data_83 = (
                pd.read_excel(
                    r.content,
                    skiprows=10,
                    nrows=11,
                    index_col=1,
                )
                .iloc[:, 1:]
                .T
            )
    data_83.index = pd.date_range(
        start="1988-03-31", freq="QE-DEC", periods=len(data_83)
    )
    data_83.drop(
        ["Sector público", "Sector privado", "Variación de existencias"],
        axis=1,
        inplace=True,
    )
    data_83.columns = aux.columns

    complete_index = list(dict.fromkeys(list(data_83.index) + list(aux.index)))
    output = aux.reindex(complete_index)
    for quarter in reversed(complete_index):
        if output.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            output.loc[quarter, :] = (
                output.loc[next_quarter, :]
                * data_83.loc[quarter, :]
                / data_83.loc[next_quarter, :]
            )

    metadata._set(
        output,
        area="Actividad económica",
        currency="UYU",
        inf_adj="Const. 2016",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def gdp_index_constant_sa_extended(pipeline: Pipeline = None) -> pd.DataFrame:
    """Get GDP data in SA constant prices, 1988-.

    Three datasets with different base years, 1983, 2005 and 2016, are spliced
    in order to get to the result DataFrame.

    Returns
    -------
    GDP, constant prices, SA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("gdp_index_constant_sa")
    data_16 = pipeline.dataset.copy()
    data_16.columns = data_16.columns.get_level_values(0)

    colnames = [
        "Actividades primarias",
        "Act. prim.: Agricultura, ganadería, caza y silvicultura",
        "Industrias manufactureras",
        "Suministro de electricidad, gas y agua",
        "Construcción",
        "Comercio, reparaciones, restaurantes y hoteles",
        "Transporte, almacenamiento y comunicaciones",
        "Otras actividades",
        "Otras actividades: SIFMI",
        "Impuestos menos subvenciones",
        "Producto bruto interno",
    ]
    data_05 = _national_accounts_retriever(
        url=sources["2005"],
        nrows=12,
        skiprows=9,
        inf_adj="Const. 2005",
        unit="Millones",
        seas_adj="SA",
        colnames=colnames,
    )
    data_05.columns = data_05.columns.get_level_values(0)
    data_05 = data_05[["Producto bruto interno"]]
    aux_index = list(dict.fromkeys(list(data_05.index) + list(data_16.index)))
    aux = data_16.reindex(aux_index)
    for quarter in reversed(aux_index):
        if aux.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            aux.loc[quarter, :] = (
                aux.loc[next_quarter, :]
                * data_05.loc[quarter, :]
                / data_05.loc[next_quarter, :]
            )
    try:
        data_83 = (
            pd.read_excel(
                sources["1983"],
                skiprows=10,
                nrows=8,
                index_col=1,
            )
            .iloc[:, 1:]
            .T
        )
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = httpx.get(sources["1983"], verify=certs_path)
            data_83 = (
                pd.read_excel(
                    r.content,
                    skiprows=10,
                    nrows=8,
                    index_col=1,
                )
                .iloc[:, 1:]
                .T
            )
    data_83.index = pd.date_range(
        start="1988-03-31", freq="QE-DEC", periods=len(data_83)
    )
    data_83 = data_83[["PRODUCTO INTERNO BRUTO"]]
    data_83.columns = aux.columns

    complete_index = list(dict.fromkeys(list(data_83.index) + list(aux.index)))
    output = aux.reindex(complete_index)
    for quarter in reversed(complete_index):
        if output.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            output.loc[quarter, :] = (
                output.loc[next_quarter, :]
                * data_83.loc[quarter, :]
                / data_83.loc[next_quarter, :]
            )

    metadata._set(
        output,
        area="Actividad económica",
        currency="UYU",
        inf_adj="Const. 2016",
        unit="Millones",
        seas_adj="SA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def gdp_constant_nsa_extended(pipeline: Pipeline = None) -> pd.DataFrame:
    """Get GDP data in NSA constant prices, 1988-.

    Three datasets with two different base years, 1983 and 2016, are
    spliced in order to get to the result DataFrame. It uses the BCU's working
    paper for retropolated GDP in current and constant prices for 1997-2015.

    Returns
    -------
    GDP, constant prices, NSA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("national_accounts_supply_constant_nsa")
    data_16 = pipeline.dataset.copy()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16 = data_16[["Producto bruto interno"]]

    colnames = ["Producto bruto interno"]
    data_97 = _national_accounts_retriever(
        url=sources["1997"],
        nrows=1,
        skiprows=6,
        inf_adj="Const. 2016",
        unit="Millones",
        seas_adj="NSA",
        colnames=colnames,
    )
    data_97.columns = data_97.columns.get_level_values(0)

    aux = pd.concat([data_97, data_16], axis=0)

    try:
        data_83 = (
            pd.read_excel(sources["1983"], skiprows=10, nrows=8, index_col=1)
            .iloc[:, 1:]
            .T
        )
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = httpx.get(sources["1983"], verify=certs_path)
            data_83 = (
                pd.read_excel(
                    r.content,
                    skiprows=10,
                    nrows=8,
                    index_col=1,
                )
                .iloc[:, 1:]
                .T
            )
    data_83.index = pd.date_range(
        start="1988-03-31", freq="QE-DEC", periods=len(data_83)
    )
    data_83 = data_83[["PRODUCTO INTERNO BRUTO"]]
    data_83.columns = ["Producto bruto interno"]

    reaux_index = list(dict.fromkeys(list(data_83.index) + list(aux.index)))
    output = aux.reindex(reaux_index)
    for quarter in reversed(reaux_index):
        if output.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            output.loc[quarter, :] = (
                output.loc[next_quarter, :]
                * data_83.loc[quarter, :]
                / data_83.loc[next_quarter, :]
            )

    metadata._set(
        output,
        area="Actividad económica",
        currency="UYU",
        inf_adj="Const. 2016",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def gdp_current_nsa_extended(pipeline: Pipeline = None) -> pd.DataFrame:
    """Get GDP data in NSA current prices, 1997-.

    It uses the BCU's working paper for retropolated GDP in current and constant prices for
    1997-2015.

    Returns
    -------
    GDP, current prices, NSA : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("national_accounts_supply_current_nsa")
    data_16 = pipeline.dataset.copy()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16 = data_16[["Producto bruto interno"]]

    colnames = ["Producto bruto interno"]
    data_97 = _national_accounts_retriever(
        url=sources["1997"],
        nrows=1,
        skiprows=6,
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        colnames=colnames,
    )
    data_97.columns = data_97.columns.get_level_values(0)

    output = pd.concat([data_97, data_16], axis=0)

    metadata._set(
        output,
        area="Actividad económica",
        currency="UYU",
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def _monthly_interpolated_gdp(pipeline: Optional[Pipeline] = None):
    """Get nominal GDP data in UYU and USD with forecasts.

    Update nominal GDP data for use in the `transform.convert_gdp()` function.
    Get IMF forecasts for year of last available data point and the next
    year (for example, if the last period available at the BCU website is
    september 2019, fetch forecasts for 2019 and 2020).

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    output : Pandas dataframe
        Quarterly GDP in UYU and USD with 1 year forecasts.

    """
    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get(name="gdp_current_nsa_extended")
    data_uyu = pipeline.dataset
    # TODO: use Pipeline methods for these
    data_uyu = transform.rolling(data_uyu, window=4, operation="sum")
    data_usd = transform.convert_usd(data_uyu, pipeline=pipeline)

    data = [data_uyu, data_usd]
    last_year = data_uyu.index.max().year
    if data_uyu.index.max().month == 12:
        last_year += 1

    results = []
    for table, gdp in zip(["NGDP", "NGDPD"], data):
        table_url = (
            f"https://www.imf.org/en/Publications/WEO/weo-database/"
            f"2021/April/weo-report?c=298,&s={table},&sy="
            f"{last_year - 1}&ey={last_year + 1}&ssm=0&scsm=1&scc=0&"
            f"ssd=1&ssc=0&sic=0&sort=country&ds=.&br=1"
        )
        imf_data = pd.to_numeric(pd.read_html(table_url)[0].iloc[0, [5, 6, 7]])
        imf_data = imf_data.reset_index(drop=True)
        fcast = (
            gdp.loc[[dt.datetime(last_year - 1, 12, 31)]]
            .multiply(imf_data.iloc[1])
            .divide(imf_data.iloc[0])
        )
        fcast = fcast.rename(
            index={dt.datetime(last_year - 1, 12, 31): dt.datetime(last_year, 12, 31)}
        )
        next_fcast = (
            gdp.loc[[dt.datetime(last_year - 1, 12, 31)]]
            .multiply(imf_data.iloc[2])
            .divide(imf_data.iloc[0])
        )
        next_fcast = next_fcast.rename(
            index={
                dt.datetime(last_year - 1, 12, 31): dt.datetime(last_year + 1, 12, 31)
            }
        )
        gdp = pd.concat([gdp, fcast, next_fcast], axis=0)
        results.append(gdp)

    output = pd.concat(results, axis=1)
    output = output.resample("QE-DEC").interpolate("linear").dropna(how="all")
    output.rename_axis(None, inplace=True)

    metadata._modify_multiindex(output, levels=[0], new_arrays=[["PBI UYU", "PBI USD"]])

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def industrial_production() -> pd.DataFrame:
    """Get industrial production data.

    Returns
    -------
    Monthly industrial production index : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=4, usecols="D:DR", na_values="(s)")
    weights = pd.read_csv(sources["weights"]).dropna(how="all")
    weights[["División", "Grupo", "Agrupación / Clase"]] = weights[
        ["División", "Grupo", "Agrupación / Clase"]
    ].astype(str)
    output = raw.dropna(how="all")
    output.index = pd.date_range(start="2018-01-31", freq="ME", periods=len(output))

    column_names = []
    for c in output.columns[2:]:
        c = str(c)
        match = weights.loc[weights["División"] == c, "Denominación"]
        prefix = "División: "
        if isinstance(match, pd.Series) and match.empty:
            prefix = "Grupo: "
            match = weights.loc[weights["Grupo"] == c, "Denominación"]
            if isinstance(match, pd.Series) and match.empty:
                prefix = "Clase: "
                match = weights.loc[weights["Agrupación / Clase"] == c, "Denominación"]
        try:
            match = match.iloc[0]
        except (AttributeError, IndexError):
            pass
        try:
            match = prefix + match.capitalize().strip()
        except AttributeError:
            match = c
        match = re.sub(r"[\(\)]", "-", match)
        column_names.append(match)

    output = output.apply(pd.to_numeric, errors="coerce").rename_axis(None)

    spanish_names = [
        "Industrias manufactureras",
        "Industrias manufactureras sin refinería",
    ] + column_names
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "2018=100",
        "seasonal_adjustment": "Not seasonally adjusted",
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


def core_industrial_production(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """
    Get total industrial production, industrial production excluding oil
    refinery and core industrial production.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Measures of industrial production : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get("industrial_production")
    data_18_weights = {"other foods": 0.0991, "pulp": 0.0907}
    data_18 = pipeline.dataset
    output = data_18[
        [
            "Industrias manufactureras",
            "Industrias manufactureras sin refinería",
            "Cls_Elaboración de comidas y platos preparados; elaboració...",
            "Cls_Fabricación de pasta de celulosa, papel y cartón",
        ]
    ].copy()
    output.columns = ["total", "ex-refinery", "other foods", "pulp"]
    output["core"] = output["ex-refinery"] - (
        output["other foods"] * data_18_weights["other foods"]
        + output["pulp"] * data_18_weights["pulp"]
    )
    output = output[["total", "ex-refinery", "core"]]
    output.columns = [
        "Industrias manufactureras",
        "Industrias manufactureras sin refinería",
        "Núcleo industrial",
    ]
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Actividad económica",
        currency="-",
        inf_adj="No",
        unit="2018=100",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )
    output = transform.rebase(output, start_date="2018-01-01", end_date="2018-12-31")

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def cattle_slaughter() -> pd.DataFrame:
    """Get weekly cattle slaughter data.

    Returns
    -------
    Weekly cattle slaughter : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    output = pd.read_excel(sources["main"], skiprows=8, usecols="C:H")
    output.index = pd.date_range(start="2005-01-02", freq="W", periods=len(output))
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Actividad económica",
        currency="-",
        inf_adj="No",
        unit="Cabezas",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def milk_shipments() -> pd.DataFrame:
    """Get monthly milk shipments from farms data.

    Returns
    -------
    Monhtly milk shipments from farms : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    r = httpx.get(sources["main"])
    url = re.findall(r'href="(.+\.xls)', r.text)[0]
    raw = (
        pd.read_excel(url, skiprows=11, skipfooter=4)
        .iloc[2:, 3:]
        .melt()
        .iloc[:, [1]]
        .dropna()
        .rename_axis(None)
    )
    output = (
        raw.set_index(pd.date_range(start="2002-01-31", freq="ME", periods=len(raw)))
        * 1000
    )
    output = output.apply(pd.to_numeric)
    output.columns = ["Remisión de leche a planta"]

    metadata._set(
        output,
        area="Actividad económica",
        currency="-",
        inf_adj="No",
        unit="Miles de litros",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def diesel_sales() -> pd.DataFrame:
    """
    Get diesel sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly diesel dales : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = httpx.get(sources["main"])
        rar_url = re.findall(
            r"https://www.gub.uy/ministerio-industria-energia-mineria/sites/ministerio-industria-energia-mineria/files/[0-9\-]+/Venta%20de%20gas%20oil%20por%20departamento.rar",
            r.text,
        )[0]
        f.write(httpx.get(rar_url).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(
            path_temp, sheet_name="vta gas oil por depto", skiprows=2, usecols="C:W"
        )
        raw.index = pd.date_range(start="2004-01-31", freq="ME", periods=len(raw))
        raw.columns = list(raw.columns.str.replace("\n", " "))[:-1] + ["Total"]
        output = raw
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Actividad económica",
        currency="-",
        inf_adj="No",
        unit="m3",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def gasoline_sales() -> pd.DataFrame:
    """
    Get gasoline sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly gasoline dales : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = httpx.get(sources["main"])
        rar_url = re.findall(
            r"https://www.gub.uy/ministerio-industria-energia-mineria/sites/ministerio-industria-energia-mineria/files/[0-9\-]+/Venta%20de%20gasolinas%20por%20departamento.rar",
            r.text,
        )[0]
        f.write(httpx.get(rar_url).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(
            path_temp, sheet_name="vta gasolinas por depto", skiprows=2, usecols="C:W"
        )
        raw.index = pd.date_range(start="2004-01-31", freq="ME", periods=len(raw))
        raw.columns = list(raw.columns.str.replace("\n", " "))[:-1] + ["Total"]
        output = raw
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Actividad económica",
        currency="-",
        inf_adj="No",
        unit="m3",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def electricity_sales() -> pd.DataFrame:
    """
    Get electricity sales by sector data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly electricity dales : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = httpx.get(sources["main"])
        rar_url = re.findall(
            r"https://www.gub.uy/ministerio-industria-energia-mineria/sites/ministerio-industria-energia-mineria/files/[0-9\-]+/Facturaci%C3%B3n%20de%20energ%C3%ADa%20el%C3%A9ctrica%20por%20sector.rar",
            r.text,
        )[0]
        f.write(httpx.get(rar_url).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(path_temp, sheet_name="fact ee", skiprows=2, usecols="C:J")
        raw.index = pd.date_range(start="2000-01-31", freq="ME", periods=len(raw))
        raw.columns = raw.columns.str.capitalize()
        output = raw
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Actividad económica",
        currency="-",
        inf_adj="No",
        unit="MWh",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output
