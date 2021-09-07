import datetime as dt
import re
import tempfile
from pathlib import Path
from io import BytesIO
from os import listdir, path
from typing import List, Optional
from urllib.error import HTTPError, URLError

import pandas as pd
import numpy as np
import patoolib
import requests
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd

from econuy import transform
from econuy.core import Pipeline
from econuy.utils import metadata, get_project_root
from econuy.utils.sources import urls


def _natacc_retriever(
    url: str,
    nrows: int,
    skiprows: int,
    inf_adj: str,
    unit: str,
    seas_adj: str,
    colnames: List[str],
) -> pd.DataFrame:
    """Helper function. See any of the `natacc_...()` functions."""

    raw = (
        pd.read_excel(url, skiprows=skiprows, nrows=nrows, usecols="B:AAA", index_col=0)
        .dropna(how="all", axis=1)
        .dropna(how="all", axis=0)
        .T
    )
    raw.index = raw.index.str.replace("*", "", regex=True)
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
def natacc_ind_con_nsa() -> pd.DataFrame:
    """Get supply-side national accounts data in NSA constant prices, 2005-.

    Returns
    -------
    National accounts, supply side, constant prices, NSA : pd.DataFrame

    """
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
    return _natacc_retriever(
        url=urls["natacc_ind_con_nsa"]["dl"]["main"],
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
def natacc_gas_con_nsa() -> pd.DataFrame:
    """Get demand-side national accounts data in NSA constant prices, 2005-.

    Returns
    -------
    National accounts, demand side, constant prices, NSA : pd.DataFrame

    """
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
    return _natacc_retriever(
        url=urls["natacc_gas_con_nsa"]["dl"]["main"],
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
def natacc_gas_cur_nsa() -> pd.DataFrame:
    """Get demand-side national accounts data in NSA current prices.

    Returns
    -------
    National accounts, demand side, current prices, NSA : pd.DataFrame

    """
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
    return _natacc_retriever(
        url=urls["natacc_gas_cur_nsa"]["dl"]["main"],
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
def natacc_ind_cur_nsa() -> pd.DataFrame:
    """Get supply-side national accounts data in NSA current prices, 2005-.

    Returns
    -------
    National accounts, supply side, current prices, NSA : pd.DataFrame

    """
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
    return _natacc_retriever(
        url=urls["natacc_ind_cur_nsa"]["dl"]["main"],
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
def gdp_con_idx_sa() -> pd.DataFrame:
    """Get supply-side national accounts data in SA real index, 1997-.

    Returns
    -------
    National accounts, supply side, real index, SA : pd.DataFrame

    """
    colnames = ["Producto bruto interno"]
    return _natacc_retriever(
        url=urls["gdp_con_idx_sa"]["dl"]["main"],
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
def natacc_ind_con_nsa_long(pipeline: Pipeline = None) -> pd.DataFrame:
    """Get supply-side national accounts data in NSA constant prices, 1988-.

    Three datasets with different base years, 1983, 2005 and 2016, are spliced
    in order to get to the result DataFrame.

    Returns
    -------
    National accounts, supply side, constant prices, NSA : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("natacc_ind_con_nsa")
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
    data_05 = _natacc_retriever(
        url=urls["natacc_ind_con_nsa_long"]["dl"]["2005"],
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
                aux.loc[next_quarter, :] * data_05.loc[quarter, :] / data_05.loc[next_quarter, :]
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

    data_83 = pd.read_excel(
        urls["natacc_ind_con_nsa_long"]["dl"]["1983"],
        skiprows=10,
        nrows=8,
        usecols="B:AAA",
        index_col=0,
    ).T
    data_83.index = pd.date_range(start="1988-03-31", freq="Q-DEC", periods=len(data_83))
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
def natacc_gas_con_nsa_long(pipeline: Pipeline = None) -> pd.DataFrame:
    """Get demand-side national accounts data in NSA constant prices, 1988-.

    Three datasets with different base years, 1983, 2005 and 2016, are spliced
    in order to get to the result DataFrame.

    Returns
    -------
    National accounts, demand side, constant prices, NSA : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("natacc_gas_con_nsa")
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
    data_05 = _natacc_retriever(
        url=urls["natacc_gas_con_nsa_long"]["dl"]["2005"],
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
                aux.loc[next_quarter, :] * data_05.loc[quarter, :] / data_05.loc[next_quarter, :]
            )

    data_83 = pd.read_excel(
        urls["natacc_gas_con_nsa_long"]["dl"]["1983"],
        skiprows=10,
        nrows=11,
        usecols="B:AAA",
        index_col=0,
    ).T
    data_83.index = pd.date_range(start="1988-03-31", freq="Q-DEC", periods=len(data_83))
    data_83.drop(
        ["Sector público", "Sector privado", "Variación de existencias"], axis=1, inplace=True
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
def gdp_con_idx_sa_long(pipeline: Pipeline = None) -> pd.DataFrame:
    """Get demand-side national accounts data in NSA constant prices, 1988-.

    Three datasets with different base years, 1983, 2005 and 2016, are spliced
    in order to get to the result DataFrame.

    Returns
    -------
    National accounts, demand side, constant prices, NSA : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("gdp_con_idx_sa")
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
    data_05 = _natacc_retriever(
        url=urls["gdp_con_idx_sa_long"]["dl"]["2005"],
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
                aux.loc[next_quarter, :] * data_05.loc[quarter, :] / data_05.loc[next_quarter, :]
            )

    data_83 = pd.read_excel(
        urls["gdp_con_idx_sa_long"]["dl"]["1983"],
        skiprows=10,
        nrows=8,
        usecols="B:AAA",
        index_col=0,
    ).T
    data_83.index = pd.date_range(start="1988-03-31", freq="Q-DEC", periods=len(data_83))
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
def gdp_con_nsa_long(pipeline: Pipeline = None) -> pd.DataFrame:
    """Get GDP data in NSA constant prices, 1988-.

    Three datasets with two different base years, 1983 and 2016, are
    spliced in order to get to the result DataFrame. It uses the BCU's working
    paper for retropolated GDP in current and constant prices for 1997-2015.

    Returns
    -------
    GDP, constant prices, NSA : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("natacc_ind_con_nsa")
    data_16 = pipeline.dataset.copy()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16 = data_16[["Producto bruto interno"]]

    colnames = ["Producto bruto interno"]
    data_97 = _natacc_retriever(
        url=urls["gdp_con_nsa_long"]["dl"]["1997"],
        nrows=1,
        skiprows=6,
        inf_adj="Const. 2016",
        unit="Millones",
        seas_adj="NSA",
        colnames=colnames,
    )
    data_97.columns = data_97.columns.get_level_values(0)

    aux = pd.concat([data_97, data_16], axis=0)

    data_83 = pd.read_excel(
        urls["gdp_con_nsa_long"]["dl"]["1983"], skiprows=10, nrows=8, usecols="B:AAA", index_col=0
    ).T
    data_83.index = pd.date_range(start="1988-03-31", freq="Q-DEC", periods=len(data_83))
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
def gdp_cur_nsa_long(pipeline: Pipeline = None) -> pd.DataFrame:
    """Get GDP data in NSA current prices, 1997-.

    It uses the BCU's working paper for retropolated GDP in current and constant prices for
    1997-2015.

    Returns
    -------
    GDP, current prices, NSA : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("natacc_ind_cur_nsa")
    data_16 = pipeline.dataset.copy()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16 = data_16[["Producto bruto interno"]]

    colnames = ["Producto bruto interno"]
    data_97 = _natacc_retriever(
        url=urls["gdp_cur_nsa_long"]["dl"]["1997"],
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
def _lin_gdp(pipeline: Optional[Pipeline] = None):
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

    pipeline.get(name="gdp_cur_nsa_long")
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
            index={dt.datetime(last_year - 1, 12, 31): dt.datetime(last_year + 1, 12, 31)}
        )
        fcast = fcast.append(next_fcast)
        gdp = gdp.append(fcast)
        results.append(gdp)

    output = pd.concat(results, axis=1)
    output = output.resample("Q-DEC").interpolate("linear").dropna(how="all")
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
    name = "industrial_production"

    raw = pd.read_excel(urls[name]["dl"]["main"], skiprows=4, usecols="C:DQ", na_values="(s)")
    weights = pd.read_csv(urls[name]["dl"]["weights"]).dropna(how="all")
    weights[["División", "Grupo", "Agrupación / Clase"]] = weights[
        ["División", "Grupo", "Agrupación / Clase"]
    ].astype(str)
    output = raw.dropna(how="all")
    output.index = pd.date_range(start="2018-01-31", freq="M", periods=len(output))

    column_names = []
    for c in output.columns[2:]:
        c = str(c)
        match = weights.loc[weights["División"] == c, "Denominación"]
        prefix = "Div_"
        if isinstance(match, pd.Series) and match.empty:
            prefix = "Gru_"
            match = weights.loc[weights["Grupo"] == c, "Denominación"]
            if isinstance(match, pd.Series) and match.empty:
                prefix = "Cls_"
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
        if len(match) > 60:
            match = match[:58] + "..."
        column_names.append(match)
    output.columns = [
        "Industrias manufactureras",
        "Industrias manufactureras sin refinería",
    ] + column_names
    output = output.apply(pd.to_numeric, errors="coerce")
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

    return output


def core_industrial(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
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
    name = "core_industrial"

    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get("industrial_production")
    data_18_weights = {"other foods": 0.0991, "pulp": 0.0907}
    data_18 = pipeline.dataset
    data_18 = data_18[
        [
            "Industrias manufactureras",
            "Industrias manufactureras sin refinería",
            "Cls_Elaboración de comidas y platos preparados; elaboració...",
            "Cls_Fabricación de pasta de celulosa, papel y cartón",
        ]
    ]
    data_18.columns = ["total", "ex-refinery", "other foods", "pulp"]
    data_18["core"] = data_18["ex-refinery"] - (
        data_18["other foods"] * data_18_weights["other foods"]
        + data_18["pulp"] * data_18_weights["pulp"]
    )

    data_06_weights = {"other foods": 0.08221045, "pulp": 0.00809761}
    data_06 = pd.read_excel(urls[name]["dl"]["2006"], skiprows=4, usecols="B:D,F,CF,CX")
    data_06 = data_06.loc[~data_06["Mes"].str.contains("Prom").astype(bool), :]
    data_06 = data_06.iloc[:, 2:]
    data_06.columns = ["total", "ex-refinery", "other foods", "pulp"]
    data_06.index = pd.date_range(start="2002-01-31", freq="M", periods=len(data_06))
    data_06["core"] = data_06["ex-refinery"] - (
        data_06["other foods"] * data_06_weights["other foods"]
        + data_06["pulp"] * data_06_weights["pulp"]
    )

    output = data_18.reindex(pd.date_range(start="2002-01-31", freq="M", end=data_18.index[-1]))
    for row in reversed(output.index):
        next_month = row + MonthEnd(1)
        if output.loc[row].isna().all():
            output.loc[row] = data_06.loc[row] / data_06.loc[next_month] * output.loc[next_month]
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
def cattle() -> pd.DataFrame:
    """Get weekly cattle slaughter data.

    Returns
    -------
    Weekly cattle slaughter : pd.DataFrame

    """
    name = "cattle"
    try:
        output = pd.read_excel(urls[name]["dl"]["main"], skiprows=8, usecols="C:H")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files", "inac_certs.pem")
            r = requests.get(urls[name]["dl"]["main"], verify=certificate)
            output = pd.read_excel(BytesIO(r.content), skiprows=8, usecols="C:H")
        else:
            raise err

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
def milk() -> pd.DataFrame:
    """Get monthly milk production in farms data.

    Returns
    -------
    Monhtly milk production in farms : pd.DataFrame

    """
    name = "milk"

    r = requests.get(urls[name]["dl"]["main"])
    soup = BeautifulSoup(r.content, features="lxml")
    link = soup.find_all(href=re.compile(".xls"))[0]
    raw = pd.read_excel(link["href"], skiprows=11, skipfooter=4)
    output = raw.iloc[:, 2:].drop(0, axis=0)
    output = pd.melt(output, id_vars="Año/ Mes")[["value"]].dropna()
    output.index = pd.date_range(start="2002-01-31", freq="M", periods=len(output))
    output = output.apply(pd.to_numeric)
    output.columns = ["Remisión de leche a planta"]
    output.rename_axis(None, inplace=True)

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
def cement() -> pd.DataFrame:
    """Get monthly cement sales data.

    Returns
    -------
    Monthly cement sales : pd.DataFrame

    """
    name = "cement"

    output = pd.read_excel(
        urls[name]["dl"]["main"], skiprows=2, usecols="B:E", index_col=0, skipfooter=1
    )
    output.index = output.index + MonthEnd(0)
    output.columns = ["Exportaciones", "Mercado interno", "Total"]
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Actividad económica",
        currency="-",
        inf_adj="No",
        unit="Toneladas",
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
def diesel() -> pd.DataFrame:
    """
    Get diesel sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly diesel dales : pd.DataFrame

    """
    name = "diesel"

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls[name]["dl"]["main"])
        soup = BeautifulSoup(r.content, features="lxml")
        rar_url = soup.find_all(href=re.compile("gas%20oil"))[0]
        f.write(requests.get(rar_url["href"]).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(
            path_temp, sheet_name="vta gas oil por depto", skiprows=2, usecols="C:W"
        )
        raw.index = pd.date_range(start="2004-01-31", freq="M", periods=len(raw))
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
def gasoline() -> pd.DataFrame:
    """
    Get gasoline sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly gasoline dales : pd.DataFrame

    """
    name = "gasoline"

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls[name]["dl"]["main"])
        soup = BeautifulSoup(r.content, features="lxml")
        rar_url = soup.find_all(href=re.compile("gasolina"))[0]
        f.write(requests.get(rar_url["href"]).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(
            path_temp, sheet_name="vta gasolinas por depto", skiprows=2, usecols="C:W"
        )
        raw.index = pd.date_range(start="2004-01-31", freq="M", periods=len(raw))
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
def electricity() -> pd.DataFrame:
    """
    Get electricity sales by sector data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly electricity dales : pd.DataFrame

    """
    name = "electricity"

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls[name]["dl"]["main"])
        soup = BeautifulSoup(r.content, features="lxml")
        rar_url = soup.find_all(href=re.compile("Facturaci[%A-z0-9]+sector"))[0]
        f.write(requests.get(rar_url["href"]).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(path_temp, sheet_name="fact ee", skiprows=2, usecols="C:J")
        raw.index = pd.date_range(start="2000-01-31", freq="M", periods=len(raw))
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
