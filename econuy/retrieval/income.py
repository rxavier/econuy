from os import PathLike
from pathlib import Path
from io import BytesIO
from typing import Union
from urllib.error import URLError, HTTPError

import pandas as pd
import requests
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import ops, metadata, get_project_root
from econuy.utils.sources import urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def income_household() -> pd.DataFrame:
    """Get average household income.

    Parameters
    ----------
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly average household income : pd.DataFrame

    """
    name = "income_household"

    try:
        raw = pd.read_excel(
            urls[name]["dl"]["main"], sheet_name="Mensual", skiprows=5, index_col=0
        ).dropna(how="all")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files", "ine_certs.pem")
            r = requests.get(urls[name]["dl"]["main"], verify=certificate)
            raw = pd.read_excel(
                BytesIO(r.content), sheet_name="Mensual", skiprows=5, index_col=0
            ).dropna(how="all")
        else:
            raise err
    raw.index = pd.to_datetime(raw.index)
    output = raw.loc[~pd.isna(raw.index)]
    output.index = output.index + MonthEnd(0)
    output.columns = [
        "Total país",
        "Montevideo",
        "Interior: total",
        "Interior: localidades de más de 5 mil hab.",
        "Interior: localidades pequeñas y rural",
    ]

    missing = pd.read_excel(urls[name]["dl"]["missing"], index_col=0, header=0).iloc[:, 10:13]
    missing.columns = output.columns[:3]
    output = output.append(missing, sort=False)
    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Ingresos",
        currency="UYU",
        inf_adj="No",
        unit="Pesos",
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
def income_capita(
    update_loc: Union[str, PathLike, Engine, Connection, None] = None,
    revise_rows: Union[str, int] = "nodup",
    save_loc: Union[str, PathLike, Engine, Connection, None] = None,
    only_get: bool = False,
) -> pd.DataFrame:
    """Get average per capita income.

    Parameters
    ----------
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly average per capita income : pd.DataFrame

    """
    name = "income_capita"

    if only_get is True and update_loc is not None:
        output = ops._io(operation="read", data_loc=update_loc, name=name)
        if not output.equals(pd.DataFrame()):
            return output
    try:
        raw = pd.read_excel(
            urls[name]["dl"]["main"], sheet_name="Mensuall", skiprows=5, index_col=0
        ).dropna(how="all")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files", "ine_certs.pem")
            r = requests.get(urls[name]["dl"]["main"], verify=certificate)
            raw = pd.read_excel(
                BytesIO(r.content), sheet_name="Mensuall", skiprows=5, index_col=0
            ).dropna(how="all")
        else:
            raise err
    raw.index = pd.to_datetime(raw.index)
    output = raw.loc[~pd.isna(raw.index)]
    output.index = output.index + MonthEnd(0)
    output.columns = [
        "Total país",
        "Montevideo",
        "Interior: total",
        "Interior: localidades de más de 5 mil hab.",
        "Interior: localidades pequeñas y rural",
    ]

    missing = pd.read_excel(urls[name]["dl"]["missing"], index_col=0, header=0).iloc[:, 13:16]
    missing.columns = output.columns[:3]
    output = output.append(missing, sort=False)
    output.rename_axis(None, inplace=True)

    if update_loc is not None:
        previous_data = ops._io(operation="read", data_loc=update_loc, name=name)
        output = ops._revise(new_data=output, prev_data=previous_data, revise_rows=revise_rows)

    metadata._set(
        output,
        area="Ingresos",
        currency="UYU",
        inf_adj="No",
        unit="Pesos",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc, data=output, name=name)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def consumer_confidence(
    update_loc: Union[str, PathLike, Engine, Connection, None] = None,
    revise_rows: Union[str, int] = "nodup",
    save_loc: Union[str, PathLike, Engine, Connection, None] = None,
    only_get: bool = False,
) -> pd.DataFrame:
    """Get monthly consumer confidence data.

    Parameters
    ----------
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly consumer confidence data : pd.DataFrame

    """
    name = "consumer_confidence"

    if only_get is True and update_loc is not None:
        output = ops._io(operation="read", data_loc=update_loc, name=name)
        if not output.equals(pd.DataFrame()):
            return output

    raw = pd.read_excel(urls[name]["dl"]["main"], skiprows=3, usecols="B:F", index_col=0)
    output = raw.loc[~pd.isna(raw.index)]
    output.index = output.index + MonthEnd(0)
    output.columns = [
        "Subíndice: Situación Económica Personal",
        "Subíndice: Situación Económica del País",
        "Subíndice: Predisposición a la Compra de Durables",
        "Índice de Confianza del Consumidor",
    ]
    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)

    if update_loc is not None:
        previous_data = ops._io(operation="read", data_loc=update_loc, name=name)
        output = ops._revise(new_data=output, prev_data=previous_data, revise_rows=revise_rows)

    metadata._set(
        output,
        area="Actividad económica",
        currency="-",
        inf_adj="No",
        unit="50 = neutralidad",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc, data=output, name=name)

    return output
