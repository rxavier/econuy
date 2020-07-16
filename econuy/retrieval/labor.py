from os import PathLike
from typing import Union
from urllib.error import URLError, HTTPError

import pandas as pd
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import ops, metadata
from econuy.utils.lstrings import urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get_rates(update_loc: Union[str, PathLike,
                                Engine, Connection, None] = None,
              revise_rows: Union[str, int] = "nodup",
              save_loc: Union[str, PathLike,
                              Engine, Connection, None] = None,
              name: str = "labor",
              index_label: str = "index",
              only_get: bool = False) -> pd.DataFrame:
    """Get labor market data.

    Get monthly labor force participation rate, employment rate (employment to
    working-age population) and unemployment rate.

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
    name : str, default 'labor'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly participation, employment and unemployment rates : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    labor_raw = pd.read_excel(urls["labor"]["dl"]["main"],
                              skiprows=39).dropna(axis=0, thresh=2)
    labor = labor_raw[~labor_raw["Unnamed: 0"].str.contains("-|/|Total",
                                                            regex=True)]
    labor.index = pd.date_range(start="2006-01-01",
                                periods=len(labor), freq="M")
    labor = labor.drop(columns="Unnamed: 0")
    labor.columns = ["Tasa de actividad: total", "Tasa de actividad: hombres",
                     "Tasa de actividad: mujeres", "Tasa de empleo: total",
                     "Tasa de empleo: hombres", "Tasa de empleo: mujeres",
                     "Tasa de desempleo: total", "Tasa de desempleo: hombres",
                     "Tasa de desempleo: mujeres"]
    missing = pd.read_excel(urls["labor"]["dl"]["missing"],
                            index_col=0, header=0)
    missing.columns = labor.columns
    labor = labor.append(missing)
    labor = labor.loc[~labor.index.duplicated(keep="first")]

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        labor = ops._revise(new_data=labor, prev_data=previous_data,
                            revise_rows=revise_rows)

    labor = labor.apply(pd.to_numeric, errors="coerce")
    metadata._set(labor, area="Mercado laboral", currency="-",
                  inf_adj="No", unit="Tasa", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=labor, name=name, index_label=index_label)

    return labor


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get_wages(update_loc: Union[str, PathLike,
                                Engine, Connection, None] = None,
              revise_rows: Union[str, int] = "nodup",
              save_loc: Union[str, PathLike,
                              Engine, Connection, None] = None,
              name: str = "wages",
              index_label: str = "index",
              only_get: bool = False) -> pd.DataFrame:
    """Get general, public and private sector wages data

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
    name : str, default 'wages'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly wages separated by public and private sector : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    historical = pd.read_excel(urls["wages"]["dl"]["historical"],
                               skiprows=8, usecols="A:B")
    historical = historical.dropna(how="any").set_index("Unnamed: 0")
    current = pd.read_excel(urls["wages"]["dl"]["current"],
                            skiprows=8, usecols="A,C:D")
    current = current.dropna(how="any").set_index("Unnamed: 0")
    wages = pd.concat([historical, current], axis=1)
    wages.index = wages.index + MonthEnd(1)
    wages.columns = ["Índice medio de salarios",
                     "Índice medio de salarios privados",
                     "Índice medio de salarios públicos"]

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        wages = ops._revise(new_data=wages, prev_data=previous_data,
                            revise_rows=revise_rows)

    wages = wages.apply(pd.to_numeric, errors="coerce")
    metadata._set(wages, area="Mercado laboral", currency="UYU",
                  inf_adj="No", unit="2008-07=100", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=wages, name=name, index_label=index_label)

    return wages
