from os import PathLike
from typing import Union

import pandas as pd
from pandas.tseries.offsets import MonthEnd
from urllib.error import URLError, HTTPError
from opnieuw import retry
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import updates, metadata
from econuy.utils.lstrings import labor_url, wages1_url, wages2_url


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get_rates(update_path: Union[str, PathLike,
                                 Engine, Connection, None] = None,
              revise_rows: Union[str, int] = "nodup",
              save_path: Union[str, PathLike,
                               Engine, Connection, None] = None,
              name: str = "labor",
              index_label: str = "index",
              only_get: bool = False) -> pd.DataFrame:
    """Get labor market data.

    Get monthly labor force participation rate, employment rate (employment to
    working-age population) and unemployment rate.

    Parameters
    ----------
    update_path : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
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
    save_path : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
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
        ``update_path``.

    Returns
    -------
    Monthly participation, employment and unemployment rates : pd.DataFrame

    """
    if only_get is True and update_path is not None:
        return updates._update_save(operation="update", data_path=update_path,
                                    name=name, index_label=index_label)

    labor_raw = pd.read_excel(labor_url, skiprows=39).dropna(axis=0, thresh=2)
    labor = labor_raw[~labor_raw["Unnamed: 0"].str.contains("-|/|Total",
                                                            regex=True)]
    labor = labor[["Unnamed: 1", "Unnamed: 4", "Unnamed: 7"]]
    labor.index = pd.date_range(start="2006-01-01",
                                periods=len(labor), freq="M")
    labor.columns = ["Tasa de actividad", "Tasa de empleo",
                     "Tasa de desempleo"]

    if update_path is not None:
        previous_data = updates._update_save(operation="update",
                                             data_path=update_path,
                                             name=name,
                                             index_label=index_label)
        labor = updates._revise(new_data=labor, prev_data=previous_data,
                                revise_rows=revise_rows)

    labor = labor.apply(pd.to_numeric, errors="coerce")
    metadata._set(labor, area="Mercado laboral", currency="-",
                  inf_adj="No", unit="Tasa", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_path is not None:
        updates._update_save(operation="save", data_path=save_path,
                             data=labor, name=name, index_label=index_label)

    return labor


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get_wages(update_path: Union[str, PathLike,
                                 Engine, Connection, None] = None,
              revise_rows: Union[str, int] = "nodup",
              save_path: Union[str, PathLike,
                               Engine, Connection, None] = None,
              name: str = "wages",
              index_label: str = "index",
              only_get: bool = False) -> pd.DataFrame:
    """Get general, public and private sector wages data

    Parameters
    ----------
    update_path : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
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
    save_path : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
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
        ``update_path``.

    Returns
    -------
    Monthly wages separated by public and private sector : pd.DataFrame

    """
    if only_get is True and update_path is not None:
        return updates._update_save(operation="update", data_path=update_path,
                                    name=name, index_label=index_label)

    historical = pd.read_excel(wages1_url, skiprows=8, usecols="A:B")
    historical = historical.dropna(how="any").set_index("Unnamed: 0")
    current = pd.read_excel(wages2_url, skiprows=8, usecols="A,C:D")
    current = current.dropna(how="any").set_index("Unnamed: 0")
    wages = pd.concat([historical, current], axis=1)
    wages.index = wages.index + MonthEnd(1)
    wages.columns = ["Índice medio de salarios",
                     "Índice medio de salarios privados",
                     "Índice medio de salarios públicos"]

    if update_path is not None:
        previous_data = updates._update_save(operation="update",
                                             data_path=update_path,
                                             name=name,
                                             index_label=index_label)
        wages = updates._revise(new_data=wages, prev_data=previous_data,
                                revise_rows=revise_rows)

    wages = wages.apply(pd.to_numeric, errors="coerce")
    metadata._set(wages, area="Mercado laboral", currency="UYU",
                  inf_adj="No", unit="2008-07-31=100", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_path is not None:
        updates._update_save(operation="save", data_path=save_path,
                             data=wages, name=name, index_label=index_label)

    return wages
