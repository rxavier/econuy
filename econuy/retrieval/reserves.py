import datetime as dt
import urllib
from os import PathLike
from typing import Union
from urllib.error import URLError, HTTPError

import pandas as pd
from dateutil.relativedelta import relativedelta
from opnieuw import retry
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import metadata, ops
from econuy.utils.lstrings import (reserves_url, reserves_cols,
                                   missing_reserves_url)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=90,
)
def get_changes(update_loc: Union[str, PathLike, Engine,
                                  Connection, None] = None,
                save_loc: Union[str, PathLike, Engine,
                                Connection, None] = None,
                name: str = "reserves_chg",
                index_label: str = "index",
                only_get: bool = False) -> pd.DataFrame:
    """Get international reserves change data.

    Parameters
    ----------
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    name : str, default 'reserves_chg'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Daily international reserves changes : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    months = ["ene", "feb", "mar", "abr", "may", "jun",
              "jul", "ago", "set", "oct", "nov", "dic"]
    years = list(range(2013, dt.datetime.now().year + 1))
    files = [month + str(year) for year in years for month in months]
    first_dates = list(pd.date_range(start="2013-01-01",
                                     periods=len(files), freq="MS"))

    urls = [f"{reserves_url}{file}.xls" for file in files]
    wrong_may14 = f"{reserves_url}may2014.xls"
    fixed_may14 = f"{reserves_url}mayo2014.xls"
    urls = [fixed_may14 if x == wrong_may14 else x for x in urls]

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name)
        if not previous_data.equals(pd.DataFrame()):
            metadata._set(previous_data)
            previous_data.columns.set_levels(["-"], level=2, inplace=True)
            previous_data.columns = reserves_cols[1:46]
            previous_data.index = pd.to_datetime(previous_data.index)
            urls = urls[-18:]
            first_dates = first_dates[-18:]

    reports = []
    for url, first_day in zip(urls, first_dates):
        try:
            raw = pd.read_excel(url, sheet_name="ACTIVOS DE RESERVA",
                                skiprows=3)
            last_day = (first_day
                        + relativedelta(months=1)
                        - dt.timedelta(days=1))
            proc = raw.dropna(axis=0, thresh=20).dropna(axis=1, thresh=20)
            proc = proc.transpose()
            proc.index.name = "Date"
            proc = proc.iloc[:, 1:46]
            proc.columns = reserves_cols[1:46]
            proc = proc.iloc[1:]
            proc.index = pd.to_datetime(proc.index, errors="coerce")
            proc = proc.loc[proc.index.dropna()]
            proc = proc.loc[first_day:last_day]
            reports.append(proc)

        except urllib.error.HTTPError:
            print(f"{url} could not be reached.")
            pass

    mar14 = pd.read_excel(missing_reserves_url, index_col=0)
    mar14.columns = reserves_cols[1:46]
    reserves = pd.concat(reports + [mar14], sort=False).sort_index()

    if update_loc is not None:
        reserves = previous_data.append(reserves, sort=False)
        reserves = reserves.loc[~reserves.index.duplicated(keep="last")]

    reserves = reserves.apply(pd.to_numeric, errors="coerce")
    metadata._set(reserves, area="Reservas internacionales",
                  currency="USD", inf_adj="No", unit="Millones",
                  seas_adj="NSA", ts_type="Flujo", cumperiods=1)
    reserves.columns.set_levels(["-"], level=2, inplace=True)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=reserves, name=name, index_label=index_label)

    return reserves
