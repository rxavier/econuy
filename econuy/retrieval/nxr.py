import datetime as dt
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
    retry_window_after_first_call_in_seconds=30,
)
def get_monthly(update_loc: Union[str, PathLike,
                                  Engine, Connection, None] = None,
                revise_rows: Union[str, int] = "nodup",
                save_loc: Union[str, PathLike,
                                Engine, Connection, None] = None,
                name: str = "nxr_monthly",
                index_label: str = "index",
                only_get: bool = False) -> pd.DataFrame:
    """Get monthly nominal exchange rate data.

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
    name : str, default 'nxr_monthly'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly nominal exchange rates : pd.DataFrame
        Sell rate, monthly average and end of period.

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    nxr_raw = pd.read_excel(urls["nxr_monthly"]["dl"]["main"],
                            skiprows=4, index_col=0, usecols="A,C,F")
    nxr = nxr_raw.dropna(how="any", axis=0)
    nxr.columns = ["Tipo de cambio venta, fin de período",
                   "Tipo de cambio venta, promedio"]
    nxr.index = nxr.index + MonthEnd(1)
    nxr = nxr.apply(pd.to_numeric, errors="coerce")

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        nxr = ops._revise(new_data=nxr, prev_data=previous_data,
                          revise_rows=revise_rows)

    metadata._set(nxr, area="Precios y salarios", currency="UYU/USD",
                  inf_adj="No", unit="-", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=nxr, name=name, index_label=index_label)

    return nxr


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=60,
)
def get_daily(update_loc: Union[str, PathLike,
                                Engine, Connection, None] = None,
              save_loc: Union[str, PathLike,
                              Engine, Connection, None] = None,
              name: str = "nxr_daily",
              index_label: str = "index",
              only_get: bool = False) -> pd.DataFrame:
    """Get daily nominal exchange rate data.

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
    name : str, default 'nxr_daily'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly nominal exchange rates : pd.DataFrame
        Sell rate, monthly average and end of period.

    """
    if only_get is True and update_loc is not None:
        return ops._io(operation="update", data_loc=update_loc,
                       name=name, index_label=index_label)

    start_date = dt.datetime(1999, 12, 31)

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        metadata._set(previous_data)
        try:
            start_date = previous_data.index[len(previous_data) - 1]
        except IndexError:
            pass

    today = dt.datetime.now() - dt.timedelta(days=1)
    runs = (today - start_date).days // 30
    data = []
    base_url = urls['nxr_daily']['dl']['main']
    if runs > 0:
        for i in range(1, runs + 1):
            from_ = (start_date + dt.timedelta(days=1)).strftime('%d/%m/%Y')
            to_ = (start_date + dt.timedelta(days=30)).strftime('%d/%m/%Y')
            dates = f"%22FechaDesde%22:%22{from_}%22,%22FechaHasta%22:%22{to_}"
            url = f"{base_url}{dates}%22,%22Grupo%22:%222%22}}" + "}"
            try:
                data.append(pd.read_excel(url))
                start_date = dt.datetime.strptime(to_, '%d/%m/%Y')
            except TypeError:
                pass
    from_ = (start_date + dt.timedelta(days=1)).strftime('%d/%m/%Y')
    to_ = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%d/%m/%Y')
    dates = f"%22FechaDesde%22:%22{from_}%22,%22FechaHasta%22:%22{to_}"
    url = f"{base_url}{dates}%22,%22Grupo%22:%222%22}}" + "}"
    try:
        data.append(pd.read_excel(url))
    except TypeError:
        pass
    try:
        output = pd.concat(data, axis=0)
        output = output.pivot(index="Fecha", columns="Moneda",
                              values="Venta").rename_axis(None)
        output.index = pd.to_datetime(output.index, format="%d/%m/%Y",
                                      errors="coerce")
        output.sort_index(inplace=True)
        output.replace(",", ".", regex=True, inplace=True)
        output.columns = ["Tipo de cambio US$, Cable"]
        output = output.apply(pd.to_numeric, errors="coerce")

        metadata._set(output, area="Precios y salarios", currency="UYU/USD",
                      inf_adj="No", unit="-", seas_adj="NSA",
                      ts_type="-", cumperiods=1)
        output.columns.set_levels(["-"], level=2, inplace=True)

        if update_loc is not None:
            output = pd.concat([previous_data, output])

        if save_loc is not None:
            ops._io(operation="save", data_loc=save_loc,
                    data=output, name=name, index_label=index_label)

    except ValueError as e:
        if str(e) == "No objects to concatenate":
            return previous_data

    return output
