import datetime as dt
from os import PathLike
from typing import Union, Dict
from urllib.error import URLError, HTTPError

import pandas as pd
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import ops, metadata
from econuy.utils.lstrings import trade_metadata, urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=8,
    retry_window_after_first_call_in_seconds=120,
)
def get(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "tb", index_label: str = "index",
        only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get trade balance data.

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
    name : str, default 'tb'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly exports and imports : Dict[str, pd.DataFrame]
        Value, volume and price data for product and destination
        classifications (exports), and sector and origin classifications
        (imports).

    """
    if only_get is True and update_loc is not None:
        output = {}
        for file in trade_metadata.keys():
            data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{file}", index_label=index_label
            )
            output.update({f"{name}_{file}": data})
        if all(not value.equals(pd.DataFrame()) for value in output.values()):
            return output

    output = {}
    for file, meta in trade_metadata.items():
        xls = pd.ExcelFile(meta["url"])
        sheets = []
        for sheet in xls.sheet_names:
            raw = (pd.read_excel(xls, sheet_name=sheet,
                                 usecols=meta["cols"],
                                 index_col=0,
                                 skiprows=7).dropna(thresh=5).T)
            raw.index = (pd.to_datetime(raw.index, errors="coerce")
                         + MonthEnd(0))
            proc = raw[raw.index.notnull()].dropna(thresh=5, axis=1)
            if file != "m_sect_val":
                proc = proc.loc[:, meta["old_colnames"]]
            else:
                proc = proc.loc[:, ~(proc == "miles de dólares").any()]
                proc = proc.drop(columns=["DESTINO ECONÓMICO"])
            proc.columns = meta["new_colnames"]
            sheets.append(proc)
        data = pd.concat(sheets).sort_index()
        data = data.apply(pd.to_numeric, errors="coerce")
        if meta["unit"] == "Millones":
            data = data.div(1000)

        if update_loc is not None:
            previous_data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{file}", index_label=index_label
            )
            data = ops._revise(new_data=data,
                               prev_data=previous_data,
                               revise_rows=revise_rows)

        metadata._set(
            data, area="Sector externo", currency=meta["currency"],
            inf_adj="No", unit=meta["unit"], seas_adj="NSA",
            ts_type="Flujo", cumperiods=1
        )

        if save_loc is not None:
            ops._io(
                operation="save", data_loc=save_loc, data=data,
                name=f"{name}_{file}", index_label=index_label
            )

        output.update({f"{name}_{file}": data})

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get_containers(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "containers", index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get monthly container traffic at the Montevideo port.

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
    name : str, default 'container'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly caontainer traffic : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    years = list(range(2007, dt.datetime.now().year + 1))
    years[11] = "contenedores"
    years[12] = "Ano+2019"
    data = []
    for year in years:
        link = f'{urls["containers"]["dl"]["main"]}{year}'
        try:
            raw = pd.read_html(link, skiprows=3, header=0, thousands=".")[0]
            raw = raw.loc[~raw["MES"].str.contains("TOTALES")]
            raw = raw.iloc[:, 1:]
            data.append(raw)
        except HTTPError:
            continue
    output = pd.concat(data)
    output.columns = ["Descarga: llenos - 40'", "Descarga: llenos - 20'",
                      "Descarga: vacíos - 40'", "Descarga: vacíos - 20'",
                      "Descarga: total",
                      "Carga: llenos - 40'", "Carga: llenos - 20'",
                      "Carga: vacíos - 40'", "Carga: vacíos - 20'",
                      "Carga: total", "Carga y descarga: total"]
    output.index = pd.date_range(start="2007-01-31", freq="M", periods=len(output))
    output = output.loc[(output != 0).all(axis=1)]

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="Unidades", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output
