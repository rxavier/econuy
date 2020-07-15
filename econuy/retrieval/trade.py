from os import PathLike
from typing import Union, Dict
from urllib.error import URLError, HTTPError

import pandas as pd
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import ops, metadata
from econuy.utils.lstrings import trade_metadata


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
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
