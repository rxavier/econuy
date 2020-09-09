from os import PathLike
from typing import Union
from urllib.error import URLError, HTTPError

import pandas as pd
from pandas.tseries.offsets import MonthEnd
from opnieuw import retry
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import ops, metadata
from econuy.utils.lstrings import urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get(update_loc: Union[str, PathLike,
                          Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike,
                        Engine, Connection, None] = None,
        name: str = "deposits",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get deposits data.

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
    name : str, default 'deposits'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly deposits : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    raw = pd.read_excel(urls["deposits"]["dl"]["main"],
                        sheet_name="Total Sist. Banc.",
                        skiprows=8, usecols="A:S", index_col=0)
    output = raw.loc[~pd.isna(raw.index)].dropna(how="all", axis=1)
    output.index = output.index + MonthEnd(0)
    output.columns = ["Depósitos: S. privado - MN",
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
                      "Depósitos: S. privado - no residente"]

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    output = output.apply(pd.to_numeric, errors="coerce")
    metadata._set(output, area="Sector financiero", currency="USD",
                  inf_adj="No", unit="Millones", seas_adj="NSA",
                  ts_type="Stock", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output
