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
        name: str = "credits",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get credit data.

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
    name : str, default 'credits'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly credit : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    raw = pd.read_excel(urls["credits"]["dl"]["main"],
                        sheet_name="Total Sist. Banc.",
                        skiprows=10, usecols="A:P,T:AB,AD:AL", index_col=0)
    output = raw.loc[~pd.isna(raw.index)].dropna(how="all", axis=1)
    output.index = output.index + MonthEnd(0)
    output.columns = ["Créditos: Resid. privado - vigentes",
                      "Créditos: Resid. privado - vencidos",
                      "Créditos: Resid. privado- total",
                      "Créditos: Resid. público - vigentes",
                      "Créditos: Resid. público - vencidos",
                      "Créditos: Resid. público - total",
                      "Créditos: Resid. total - vigentes",
                      "Créditos: Resid. total - vencidos",
                      "Créditos: Resid. total - total",
                      "Créditos: No residentes - vigentes",
                      "Créditos: No residentes - vencidos",
                      "Créditos: No residentes - total",
                      "Créditos: Total - vigentes",
                      "Créditos: Total - vencidos",
                      "Créditos: Total - total",
                      "Créditos: Resid. MN privado - vigentes",
                      "Créditos: Resid. MN privado - vencidos",
                      "Créditos: Resid. MN privado- total",
                      "Créditos: Resid. MN público - vigentes",
                      "Créditos: Resid. MN público - vencidos",
                      "Créditos: Resid. MN público- total",
                      "Créditos: Resid. MN total - vigentes",
                      "Créditos: Resid. MN total - vencidos",
                      "Créditos: Resid. MN total- total",
                      "Créditos: Resid. ME privado - vigentes",
                      "Créditos: Resid. ME privado - vencidos",
                      "Créditos: Resid. ME privado- total",
                      "Créditos: Resid. ME público - vigentes",
                      "Créditos: Resid. ME público - vencidos",
                      "Créditos: Resid. ME público- total",
                      "Créditos: Resid. ME total - vigentes",
                      "Créditos: Resid. ME total - vencidos",
                      "Créditos: Resid. ME total- total"]

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
