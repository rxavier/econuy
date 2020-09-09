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
        name: str = "rates",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get interest rates data.

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
    name : str, default 'rates'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly interest rates : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    xls = pd.ExcelFile(urls["rates"]["dl"]["main"])
    sheets = ["Activas $", "Activas UI", "Activas U$S",
              "Pasivas $", "Pasivas UI", "Pasivas U$S"]
    columns = ["B:C,G,K", "B:C,G,K", "B:C,H,L",
               "B:C,N,T", "B:C,P,W", "B:C,N,T"]
    sheet_data = []
    for sheet, columns in zip(sheets, columns):
        if "Activas" in sheet:
            skip = 11
        else:
            skip = 10
        data = pd.read_excel(xls, sheet_name=sheet, skiprows=skip,
                             usecols=columns, index_col=0)
        data.index = pd.to_datetime(data.index, errors="coerce")
        data = data.loc[~pd.isna(data.index)]
        data.index = data.index + MonthEnd(0)
        sheet_data.append(data)
    output = pd.concat(sheet_data, axis=1)
    output.columns = ["Tasas activas: $, promedio",
                      "Tasas activas: $, promedio empresas",
                      "Tasas activas: $, promedio familias",
                      "Tasas activas: UI, promedio",
                      "Tasas activas: UI, promedio empresas",
                      "Tasas activas: UI, promedio familias",
                      "Tasas activas: US$, promedio",
                      "Tasas activas: US$, promedio empresas",
                      "Tasas activas: US$, promedio familias",
                      "Tasas pasivas: $, promedio",
                      "Tasas pasivas: $, promedio empresas",
                      "Tasas pasivas: $, promedio familias",
                      "Tasas pasivas: UI, promedio",
                      "Tasas pasivas: UI, promedio empresas",
                      "Tasas pasivas: UI, promedio familias",
                      "Tasas pasivas: US$, promedio",
                      "Tasas pasivas: US$, promedio empresas",
                      "Tasas pasivas: US$, promedio familias"]

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    output = output.apply(pd.to_numeric, errors="coerce")
    metadata._set(output, area="Sector financiero", currency="-",
                  inf_adj="-", unit="Tasa", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)
    metadata._modify_multiindex(output, levels=[3, 4],
                                new_arrays=[["UYU", "UYU", "UYU",
                                             "UYU", "UYU", "UYU",
                                             "USD", "USD", "USD",
                                             "UYU", "UYU", "UYU",
                                             "UYU", "UYU", "UYU",
                                             "USD", "USD", "USD"],
                                            ["No", "No", "No",
                                             "Const.", "Const.", "Const.",
                                             "No", "No", "No",
                                             "No", "No", "No",
                                             "Const.", "Const.", "Const.",
                                             "No", "No", "No"]])

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output
