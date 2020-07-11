import re
import tempfile
from os import PathLike, path, listdir
from typing import Union, Dict

import pandas as pd
import patoolib
import requests
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from requests.exceptions import ConnectionError, HTTPError
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import ops, metadata
from econuy.utils.lstrings import urls, fiscal_sheets


@retry(
    retry_on_exceptions=(HTTPError, ConnectionError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "fiscal",
        index_label: str = "index",
        only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get fiscal data.

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
    name : str, default 'fiscal'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal accounts different aggregations : Dict[str, pd.DataFrame]
        Available aggregations: non-financial public sector, consolidated
        public sector, central government, aggregated public enterprises
        and individual public enterprises.

    """
    if only_get is True and update_loc is not None:
        output = {}
        for meta in fiscal_sheets.values():
            data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{meta['Name']}", index_label=index_label
            )
            output.update({meta["Name"]: data})
        if all(not value.equals(pd.DataFrame()) for value in output.values()):
            return output

    response = requests.get(urls["fiscal"]["dl"]["main"])
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all(href=re.compile("\\.rar$"))
    rar = links[0]["href"]
    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        f.write(requests.get(rar).content)

    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        path_temp = path.join(temp_dir, listdir(temp_dir)[0])

        output = {}
        with pd.ExcelFile(path_temp) as xls:
            for sheet, meta in fiscal_sheets.items():
                data = (pd.read_excel(xls, sheet_name=sheet).
                        dropna(axis=0, thresh=4).dropna(axis=1, thresh=4).
                        transpose().set_index(2, drop=True))
                data.columns = data.iloc[0]
                data = data[data.index.notnull()].rename_axis(None)
                data.index = data.index + MonthEnd(1)
                data.columns = meta["Colnames"]

                if update_loc is not None:
                    previous_data = ops._io(
                        operation="update", data_loc=update_loc,
                        name=f"{name}_{meta['Name']}", index_label=index_label
                    )
                    data = ops._revise(new_data=data,
                                       prev_data=previous_data,
                                       revise_rows=revise_rows)
                data = data.apply(pd.to_numeric, errors="coerce")
                metadata._set(
                    data, area="Cuentas fiscales y deuda", currency="UYU",
                    inf_adj="No", unit="Millones", seas_adj="NSA",
                    ts_type="Flujo", cumperiods=1
                )

                if save_loc is not None:
                    ops._io(
                        operation="save", data_loc=save_loc, data=data,
                        name=f"{name}_{meta['Name']}", index_label=index_label
                    )

                output.update({meta["Name"]: data})

    return output
