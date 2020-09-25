import re
from os import PathLike
from typing import Union
from urllib.error import URLError, HTTPError

import pandas as pd
import requests
from bs4 import BeautifulSoup
from opnieuw import retry
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import ops, metadata
from econuy.utils.lstrings import urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get_bond_index(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "ubi",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get Uruguayan Bond Index data.

    This function requires a Selenium webdriver. It can be provided in the
    driver parameter, or it will attempt to configure a Chrome webdriver.

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
    name : str, default 'ubi'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Uruguayan Bond Index : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    historical = pd.read_excel(urls["ubi"]["dl"]["historical"], usecols="B:C",
                               skiprows=1, index_col=0,
                               sheet_name="Valores de Cierre Diarios")
    r = requests.get(urls["ubi"]["dl"]["current"])
    soup = BeautifulSoup(r.content, features="lxml")
    raw_string = soup.find_all(type="hidden")[0]["value"]
    raw_list = raw_string.split("],")
    raw_list = [re.sub('["\[\]]', "", line) for line in raw_list]
    index = [x.split(",")[0] for x in raw_list]
    values = [x.split(",")[1] for x in raw_list]
    current = pd.DataFrame(data=values, index=index, columns=["UBI"])
    current.index = pd.to_datetime(current.index, format="%d/%m/%y")
    output = pd.concat([historical, current])
    output = output.loc[~output.index.duplicated(keep="last")]
    output = output.apply(pd.to_numeric, errors="coerce")

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Sector financiero", currency="USD",
                  inf_adj="No", unit="PBS", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output
