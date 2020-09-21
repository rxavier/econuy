import re
import tempfile
from os import PathLike, path, listdir
from typing import Union
from urllib.error import URLError, HTTPError

import pandas as pd
import patoolib
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
def get_diesel(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "diesel",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get diesel sales by department data.

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
    name : str, default 'diesel'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly diesel dales : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls["diesel"]["dl"]["main"])
        soup = BeautifulSoup(r.content, features="lxml")
        rar_url = soup.find_all(href=re.compile("venta[%20a-z]+oil"))[0]
        f.write(requests.get(rar_url["href"]).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(path_temp, sheet_name="vta gas oil por depto",
                            skiprows=2, usecols="C:W")
        raw.index = pd.date_range(start="2004-01-31", freq="M",
                                  periods=len(raw))
        raw.columns = list(raw.columns.str.replace("\n", " "))[:-1] + ["Total"]
        output = raw

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="m3", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get_gasoline(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "gasoline",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get gasoline sales by department data.

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
    name : str, default 'gasoline'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly gasoline dales : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls["gasoline"]["dl"]["main"])
        soup = BeautifulSoup(r.content, features="lxml")
        rar_url = soup.find_all(href=re.compile("venta[%20a-z]+gasolina"))[0]
        f.write(requests.get(rar_url["href"]).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(path_temp, sheet_name="vta gasolinas por depto",
                            skiprows=2, usecols="C:W")
        raw.index = pd.date_range(start="2004-01-31", freq="M",
                                  periods=len(raw))
        raw.columns = list(raw.columns.str.replace("\n", " "))[:-1] + ["Total"]
        output = raw

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="m3", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get_electricity(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "electricity",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get electricity sales by sector data.

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
    name : str, default 'electricity'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly electricity dales : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls["electricity"]["dl"]["main"])
        soup = BeautifulSoup(r.content, features="lxml")
        rar_url = soup.find_all(href=re.compile("facturaci[%A-z0-9]+sector"))[0]
        f.write(requests.get(rar_url["href"]).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(path_temp, sheet_name="fact ee",
                            skiprows=2, usecols="C:J")
        raw.index = pd.date_range(start="2000-01-31", freq="M",
                                  periods=len(raw))
        raw.columns = raw.columns.str.capitalize()
        output = raw

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="MWh", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output
