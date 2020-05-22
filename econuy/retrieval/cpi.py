from os import PathLike
from typing import Union

import pandas as pd
from pandas.tseries.offsets import MonthEnd
from urllib.error import URLError, HTTPError
from opnieuw import retry
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import updates, metadata
from econuy.utils.lstrings import cpi_url


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get(update_path: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_path: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "cpi",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get CPI data.

    Parameters
    ----------
    update_path : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
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
    save_path : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    name : str, default 'cpi'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_path``.

    Returns
    -------
    Monthly CPI index : pd.DataFrame

    """
    if only_get is True:
        return updates._update_save(operation="update", data_path=update_path,
                                    name=name, index_label=index_label)

    cpi_raw = pd.read_excel(cpi_url, skiprows=7).dropna(axis=0, thresh=2)
    cpi = (cpi_raw.drop(["Mensual", "Acum.año", "Acum.12 meses"], axis=1).
           dropna(axis=0, how="all").set_index("Mes y año").rename_axis(None))
    cpi.columns = ["Índice de precios al consumo"]
    cpi.index = cpi.index + MonthEnd(1)

    if update_path is not None:
        previous_data = updates._update_save(
            operation="update", data_path=update_path,
            name=name, index_label=index_label
        )
        cpi = updates._revise(new_data=cpi, prev_data=previous_data,
                              revise_rows=revise_rows)

    cpi = cpi.apply(pd.to_numeric, errors="coerce")
    metadata._set(cpi, area="Precios y salarios", currency="-",
                  inf_adj="No", unit="2010-10-31=100", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_path is not None:
        updates._update_save(operation="save", data_path=save_path,
                             data=cpi, name=name, index_label=index_label)

    return cpi
