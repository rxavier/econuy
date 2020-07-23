import datetime as dt
from os import PathLike
from typing import Union, Dict
from urllib.error import URLError, HTTPError

import pandas as pd
from opnieuw import retry
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import ops, metadata
from econuy.utils.lstrings import urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=30,
)
def get(update_loc: Union[str, PathLike,
                          Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike,
                        Engine, Connection, None] = None,
        name: str = "public_debt",
        index_label: str = "index",
        only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get public debt.

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
    name : str, default 'public_debt'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Quarterly public debt data : pd.DataFrame
        Global public sector, non-monetary public sector and BCU debts.

    """
    if only_get is True and update_loc is not None:
        output = {}
        for meta in ["gps", "nfps", "cb", "assets"]:
            data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{meta}", index_label=index_label
            )
            output.update({meta: data})
        if all(not value.equals(pd.DataFrame()) for value in output.values()):
            return output

    colnames = ["Total deuda", "Plazo contractual: hasta 1 año",
                "Plazo contractual: entre 1 y 5 años",
                "Plazo contractual: más de 5 años",
                "Plazo residual: hasta 1 año",
                "Plazo residual: entre 1 y 5 años",
                "Plazo residual: más de 5 años",
                "Moneda: pesos", "Moneda: dólares", "Moneda: euros",
                "Moneda: yenes", "Moneda: DEG", "Moneda: otras",
                "Residencia: no residentes", "Residencia: residentes"]

    xls = pd.ExcelFile(urls["public_debt"]["dl"]["main"])
    gps_raw = pd.read_excel(xls, sheet_name="SPG2",
                            usecols="B:Q", index_col=0, skiprows=10,
                            nrows=(dt.datetime.now().year - 1999) * 4)
    gps = gps_raw.dropna(how="any", thresh=2)
    gps.index = pd.date_range(start="1999-12-31", periods=len(gps),
                              freq="Q-DEC")
    gps.columns = colnames

    nfps_raw = pd.read_excel(xls, sheet_name="SPNM bruta",
                             usecols="B:O", index_col=0,
                             skiprows=(dt.datetime.now().year - 1999) * 8 + 18)
    nfps = nfps_raw.dropna(how="any")
    nfps.index = pd.date_range(start="1999-12-31", periods=len(nfps),
                               freq="Q-DEC")
    nfps_extra_raw = pd.read_excel(xls, sheet_name="SPNM bruta",
                                   usecols="O:P", skiprows=11,
                                   nrows=(dt.datetime.now().year - 1999) * 4)
    nfps_extra = nfps_extra_raw.dropna(how="all")
    nfps_extra.index = nfps.index
    nfps = pd.concat([nfps, nfps_extra], axis=1)
    nfps.columns = colnames

    cb_raw = pd.read_excel(xls, sheet_name="BCU bruta",
                           usecols="B:O", index_col=0,
                           skiprows=(dt.datetime.now().year - 1999) * 8 + 20)
    cb = cb_raw.dropna(how="any")
    cb.index = pd.date_range(start="1999-12-31", periods=len(cb),
                             freq="Q-DEC")
    cb_extra_raw = pd.read_excel(xls, sheet_name="BCU bruta",
                                 usecols="O:P", skiprows=11,
                                 nrows=(dt.datetime.now().year - 1999) * 4)
    bcu_extra = cb_extra_raw.dropna(how="all")
    bcu_extra.index = cb.index
    cb = pd.concat([cb, bcu_extra], axis=1)
    cb.columns = colnames

    assets_raw = pd.read_excel(xls, sheet_name="Activos Neta",
                               usecols="B,C,D,K", index_col=0, skiprows=13,
                               nrows=(dt.datetime.now().year - 1999) * 4)
    assets = assets_raw.dropna(how="any")
    assets.index = pd.date_range(start="1999-12-31", periods=len(assets),
                                 freq="Q-DEC")
    assets.columns = ["Total activos", "Sector público no monetario",
                      "BCU"]

    output = {"gps": gps, "nfps": nfps, "cb": cb, "assets": assets}

    for meta, data in output.items():
        if update_loc is not None:
            previous_data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{meta}", index_label=index_label
            )
            data = ops._revise(new_data=data,
                               prev_data=previous_data,
                               revise_rows=revise_rows)
        metadata._set(data, area="Cuentas fiscales y deuda", currency="USD",
                      inf_adj="No", unit="Millones", seas_adj="NSA",
                      ts_type="Stock", cumperiods=1)

        if save_loc is not None:
            ops._io(operation="save", data_loc=save_loc,
                    data=data, name=f"{name}_{meta}", index_label=index_label)

        output.update({meta: data})

    return output
