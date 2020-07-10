import datetime as dt
import re
from os import PathLike
from typing import Union
from urllib import error
from json.decoder import JSONDecodeError

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from requests import exceptions
from sqlalchemy.engine.base import Connection, Engine

from econuy import transform
from econuy.retrieval import cpi, nxr
from econuy.utils import ops, metadata
from econuy.utils.lstrings import urls


@retry(
    retry_on_exceptions=(error.HTTPError, error.URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get_official(update_loc: Union[str, PathLike, Engine,
                                   Connection, None] = None,
                 revise_rows: Union[str, int] = "nodup",
                 save_loc: Union[str, PathLike, Engine,
                                 Connection, None] = None,
                 name: str = "rxr_official",
                 index_label: str = "index",
                 only_get: bool = False) -> pd.DataFrame:
    """Get official real exchange rates from the BCU website.

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
    name : str, default 'rxr_official'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly real exchange rates vs select countries/regions : pd.DataFrame
        Available: global, regional, extraregional, Argentina, Brazil, US.

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    r = requests.get(urls["rxr_official"]["dl"]["main"])
    soup = BeautifulSoup(r.content, "html.parser")
    links = soup.find_all(href=re.compile("eese[A-z0-9]+\\.xls$"))
    xls = "https://www.bcu.gub.uy" + links[0]["href"]
    raw = pd.read_excel(xls, skiprows=8, usecols="B:N", index_col=0)
    proc = raw.dropna(how="any")
    proc.columns = ["Global", "Extrarregional", "Regional",
                    "Argentina", "Brasil", "EE.UU.", "México", "Alemania",
                    "España", "Reino Unido", "Italia", "China"]
    proc.index = pd.to_datetime(proc.index) + MonthEnd(1)

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        proc = ops._revise(new_data=proc, prev_data=previous_data,
                           revise_rows=revise_rows)

    metadata._set(proc, area="Precios y salarios", currency="UYU/Otro",
                  inf_adj="No", unit="2017=100", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=proc, name=name, index_label=index_label)

    return proc


@retry(
    retry_on_exceptions=(error.HTTPError, error.URLError, JSONDecodeError),
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=90,
)
def get_custom(update_loc: Union[str, PathLike, Engine,
                                 Connection, None] = None,
               revise_rows: Union[str, int] = "nodup",
               save_loc: Union[str, PathLike, Engine,
                               Connection, None] = None,
               name: str = "rxr_custom",
               index_label: str = "index",
               only_get: bool = False) -> pd.DataFrame:
    """Get official real exchange rates from the BCU website.

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
    name : str, default 'rxr_custom'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly real exchange rates vs select countries : pd.DataFrame
        Available: Argentina, Brazil, US.

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    url_ = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M."
    url_extra = ".?startPeriod=1970&endPeriod="
    raw = []
    for country in ["US", "BR", "AR"]:
        for indicator in ["PCPI_IX", "ENDA_XDC_USD_RATE"]:
            base_url = (f"{url_}{country}.{indicator}{url_extra}"
                        f"{dt.datetime.now().year}")
            r_json = requests.get(base_url).json()
            data = r_json["CompactData"]["DataSet"]["Series"]["Obs"]
            try:
                data = pd.DataFrame(data)
                data.set_index("@TIME_PERIOD", drop=True, inplace=True)
            except ValueError:
                data = pd.DataFrame(np.nan,
                                    index=pd.date_range(start="1970-01-01",
                                                        end=dt.datetime.now(),
                                                        freq="M"),
                                    columns=[f"{country}.{indicator}"])
            if "@OBS_STATUS" in data.columns:
                data.drop("@OBS_STATUS", inplace=True, axis=1)
            data.index = (pd.to_datetime(data.index, format="%Y-%m")
                          + MonthEnd(1))
            data.columns = [f"{country}.{indicator}"]
            raw.append(data)
    raw = pd.concat(raw, axis=1, sort=True).apply(pd.to_numeric)

    ar_black_xr, ar_cpi = _missing_ar()
    proc = raw.copy()
    proc["AR.PCPI_IX"] = ar_cpi
    ar_black_xr = pd.concat([ar_black_xr, proc["AR.ENDA_XDC_USD_RATE"]],
                            axis=1)
    ar_black_xr[0] = np.where(pd.isna(ar_black_xr[0]),
                              ar_black_xr["AR.ENDA_XDC_USD_RATE"],
                              ar_black_xr[0])
    proc["AR.ENDA_XDC_USD_RATE_black"] = ar_black_xr.iloc[:, 0]
    proc["AR_E_A"] = proc.iloc[:, [5, 6]].mean(axis=1)

    uy_cpi = cpi.get(update_loc=update_loc, save_loc=save_loc,
                     only_get=True)
    uy_e = nxr.get_monthly(update_loc=update_loc, save_loc=save_loc,
                           only_get=True).iloc[:, [1]]
    proc = pd.concat([proc, uy_cpi, uy_e], axis=1)
    proc = proc.interpolate(method="linear", limit_area="inside")
    proc = proc.dropna(how="any")
    proc.columns = ["US_P", "US_E", "BR_P", "BR_E", "AR_P", "AR_E",
                    "AR_E_B", "AR_E_A", "UY_P", "UY_E"]

    output = pd.DataFrame()
    output["UY_E_P"] = proc["UY_E"] / proc["UY_P"]
    output["TCR_UY_AR"] = output["UY_E_P"] / proc["AR_E_A"] * proc["AR_P"]
    output["TCR_UY_BR"] = output["UY_E_P"] / proc["BR_E"] * proc["BR_P"]
    output["TCR_UY_US"] = output["UY_E_P"] * proc["US_P"]
    output["TCR_AR_US"] = proc["BR_E"] * proc["US_P"] / proc["BR_P"]
    output["TCR_BR_US"] = proc["AR_E"] * proc["US_P"] / proc["AR_P"]
    output.drop("UY_E_P", axis=1, inplace=True)
    output.rename_axis(None, inplace=True)

    metadata._set(output, area="Precios y salarios", currency="-",
                  inf_adj="No", unit="-", seas_adj="NSA",
                  ts_type="-", cumperiods=1)
    output = transform.base_index(output, start_date="2010-01-01",
                                  end_date="2010-12-31", base=100)
    arrays = []
    for level in range(0, 9):
        arrays.append(list(output.columns.get_level_values(level)))
    arrays[3] = ["UYU/ARS", "UYU/BRL", "UYU/USD", "ARS/USD", "BRL/USD"]
    tuples = list(zip(*arrays))
    output.columns = pd.MultiIndex.from_tuples(tuples,
                                               names=["Indicador", "Área",
                                                      "Frecuencia", "Moneda",
                                                      "Inf. adj.", "Unidad",
                                                      "Seas. Adj.", "Tipo",
                                                      "Acum. períodos"])

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(exceptions.HTTPError, exceptions.ConnectionError,
                         error.HTTPError, error.URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def _missing_ar():
    """Get Argentina's non-official exchange rate and CPI."""
    black_r = requests.get(urls["rxr_custom"]["dl"]["ar_black"]).json()
    black_xr = pd.DataFrame(black_r)
    black_xr.set_index(0, drop=True, inplace=True)
    black_xr.drop("Fecha", inplace=True)
    black_xr = black_xr.replace(",", ".", regex=True).apply(pd.to_numeric)
    black_xr.index = pd.to_datetime(black_xr.index, format="%d-%m-%Y")
    black_xr = black_xr.mean(axis=1).to_frame().sort_index()
    black_xr = black_xr.resample("M").mean()

    cpi_r = requests.get(urls["rxr_custom"]["dl"]["ar_cpi"],
                         params=urls["rxr_custom"]["dl"]["ar_cpi_payload"])
    cpi_ar = pd.read_html(cpi_r.content)[0]
    cpi_ar.set_index("Fecha", drop=True, inplace=True)
    cpi_ar.index = pd.to_datetime(cpi_ar.index, format="%d/%m/%Y")
    cpi_ar.columns = ["nivel"]
    cpi_ar = cpi_ar.divide(10)

    ps_url = urls["rxr_custom"]["dl"]["inf_black"]
    cpi_ps = pd.read_excel(ps_url)
    cpi_ps.set_index("date", drop=True, inplace=True)
    cpi_ps.index = cpi_ps.index + MonthEnd(1)
    cpi_ps = cpi_ps.loc[(cpi_ps.index >= "2006-12-31") &
                        (cpi_ps.index <= "2016-12-01"), "index"]
    cpi_ps = cpi_ps.to_frame().pct_change(periods=1).multiply(100)
    cpi_ps.columns = ["nivel"]

    cpi_all = (cpi_ar.append(cpi_ps).reset_index().
               drop_duplicates(subset="index", keep="last").
               set_index("index", drop=True).sort_index())

    cpi_all = cpi_all.divide(100).add(1).cumprod()

    return black_xr, cpi_all
