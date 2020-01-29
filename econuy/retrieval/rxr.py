import os
import datetime as dt
from pathlib import Path
from typing import Union

import pandas as pd
import numpy as np
import requests
from pandas.tseries.offsets import MonthEnd

from econuy.retrieval import cpi, nxr
from econuy.config import ROOT_DIR
from econuy.processing import index, updates, columns
from econuy.resources.utils import reer_url, ar_cpi_url, ar_cpi_payload

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 25


def official(update: Union[str, Path, None] = None, revise_rows: int = 0,
             save: Union[str, Path, None] = None, force_update: bool = False):
    """Get official real exchange rates from the BCU website.

    Parameters
    ----------
    update : str, Path or None (default is None)
        Path or path-like string pointing to a CSV file for updating.
    revise_rows : int (default is 0)
        How many rows of old data to replace with new data.
    save : str, Path or None (default is None)
        Path or path-like string where to save the output dataframe in CSV
        format.
    force_update : bool (default is False)
        If True, fetch data and update existing data even if it was modified
        within its update window (for CPI, 25 days)

    Returns
    -------
    proc : Pandas dataframe

    """
    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        delta, previous_data = updates.check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update} was modified within {update_threshold} day(s). "
                  f"Skipping download...")
            return previous_data

    raw = pd.read_excel(reer_url, skiprows=8, usecols="B:H", index_col=0)
    proc = raw.dropna(how="any")
    proc.columns = ["Global", "Regional", "Extrarregional",
                    "Argentina", "Brasil", "EEUU"]
    proc.index = pd.to_datetime(proc.index) + MonthEnd(1)

    if update is not None:
        proc = updates.revise(new_data=proc, prev_data=previous_data,
                              revise_rows=revise_rows)

    columns.set_metadata(proc, area="Precios y salarios", currency="-",
                         inf_adj="No", index="2017", seas_adj="NSA",
                         ts_type="-", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        proc.to_csv(save_path, sep=" ")

    return proc


def custom(update: Union[str, Path, None] = None, revise_rows: int = 0,
           save: Union[str, Path, None] = None, force_update: bool = False):
    """Calculate custom real exchange rates from various sources.

    Parameters
    ----------
    update : str, Path or None (default is None)
        Path or path-like string pointing to a CSV file for updating.
    revise_rows : int (default is 0)
        How many rows of old data to replace with new data.
    save : str, Path or None (default is None)
        Path or path-like string where to save the output dataframe in CSV
        format.
    force_update : bool (default is False)
        If True, fetch data and update existing data even if it was modified
        within its update window (for CPI, 25 days)

    Returns
    -------
    output : Pandas dataframe

    """
    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        delta, previous_data = updates.check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update} was modified within {update_threshold} day(s). "
                  f"Skipping download...")
            return previous_data

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

    ar_black_xr, ar_cpi = missing_ar()
    proc = raw.copy()
    proc["AR.PCPI_IX"] = ar_cpi
    ar_black_xr = pd.concat([ar_black_xr, proc["AR.ENDA_XDC_USD_RATE"]], axis=1)
    ar_black_xr[0] = np.where(pd.isna(ar_black_xr[0]),
                              ar_black_xr["AR.ENDA_XDC_USD_RATE"],
                              ar_black_xr[0])
    proc["AR.ENDA_XDC_USD_RATE_black"] = ar_black_xr.iloc[:, 0]
    proc["AR_E_A"] = proc.iloc[:, [5, 6]].mean(axis=1)

    uy_cpi = cpi.get(update="cpi.csv", revise_rows=6,
                     save="cpi.csv", force_update=False)
    uy_e = nxr.get(update="nxr.csv", revise_rows=6,
                   save="nxr.csv", force_update=False).iloc[:, [3]]
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

    columns.set_metadata(output, area="Precios y salarios", currency="-",
                         inf_adj="-", index="-", seas_adj="NSA",
                         ts_type="Flujo", cumperiods=1)
    output = index.base_index(output, start_date="2010-01-01",
                              end_date="2010-12-31", base=100)

    if update is not None:
        output = updates.revise(new_data=proc, prev_data=previous_data,
                                revise_rows=revise_rows)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        output.to_csv(save_path, sep=" ")

    return output


def missing_ar():
    """Get Argentina's non-official exchange rate and CPI."""
    today_fmt = dt.datetime.now().strftime("%d-%m-%Y")
    black_url = (f"https://mercados.ambito.com/dolar/informal/"
                 f"historico-general/11-01-2002/{today_fmt}")
    black_r = requests.get(black_url).json()
    black_xr = pd.DataFrame(black_r)
    black_xr.set_index(0, drop=True, inplace=True)
    black_xr.drop("Fecha", inplace=True)
    black_xr = black_xr.replace(",", ".", regex=True).apply(pd.to_numeric)
    black_xr.index = pd.to_datetime(black_xr.index, format="%d-%m-%Y")
    black_xr = black_xr.mean(axis=1).to_frame().sort_index()
    black_xr = black_xr.resample("M").mean()

    cpi_r = requests.get(ar_cpi_url, params=ar_cpi_payload)
    cpi_ar = pd.read_html(cpi_r.content)[0]
    cpi_ar.set_index("Fecha", drop=True, inplace=True)
    cpi_ar.index = pd.to_datetime(cpi_ar.index, format="%d/%m/%Y")
    cpi_ar.columns = ["nivel"]
    cpi_ar = cpi_ar.divide(10)

    ps_url = "http://www.inflacionverdadera.com/Argentina_inflation.xls"
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
