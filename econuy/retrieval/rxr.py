import datetime as dt
from os import PathLike, path, mkdir
from pathlib import Path
from typing import Union, Optional

import numpy as np
import pandas as pd
import requests
from pandas.tseries.offsets import MonthEnd

from econuy import transform
from econuy.resources import updates, columns
from econuy.resources.lstrings import reer_url, ar_cpi_url, ar_cpi_payload
from econuy.retrieval import cpi, nxr


def get_official(update: Union[str, PathLike, None] = None,
                 revise_rows: Union[str, int] = "nodup",
                 save: Union[str, PathLike, None] = None,
                 force_update: bool = False,
                 name: Optional[str] = None) -> pd.DataFrame:
    """Get official real exchange rates from the BCU website.

    Parameters
    ----------
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    force_update : bool, default False
        If ``True``, fetch data and update existing data even if it was
        modified within its update window (for real exchange rates, 25 days).
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Monthly real exchange rates vs select countries/regions : pd.DataFrame
        Available: global, regional, extraregional, Argentina, Brazil, US.

    """
    update_threshold = 25
    if name is None:
        name = "rxr_official"

    if update is not None:
        update_path = (Path(update) / name).with_suffix(".csv")
        delta, previous_data = updates._check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update_path} was modified within {update_threshold} "
                  f"day(s). Skipping download...")
            return previous_data

    raw = pd.read_excel(reer_url, skiprows=8, usecols="B:H", index_col=0)
    proc = raw.dropna(how="any")
    proc.columns = ["Global", "Regional", "Extrarregional",
                    "Argentina", "Brasil", "EEUU"]
    proc.index = pd.to_datetime(proc.index) + MonthEnd(1)

    if update is not None:
        proc = updates._revise(new_data=proc, prev_data=previous_data,
                               revise_rows=revise_rows)

    columns._setmeta(proc, area="Precios y salarios", currency="-",
                     inf_adj="No", index="2017", seas_adj="NSA",
                     ts_type="-", cumperiods=1)

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        if not path.exists(path.dirname(save_path)):
            mkdir(path.dirname(save_path))
        proc.to_csv(save_path)

    return proc


def get_custom(update: Union[str, PathLike, None] = None,
               revise_rows: Union[str, int] = "nodup",
               save: Union[str, PathLike, None] = None,
               force_update: bool = False,
               name: Optional[str] = None) -> pd.DataFrame:
    """Get official real exchange rates from the BCU website.

    Parameters
    ----------
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    force_update : bool, default False
        If ``True``, fetch data and update existing data even if it was
        modified within its update window (for real exchange rates, 25 days).
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Monthly real exchange rates vs select countries : pd.DataFrame
        Available: Argentina, Brazil, US.

    """
    update_threshold = 25
    if name is None:
        name = "rxr_custom"

    if update is not None:
        update_path = (Path(update) / name).with_suffix(".csv")
        delta, previous_data = updates._check_modified(update_path)

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

    uy_cpi = cpi.get(update=update, revise_rows=6,
                     save=save, force_update=False)
    uy_e = nxr.get(update=update, revise_rows=6,
                   save=save, force_update=False).iloc[:, [3]]
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

    columns._setmeta(output, area="Precios y salarios", currency="-",
                     inf_adj="-", index="-", seas_adj="NSA",
                     ts_type="Flujo", cumperiods=1)
    output = transform.base_index(output, start_date="2010-01-01",
                                  end_date="2010-12-31", base=100)

    if update is not None:
        output = updates._revise(new_data=output, prev_data=previous_data,
                                 revise_rows=revise_rows)

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        if not path.exists(path.dirname(save_path)):
            mkdir(path.dirname(save_path))
        output.to_csv(save_path)

    return output


def _missing_ar():
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
