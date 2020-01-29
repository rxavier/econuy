import datetime as dt
import os
import urllib
from pathlib import Path
from typing import List, Union

import pandas as pd
from dateutil.relativedelta import relativedelta

from econuy.config import ROOT_DIR
from econuy.processing import columns
from econuy.resources.utils import reserves_cols, reserves_url

months = ["ene", "feb", "mar", "abr", "may", "jun",
          "jul", "ago", "set", "oct", "nov", "dic"]
years = list(range(2013, dt.datetime.now().year + 1))
files_ = [month + str(year) for year in years for month in months]

DATA_PATH = os.path.join(ROOT_DIR, "data")


def base_reports(files: List[str], update: Union[str, Path, None] = None):
    """Get international reserves change data from online sources.

    Use as input a list of strings of the format %b%Y, each representing a
    month of data.

    Parameters
    ----------
    files : list of strings
        List of strings of the type '%b%Y', only %b is in Spanish. So 'ene'
        instead of 'jan'. For example, 'oct2017'.
    update : str, Path or None (default is None)
        Path or path-like string pointing to a CSV file for updating.

    Returns
    -------
    reserves : Pandas dataframe

    """
    urls = [f"{reserves_url}{file}.xls" for file in files]

    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        urls = urls[-18:]
        previous_data = pd.read_csv(update_path, sep=" ", index_col=0,
                                    header=list(range(9)))
        previous_data.columns = reserves_cols[1:46]
        previous_data.index = pd.to_datetime(previous_data.index)

    reports = []
    for url in urls:

        try:
            with pd.ExcelFile(url) as xls:
                month_of_report = pd.read_excel(xls, sheet_name="INDICE")
                raw = pd.read_excel(xls, sheet_name="ACTIVOS DE RESERVA",
                                    skiprows=3)

            first_day = month_of_report.iloc[7, 4]
            last_day = (first_day
                        + relativedelta(months=1)
                        - dt.timedelta(days=1))

            proc = raw.dropna(axis=0, thresh=20).dropna(axis=1, thresh=20)
            proc = proc.transpose()
            proc.index.name = "Date"
            proc = proc.iloc[:, 1:46]
            proc.columns = reserves_cols[1:46]
            proc = proc.iloc[1:]
            proc.index = pd.to_datetime(proc.index, errors="coerce")
            proc = proc.loc[proc.index.dropna()]
            proc = proc.loc[first_day:last_day]

            reports.append(proc)

        except urllib.error.HTTPError:
            print(f"{url} could not be reached.")
            pass

    reserves = pd.concat(reports, sort=False)

    if update is not None:
        reserves = previous_data.append(reserves, sort=False)
        reserves = reserves.loc[~reserves.index.duplicated(keep="last")]

    reserves = reserves.apply(pd.to_numeric, errors="coerce")

    return reserves


def missing_reports(online_files: List[str], offline_files: List[str]):
    """Get missing reserves data from online and offline sources.

    Parameters
    ----------
    online_files : list of strings
        Used for reserves files that do not match the '%b%Y' format. See
        `base_reports`.
    offline_files : list of strings
        List of strings pointing to CSV locations.

    Returns
    -------
    missing : Pandas dataframe

    """
    missing_online = base_reports(online_files)

    missing_offline = []
    for file in offline_files:
        offline_aux = pd.read_csv(file, sep=" ", index_col=0)
        offline_aux.index = pd.to_datetime(offline_aux.index, errors="coerce")

        missing_offline.append(offline_aux)

    missing_offline = pd.concat(missing_offline, sort=False)

    missing = missing_online.append(missing_offline, sort=False)
    missing = missing.apply(pd.to_numeric, errors="coerce")

    return missing


def get_reserves_chg(files: List[str], online_files: List[str] = None,
                     offline_files: List[str] = None,
                     update: Union[str, Path, None] = None,
                     save: Union[str, Path, None] = None):
    """Get international reserves changes data from online and offline sources.

    Call the `base_reports()` and `missing_reports()` functions.

    Parameters
    ----------
    files : list of strings
    online_files : list of strings (default is None)
        Used for reserves files that do not match the '%b%Y' format. See
        `base_reports`.
    offline_files : list of strings (default is None)
        List of strings pointing to CSV locations.
    update : str, Path or None (default is None)
        Path or path-like string pointing to a CSV file for updating.
    save : str, Path or None (default is None)
        Path or path-like string where to save the output dataframe in CSV
        format.

    Returns
    -------
    reserves : Pandas dataframe

    """
    reserves = base_reports(files=files, update=update)

    if online_files is not None or offline_files is not None:
        missing = missing_reports(online_files=online_files,
                                  offline_files=offline_files)
        reserves = reserves.append(missing, sort=False)
        reserves.sort_index(inplace=True)

    columns.set_metadata(reserves, area="Reservas internacionales",
                         currency="USD", inf_adj="No", index="No",
                         seas_adj="NSA", ts_type="Flujo", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        reserves.to_csv(save_path, sep=" ")

    return reserves
