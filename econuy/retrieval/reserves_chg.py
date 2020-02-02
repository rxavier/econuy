import datetime as dt
import urllib
from os import PathLike
from typing import List, Union

import pandas as pd
from dateutil.relativedelta import relativedelta

from econuy.resources import columns, updates
from econuy.resources.lstrings import (reserves_cols, reserves_url,
                                       missing_reserves_url)

months = ["ene", "feb", "mar", "abr", "may", "jun",
          "jul", "ago", "set", "oct", "nov", "dic"]
years = list(range(2013, dt.datetime.now().year + 1))
files_ = [month + str(year) for year in years for month in months]


def get(files: List[str] = files_, update: Union[str, PathLike, bool] = False,
        save: Union[str, PathLike, bool] = False):
    """Get international reserves change data from online sources.

    Use as input a list of strings of the format %b%Y, each representing a
    month of data.

    Parameters
    ----------
    files : list of strings
        List of strings of the type '%b%Y', only %b is in Spanish. So 'ene'
        instead of 'jan'. For example, 'oct2017'.
    update : str, PathLike or bool (default is False)
        Path, path-like string pointing to a CSV file for updating, or bool,
        in which case if True, save in predefined file, or False, don't update.
    save : str, PathLike or bool (default is False)
        Path, path-like string pointing to a CSV file for saving, or bool,
        in which case if True, save in predefined file, or False, don't save.

    Returns
    -------
    reserves : Pandas dataframe

    """
    urls = [f"{reserves_url}{file}.xls" for file in files]
    wrong_may14 = f"{reserves_url}may2014.xls"
    fixed_may14 = f"{reserves_url}mayo2014.xls"
    urls = [fixed_may14 if x == wrong_may14 else x for x in urls]

    if update is not False:
        update_path = updates._paths(update, multiple=False,
                                     name="reserves_chg.csv")
        previous_data = pd.read_csv(update_path, index_col=0,
                                    header=list(range(9)))
        previous_data.columns = reserves_cols[1:46]
        previous_data.index = pd.to_datetime(previous_data.index)
        urls = urls[-18:]

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

    mar14 = pd.read_excel(missing_reserves_url, index_col=0)
    mar14.columns = reserves_cols[1:46]
    reserves = pd.concat(reports + [mar14], sort=False).sort_index()

    if update is not False:
        reserves = previous_data.append(reserves, sort=False)
        reserves = reserves.loc[~reserves.index.duplicated(keep="last")]

    reserves = reserves.apply(pd.to_numeric, errors="coerce")
    columns._setmeta(reserves, area="Reservas internacionales",
                     currency="USD", inf_adj="No", index="No",
                     seas_adj="NSA", ts_type="Flujo", cumperiods=1)

    if save is not False:
        save_path = updates._paths(save, multiple=False,
                                   name="reserves_chg.csv")
        reserves.to_csv(save_path)

    return reserves
