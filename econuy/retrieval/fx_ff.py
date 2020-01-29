import datetime as dt
import os
import urllib
from pathlib import Path
from typing import Union

import pandas as pd

from econuy.config import ROOT_DIR
from econuy.processing import columns
from econuy.resources.utils import ff_url

DATA_PATH = os.path.join(ROOT_DIR, "data")


def get(update: Union[str, Path, None] = None,
        save: Union[str, Path, None] = None):
    """Get future and forwards FX operations by the Central Bank.

    Parameters
    ----------
    update : str, Path or None (default is None)
        Path or path-like string pointing to a CSV file for updating.
    save : str, Path or None (default is None)
        Path or path-like string where to save the output dataframe in CSV
        format.

    Returns
    -------
    operations : Pandas dataframe
        Daily future and forward FX operations since november 2013.

    """
    dates = pd.bdate_range("2013-11-01",
                           dt.datetime.today()).strftime("%y%m%d").tolist()
    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        prev_data = pd.read_csv(update_path, sep=" ", index_col=0,
                                header=[0, 1, 2, 3, 4, 5, 6, 7, 8])
        prev_data.columns = ["Futuros", "Forwards"]
        prev_data.index = pd.to_datetime(prev_data.index)
        last_date = prev_data.index[len(prev_data)-1]
        dates = pd.bdate_range(last_date,
                               dt.datetime.today()).strftime("%y%m%d").tolist()

    reports = []
    for date in dates:

        try:
            raw_report = pd.read_excel(f"{ff_url}{date}.xls")

            if (dt.datetime.strptime(date, "%y%m%d") >=
                    dt.datetime(2014, 5, 21)):
                future1 = raw_report.iloc[19, ].apply(pd.to_numeric,
                                                      errors="coerce").sum()
                future2 = raw_report.iloc[21, ].apply(pd.to_numeric,
                                                      errors="coerce").sum()
                future = future1 + future2
                forward = raw_report.iloc[24, ].apply(pd.to_numeric,
                                                      errors="coerce").sum()

            else:
                future = raw_report.iloc[19, ].apply(pd.to_numeric,
                                                     errors="coerce").sum()
                forward = raw_report.iloc[22, ].apply(pd.to_numeric,
                                                      errors="coerce").sum()

            reports.append([dt.datetime.strptime(date, "%y%m%d"),
                            future, forward])

        except urllib.error.HTTPError:
            print(f"Report for {date} could not be reached.")
            pass

    operations = pd.DataFrame(reports)
    operations.columns = ["Date", "Futuros", "Forwards"]
    operations.set_index("Date", inplace=True)
    operations = operations.divide(1000)

    if update is not None:
        operations = prev_data.append(operations, sort=False)

    columns.set_metadata(
        operations, area="Reservas internacionales",  currency="USD",
        inf_adj="No", index="No", seas_adj="NSA", ts_type="Flujo",
        cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        operations.to_csv(save_path, sep=" ")

    return operations
