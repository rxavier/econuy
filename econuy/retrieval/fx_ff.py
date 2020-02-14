import datetime as dt
import urllib
from os import PathLike
from pathlib import Path
from typing import Union, Optional

import pandas as pd

from econuy.resources import columns
from econuy.resources.lstrings import ff_url


def get(update: Union[str, PathLike, None] = None,
        save: Union[str, PathLike, None] = None,
        name: Optional[str] = None) -> pd.DataFrame:
    """Get future and forwards FX operations by the Central Bank.

    Parameters
    ----------
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Daily future and forwards foreign exchange operations : pd.DataFrame

    """
    if name is None:
        name = "fx_ff"
    dates = pd.bdate_range("2013-11-01",
                           dt.datetime.today()).strftime("%y%m%d").tolist()
    if update is not None:
        update_path = (Path(update) / name).with_suffix(".csv")
        try:
            prev_data = pd.read_csv(update_path, index_col=0,
                                    header=list(range(9)))
            prev_data.columns = ["Futuros", "Forwards"]
            prev_data.index = pd.to_datetime(prev_data.index)
            last_date = prev_data.index[len(prev_data)-1]
            dates = pd.bdate_range(
                last_date, dt.datetime.today()
            ).strftime("%y%m%d").tolist()
        except FileNotFoundError:
            prev_data = pd.DataFrame()
            pass

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

    columns._setmeta(
        operations, area="Reservas internacionales",  currency="USD",
        inf_adj="No", index="No", seas_adj="NSA", ts_type="Flujo",
        cumperiods=1
    )

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        operations.to_csv(save_path)

    return operations
