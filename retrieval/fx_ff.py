import datetime as dt
import os
import urllib

import pandas as pd

from config import ROOT_DIR
from processing import colnames

URL = "https://www.bcu.gub.uy/Politica-Economica-y-Mercados/Mercado%20de%20Cambios/informepublico"
DATES = pd.bdate_range("2013-11-01", dt.datetime.today()).strftime("%y%m%d").tolist()

DATA_PATH = os.path.join(ROOT_DIR, "data")


def get(dates, update=None, save=None):

    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        prev_data = pd.read_csv(update_path, sep=" ", index_col=0, header=[0, 1, 2, 3, 4, 5, 6, 7, 8])
        prev_data.columns = ["Future", "Forward"]
        prev_data.index = pd.to_datetime(prev_data.index)
        last_date = prev_data.index[len(prev_data)-1]
        dates = pd.bdate_range(last_date, dt.datetime.today()).strftime("%y%m%d").tolist()

    reports = []
    for date in dates:

        try:
            raw_report = pd.read_excel(f"{URL}{date}.xls")

            if dt.datetime.strptime(date, "%y%m%d") >= dt.datetime(2014, 5, 21):
                future1 = raw_report.iloc[19, ].apply(pd.to_numeric, errors="coerce").sum()
                future2 = raw_report.iloc[21, ].apply(pd.to_numeric, errors="coerce").sum()
                future = future1 + future2
                forward = raw_report.iloc[24, ].apply(pd.to_numeric, errors="coerce").sum()

            else:
                future = raw_report.iloc[19, ].apply(pd.to_numeric, errors="coerce").sum()
                forward = raw_report.iloc[22, ].apply(pd.to_numeric, errors="coerce").sum()

            reports.append([dt.datetime.strptime(date, "%y%m%d"), future, forward])

        except urllib.error.HTTPError:
            print(f"Report for {date} could not be reached.")
            pass

    operations = pd.DataFrame(reports)
    operations.columns = ["Date", "Future", "Forward"]
    operations.set_index("Date", inplace=True)
    operations = operations.divide(1000)

    if update is not None:
        operations = prev_data.append(operations, sort=False)

    colnames.set_colnames(operations, area="International reserves", currency="USD", inf_adj="No",
                          index="No", seas_adj="NSA", ts_type="Flow", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        operations.to_csv(save_path, sep=" ")

    return operations


if __name__ == "__main__":
    fx_ops = get(dates=DATES, update="fx_ff.csv", save="fx_ff.csv")
