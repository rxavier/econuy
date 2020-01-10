import os
import datetime as dt

import pandas as pd

from config import ROOT_DIR
from processing import colnames, update_revise

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 25


def get(update=None, revise=0, save=None, force_update=False):

    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        delta, previous_data = update_revise.check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"File in update path was modified within {update_threshold} day(s). Skipping download...")
            return previous_data

    file = "http://ine.gub.uy/c/document_library/get_file?uuid=50ae926c-1ddc-4409-afc6-1fecf641e3d0&groupId=10181"

    labor_raw = pd.read_excel(file, skiprows=39).dropna(axis=0, thresh=2)
    labor = labor_raw[~labor_raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    labor = labor[["Unnamed: 1", "Unnamed: 4", "Unnamed: 7"]]
    labor.index = pd.date_range(start="2006-01-01", periods=len(labor), freq="M")
    labor.columns = ["Tasa de actividad", "Tasa de empleo", "Tasa de desempleo"]

    if update is not None:
        labor = update_revise.upd_rev(new_data=labor, prev_data=previous_data, revise=revise)

    labor = labor.apply(pd.to_numeric, errors="coerce")
    colnames.set_colnames(labor, area="Mercado laboral", currency="-", inf_adj="No",
                          index="No", seas_adj="NSA", ts_type="-", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        labor.to_csv(save_path, sep=" ")

    return labor


if __name__ == "__main__":
    labor_mkt = get(update="labor.csv", revise=6, save="labor.csv")
