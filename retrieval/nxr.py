import os

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from config import ROOT_DIR
from processing import colnames, updates

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 25


def get(update=None, revise_rows=0, save=None, force_update=False):

    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        delta, previous_data = updates.check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"File in update path was modified within {update_threshold} day(s). Skipping download...")
            return previous_data

    file = "http://ine.gub.uy/c/document_library/get_file?uuid=3fbf4ffd-a829-420c-aca9-9f01ecd7919a&groupId=10181"

    nxr_raw = pd.read_excel(file, skiprows=4)
    nxr = nxr_raw.dropna(axis=0, thresh=4).set_index("Mes y año").dropna(axis=1, how="all").rename_axis(None)
    nxr.columns = ["Tipo de cambio compra, promedio", "Tipo de cambio venta, promedio",
                   "Tipo de cambio compra, fin período", "Tipo de cambio venta, fin período"]
    nxr.index = nxr.index + MonthEnd(1)

    if update is not None:
        nxr = updates.revise(new_data=nxr, prev_data=previous_data, revise_rows=revise_rows)

    nxr = nxr.apply(pd.to_numeric, errors="coerce")
    colnames.set_colnames(nxr, area="Precios y salarios", currency="-", inf_adj="No",
                          index="No", seas_adj="NSA", ts_type="-", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        nxr.to_csv(save_path, sep=" ")

    return nxr


if __name__ == "__main__":
    exchange_rate = get(update="nxr.csv", revise_rows=6, save="nxr.csv")
