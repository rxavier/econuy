import os

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from config import ROOT_DIR
from processing import colnames, updates

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 25


def get(update=None, revise=0, save=None, force_update=False):

    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        delta, previous_data = updates.check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"File in update path was modified within {update_threshold} day(s). Skipping download...")
            return previous_data

    file = "http://ine.gub.uy/c/document_library/get_file?uuid=2e92084a-94ec-4fec-b5ca-42b40d5d2826&groupId=10181"

    cpi_raw = pd.read_excel(file, skiprows=7).dropna(axis=0, thresh=2)
    cpi = (cpi_raw.drop(["Mensual", "Acum.año", "Acum.12 meses"], axis=1).
           dropna(axis=0, how="all").set_index("Mes y año").rename_axis(None))
    cpi.columns = ["Índice de precios al consumo"]
    cpi.index = cpi.index + MonthEnd(1)

    if update is not None:
        cpi = updates.revise(new_data=cpi, prev_data=previous_data, revise=revise)

    cpi = cpi.apply(pd.to_numeric, errors="coerce")
    colnames.set_colnames(cpi, area="Precios y salarios", currency="-", inf_adj="No",
                          index="2010-10-31", seas_adj="NSA", ts_type="-", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        cpi.to_csv(save_path, sep=" ")

    return cpi


if __name__ == "__main__":
    prices = get(update="cpi.csv", revise=6, save="cpi.csv")
