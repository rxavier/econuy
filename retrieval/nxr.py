import pandas as pd
import os

from config import ROOT_DIR
from processing import colnames, update_revise

DATA_PATH = os.path.join(ROOT_DIR, "data")


def get(update=None, revise=0, save=None):

    file = "http://ine.gub.uy/c/document_library/get_file?uuid=3fbf4ffd-a829-420c-aca9-9f01ecd7919a&groupId=10181"

    nxr_raw = pd.read_excel(file, skiprows=4)
    nxr = nxr_raw.dropna(axis=0, thresh=4).set_index("Mes y año").dropna(axis=1, how="all").rename_axis(None)
    nxr.columns = ["Buy, average", "Sell, average", "Buy, EOP", "Sell, EOP"]

    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        nxr = update_revise.upd_rev(nxr, prev_data=update_path, revise=revise)

    nxr = nxr.apply(pd.to_numeric, errors="coerce")
    colnames.set_colnames(nxr, area="Prices and wages", currency="-", inf_adj="No",
                          index="No", seas_adj="NSA", ts_type="-", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        nxr.to_csv(save_path, sep=" ")

    return nxr


if __name__ == "__main__":
    exchange_rate = get(update="nxr.csv", revise=6, save="nxr.csv")
