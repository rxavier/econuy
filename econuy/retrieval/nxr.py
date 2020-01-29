import os
from pathlib import Path
from typing import Union

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from econuy.config import ROOT_DIR
from econuy.processing import updates, columns
from econuy.resources.utils import nxr_url

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 25


def get(update: Union[str, Path, None] = None, revise_rows: int = 0,
        save: Union[str, Path, None] = None, force_update: bool = False):
    """Get nominal exchange rate data.

    Parameters
    ----------
    update : str, Path or None (default is None)
        Path or path-like string pointing to a CSV file for updating.
    revise_rows : int (default is 0)
        How many rows of old data to replace with new data.
    save : str, Path or None (default is None)
        Path or path-like string where to save the output dataframe in CSV
        format.
    force_update : bool (default is False)
        If True, fetch data and update existing data even if it was modified
        within its update window (for NXR, 25 days)

    Returns
    -------
    nxr : Pandas dataframe

    """
    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        delta, previous_data = updates.check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update} was modified within {update_threshold} day(s). "
                  f"Skipping download...")
            return previous_data

    nxr_raw = pd.read_excel(nxr_url, skiprows=4)
    nxr = (nxr_raw.dropna(axis=0, thresh=4).set_index("Mes y año").
           dropna(axis=1, how="all").rename_axis(None))
    nxr.columns = ["Tipo de cambio compra, fin de período",
                   "Tipo de cambio venta, fin de período",
                   "Tipo de cambio compra, promedio",
                   "Tipo de cambio venta, promedio"]
    nxr.index = nxr.index + MonthEnd(1)

    if update is not None:
        nxr = updates.revise(new_data=nxr, prev_data=previous_data,
                             revise_rows=revise_rows)

    nxr = nxr.apply(pd.to_numeric, errors="coerce")
    columns.set_metadata(nxr, area="Precios y salarios", currency="-",
                         inf_adj="No", index="No", seas_adj="NSA",
                         ts_type="-", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        nxr.to_csv(save_path, sep=" ")

    return nxr
