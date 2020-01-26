import os
from pathlib import Path
from typing import Union

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from config import ROOT_DIR
from processing import columns, updates

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 25


def get(update: Union[str, Path, None] = None, revise_rows: int = 0,
        save: Union[str, Path, None] = None, force_update: bool = False):
    """Convert dataframe from UYU to USD.

    Convert a dataframe's columns from Uruguayan pesos to US dollars. Call the
    `nxr.get()` function to obtain nominal exchange rates, and take into
    account whether the input dataframe's `Type`, as defined by its multiindex,
    is flow or stock, in order to choose end of period or monthly average NXR.
    Also take into account the input dataframe's frequency and whether columns
    represent rolling averages or sums.

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
        within its update window (for CPI, 25 days)

    Returns
    -------
    dataframe : Pandas dataframe

    """
    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        delta, previous_data = updates.check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update} was modified within {update_threshold} day(s). "
                  f"Skipping download...")
            return previous_data

    file = "http://ine.gub.uy/c/document_library/get_file?uuid=2e92084a-94ec-4fec-b5ca-42b40d5d2826&groupId=10181"

    cpi_raw = pd.read_excel(file, skiprows=7).dropna(axis=0, thresh=2)
    cpi = (cpi_raw.drop(["Mensual", "Acum.año", "Acum.12 meses"], axis=1).
           dropna(axis=0, how="all").set_index("Mes y año").rename_axis(None))
    cpi.columns = ["Índice de precios al consumo"]
    cpi.index = cpi.index + MonthEnd(1)

    if update is not None:
        cpi = updates.revise(new_data=cpi, prev_data=previous_data,
                             revise_rows=revise_rows)

    cpi = cpi.apply(pd.to_numeric, errors="coerce")
    columns.set_metadata(cpi, area="Precios y salarios", currency="-",
                         inf_adj="No", index="2010-10-31", seas_adj="NSA",
                         ts_type="-", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        cpi.to_csv(save_path, sep=" ")

    return cpi
