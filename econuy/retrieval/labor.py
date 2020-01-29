import os
from pathlib import Path
from typing import Union

import pandas as pd

from econuy.config import ROOT_DIR
from econuy.processing import updates, columns
from econuy.resources.utils import labor_url

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 25


def get(update: Union[str, Path, None] = None, revise_rows: int = 0,
        save: Union[str, Path, None] = None, force_update: bool = False):
    """Get labor market data.

    Get monthly labor force participation rate, employment rate (employment to
    working-age population) and unemployment rate.

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
        within its update window (for labor market, 25 days)

    Returns
    -------
    labor : Pandas dataframe

    """
    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        delta, previous_data = updates.check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update} was modified within {update_threshold} day(s). "
                  f"Skipping download...")
            return previous_data

    labor_raw = pd.read_excel(labor_url, skiprows=39).dropna(axis=0, thresh=2)
    labor = labor_raw[~labor_raw["Unnamed: 0"].str.contains("-|/|Total",
                                                            regex=True)]
    labor = labor[["Unnamed: 1", "Unnamed: 4", "Unnamed: 7"]]
    labor.index = pd.date_range(start="2006-01-01",
                                periods=len(labor), freq="M")
    labor.columns = ["Tasa de actividad", "Tasa de empleo",
                     "Tasa de desempleo"]

    if update is not None:
        labor = updates.revise(new_data=labor, prev_data=previous_data,
                               revise_rows=revise_rows)

    labor = labor.apply(pd.to_numeric, errors="coerce")
    columns.set_metadata(labor, area="Mercado laboral", currency="-",
                         inf_adj="No", index="No", seas_adj="NSA",
                         ts_type="-", cumperiods=1)

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        labor.to_csv(save_path, sep=" ")

    return labor
