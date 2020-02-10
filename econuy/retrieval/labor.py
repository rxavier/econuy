from os import PathLike
from pathlib import Path
from typing import Union

import pandas as pd

from econuy.resources import updates, columns
from econuy.resources.lstrings import labor_url


def get(update: Union[str, PathLike, None] = None, 
        revise_rows: Union[str, int] = 0,
        save: Union[str, PathLike, None] = None, 
        force_update: bool = False, name: Union[str, None] = None):
    """Get labor market data.

    Get monthly labor force participation rate, employment rate (employment to
    working-age population) and unemployment rate.

    Parameters
    ----------
    update : str, PathLike or None, default is None
        Path or path-like string pointing to a directory where to find a CSV 
        for updating, or None, don't update.
    revise_rows : str or int, default is 0
        How many rows of old data to replace with new data.
    save : str, PathLike or None, default is None
        Path or path-like string pointing to a directory where to save the CSV, 
        or None, don't update.
    force_update : bool, default is False
        If True, fetch data and update existing data even if it was modified
        within its update window (for labor market, 25 days).
    name : str or None, default is None
        CSV filename for updating and/or saving.

    Returns
    -------
    labor : Pandas dataframe

    """
    update_threshold = 25
    if name is None:
        name = "labor"

    if update is not None:
        update_path = (Path(update) / name).with_suffix(".csv")
        delta, previous_data = updates._check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update_path} was modified within {update_threshold} "
                  f"day(s). Skipping download...")
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
        labor = updates._revise(new_data=labor, prev_data=previous_data,
                                revise_rows=revise_rows)

    labor = labor.apply(pd.to_numeric, errors="coerce")
    columns._setmeta(labor, area="Mercado laboral", currency="-",
                     inf_adj="No", index="No", seas_adj="NSA",
                     ts_type="-", cumperiods=1)

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        labor.to_csv(save_path)

    return labor
