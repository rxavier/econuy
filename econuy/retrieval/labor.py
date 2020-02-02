from os import PathLike
from typing import Union

import pandas as pd

from econuy.resources import updates, columns
from econuy.resources.lstrings import labor_url


def get(update: Union[str, PathLike, bool] = False, revise_rows: int = 0,
        save: Union[str, PathLike, bool] = False, force_update: bool = False):
    """Get labor market data.

    Get monthly labor force participation rate, employment rate (employment to
    working-age population) and unemployment rate.

    Parameters
    ----------
    update : str, PathLike or bool (default is False)
        Path, path-like string pointing to a CSV file for updating, or bool,
        in which case if True, save in predefined file, or False, don't update.
    revise_rows : int (default is 0)
        How many rows of old data to replace with new data.
    save : str, PathLike or bool (default is False)
        Path, path-like string pointing to a CSV file for saving, or bool,
        in which case if True, save in predefined file, or False, don't save.
    force_update : bool (default is False)
        If True, fetch data and update existing data even if it was modified
        within its update window (for labor market, 25 days)

    Returns
    -------
    labor : Pandas dataframe

    """
    update_threshold = 25

    if update is not False:
        update_path = updates._paths(update, multiple=False, name="labor.csv")
        delta, previous_data = updates._check_modified(update_path)

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

    if update is not False:
        labor = updates._revise(new_data=labor, prev_data=previous_data,
                                revise_rows=revise_rows)

    labor = labor.apply(pd.to_numeric, errors="coerce")
    columns._setmeta(labor, area="Mercado laboral", currency="-",
                     inf_adj="No", index="No", seas_adj="NSA",
                     ts_type="-", cumperiods=1)

    if save is not False:
        save_path = updates._paths(save, multiple=False, name="labor.csv")
        labor.to_csv(save_path)

    return labor
