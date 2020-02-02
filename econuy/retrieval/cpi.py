from os import PathLike
from typing import Union

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from econuy.resources import updates, columns
from econuy.resources.lstrings import cpi_url


def get(update: Union[str, PathLike, bool] = False, revise_rows: int = 0,
        save: Union[str, PathLike, bool] = False, force_update: bool = False):
    """Get CPI data.

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
        within its update window (for CPI, 25 days)

    Returns
    -------
    cpi : Pandas dataframe

    """
    update_threshold = 25

    if update is not False:
        update_path = updates._paths(update, multiple=False, name="cpi.csv")
        delta, previous_data = updates._check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update} was modified within {update_threshold} day(s). "
                  f"Skipping download...")
            return previous_data

    cpi_raw = pd.read_excel(cpi_url, skiprows=7).dropna(axis=0, thresh=2)
    cpi = (cpi_raw.drop(["Mensual", "Acum.año", "Acum.12 meses"], axis=1).
           dropna(axis=0, how="all").set_index("Mes y año").rename_axis(None))
    cpi.columns = ["Índice de precios al consumo"]
    cpi.index = cpi.index + MonthEnd(1)

    if update is not False:
        cpi = updates._revise(new_data=cpi, prev_data=previous_data,
                              revise_rows=revise_rows)

    cpi = cpi.apply(pd.to_numeric, errors="coerce")
    columns._setmeta(cpi, area="Precios y salarios", currency="-",
                     inf_adj="No", index="2010-10-31", seas_adj="NSA",
                     ts_type="-", cumperiods=1)

    if save is not False:
        save_path = updates._paths(save, multiple=False, name="cpi.csv")
        cpi.to_csv(save_path)

    return cpi
