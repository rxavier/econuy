from os import PathLike
from typing import Union

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from econuy.resources import updates, columns
from econuy.resources.lstrings import nxr_url


def get(update: Union[str, PathLike, bool] = False, revise_rows: int = 0,
        save: Union[str, PathLike, bool] = False, force_update: bool = False):
    """Get nominal exchange rate data.

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
        within its update window (for NXR, 25 days)

    Returns
    -------
    nxr : Pandas dataframe

    """
    update_threshold = 25

    if update is not False:
        update_path = updates._paths(update, multiple=False, name="nxr.csv")
        delta, previous_data = updates._check_modified(update_path)

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

    if update is not False:
        nxr = updates._revise(new_data=nxr, prev_data=previous_data,
                              revise_rows=revise_rows)

    nxr = nxr.apply(pd.to_numeric, errors="coerce")
    columns._setmeta(nxr, area="Precios y salarios", currency="-",
                     inf_adj="No", index="No", seas_adj="NSA",
                     ts_type="-", cumperiods=1)

    if save is not False:
        save_path = updates._paths(save, multiple=False, name="nxr.csv")
        nxr.to_csv(save_path)

    return nxr
