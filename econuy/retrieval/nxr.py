from os import PathLike, mkdir, path
from pathlib import Path
from typing import Union, Optional

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from econuy.resources import updates, columns
from econuy.resources.lstrings import nxr_url


def get(update: Union[str, PathLike, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save: Union[str, PathLike, None] = None,
        force_update: bool = False,
        name: Optional[str] = None) -> pd.DataFrame:
    """Get nominal exchange rate data.

    Parameters
    ----------
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    force_update : bool, default False
        If ``True``, fetch data and update existing data even if it was
        modified within its update window(for nominal exchange rates, 25 days).
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Monthly nominal exchange rates : pd.DataFrame
        Buy and sell, and monthly average and end of period.

    """
    update_threshold = 25
    if name is None:
        name = "nxr"

    if update is not None:
        update_path = (Path(update) / name).with_suffix(".csv")
        delta, previous_data = updates._check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update_path} was modified within {update_threshold} "
                  f"day(s). Skipping download...")
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
        nxr = updates._revise(new_data=nxr, prev_data=previous_data,
                              revise_rows=revise_rows)

    nxr = nxr.apply(pd.to_numeric, errors="coerce")
    columns._setmeta(nxr, area="Precios y salarios", currency="-",
                     inf_adj="No", index="No", seas_adj="NSA",
                     ts_type="-", cumperiods=1)

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        if not path.exists(path.dirname(save_path)):
            mkdir(path.dirname(save_path))
        nxr.to_csv(save_path)

    return nxr
