from os import PathLike, path, mkdir
from pathlib import Path
from typing import Union, Optional

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from econuy.utils import updates, metadata
from econuy.utils.lstrings import labor_url, wages1_url, wages2_url


def get_rates(update_path: Union[str, PathLike, None] = None,
              revise_rows: Union[str, int] = "nodup",
              save_path: Union[str, PathLike, None] = None,
              force_update: bool = False,
              name: Optional[str] = None) -> pd.DataFrame:
    """Get labor market data.

    Get monthly labor force participation rate, employment rate (employment to
    working-age population) and unemployment rate.

    Parameters
    ----------
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    force_update : bool, default False
        If ``True``, fetch data and update existing data even if it was
        modified within its update window (for labor market, 25 days).
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Monthly participation, employment and unemployment rates : pd.DataFrame

    """
    update_threshold = 25
    if name is None:
        name = "labor"

    if update_path is not None:
        full_update_path = (Path(update_path) / name).with_suffix(".csv")
        delta, previous_data = updates._check_modified(full_update_path)

        if delta < update_threshold and force_update is False:
            print(f"{full_update_path} was modified within {update_threshold} "
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

    if update_path is not None:
        labor = updates._revise(new_data=labor, prev_data=previous_data,
                                revise_rows=revise_rows)

    labor = labor.apply(pd.to_numeric, errors="coerce")
    metadata._set(labor, area="Mercado laboral", currency="-",
                  inf_adj="No", unit="Tasa", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        labor.to_csv(full_save_path)

    return labor


def get_wages(update_path: Union[str, PathLike, None] = None,
              revise_rows: Union[str, int] = "nodup",
              save_path: Union[str, PathLike, None] = None,
              force_update: bool = False,
              name: Optional[str] = None) -> pd.DataFrame:
    """Get general, public and private sector wages data

    Parameters
    ----------
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    force_update : bool, default False
        If ``True``, fetch data and update existing data even if it was
        modified within its update window (for labor market, 25 days).
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Monthly wages separated by public and private sector : pd.DataFrame

    """
    update_threshold = 25
    if name is None:
        name = "wages"

    if update_path is not None:
        full_update_path = (Path(update_path) / name).with_suffix(".csv")
        delta, previous_data = updates._check_modified(full_update_path)

        if delta < update_threshold and force_update is False:
            print(f"{full_update_path} was modified within {update_threshold} "
                  f"day(s). Skipping download...")
            return previous_data

    historical = pd.read_excel(wages1_url, skiprows=8, usecols="A:B")
    historical = historical.dropna(how="any").set_index("Unnamed: 0")
    current = pd.read_excel(wages2_url, skiprows=8, usecols="A,C:D")
    current = current.dropna(how="any").set_index("Unnamed: 0")
    wages = pd.concat([historical, current], axis=1)
    wages.index = wages.index + MonthEnd(1)
    wages.columns = ["Índice medio de salarios",
                     "Índice medio de salarios privados",
                     "Índice medio de salarios públicos"]

    if update_path is not None:
        wages = updates._revise(new_data=wages, prev_data=previous_data,
                                revise_rows=revise_rows)

    wages = wages.apply(pd.to_numeric, errors="coerce")
    metadata._set(wages, area="Mercado laboral", currency="UYU",
                  inf_adj="No", unit="2008-07-31=100", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        wages.to_csv(full_save_path)

    return wages
