import datetime as dt
import re
import tempfile
from os import PathLike, path, listdir, mkdir
from pathlib import Path
from typing import Union, Optional, Dict

import pandas as pd
import patoolib
import requests
from bs4 import BeautifulSoup
from pandas.tseries.offsets import MonthEnd

from econuy.utils import updates, metadata
from econuy.utils.lstrings import fiscal_url, fiscal_sheets


def get(update_path: Union[str, PathLike, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_path: Union[str, PathLike, None] = None,
        force_update: bool = False,
        name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """Get fiscal data.

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
        modified within its update window (for fiscal accounts, 25 days).
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Monthly fiscal accounts different aggregations : Dict[str, pd.DataFrame]
        Available aggregations: non-financial public sector, consolidated
        public sector, central government, aggregated public enterprises
        and individual public enterprises.

    """
    update_threshold = 25
    if name is None:
        name = "fiscal"

    if update_path is not None:
        full_update_path = (Path(update_path)
                            / f"{name}_nfps").with_suffix(".csv")
        try:
            modified = dt.datetime.fromtimestamp(
                path.getmtime(full_update_path))
            delta = (dt.datetime.now() - modified).days

            if delta < update_threshold and force_update is False:
                print(f"Fiscal data ({full_update_path}) was modified within "
                      f"{update_threshold} day(s). Skipping download...")
                output = {}
                for meta in fiscal_sheets.values():
                    full_update_path = (Path(update_path)
                                        / f"{name}_"
                                          f"{meta['Name']}").with_suffix(
                        ".csv")
                    delta, previous_data = updates._check_modified(
                        full_update_path)
                    output.update({meta["Name"]: previous_data})
                return output
        except FileNotFoundError:
            pass

    response = requests.get(fiscal_url)
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all(href=re.compile("\\.rar$"))
    rar = links[0]["href"]
    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        f.write(requests.get(rar).content)

    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        path_temp = path.join(temp_dir, listdir(temp_dir)[0])

        output = {}
        with pd.ExcelFile(path_temp) as xls:
            for sheet, meta in fiscal_sheets.items():
                data = (pd.read_excel(xls, sheet_name=sheet).
                        dropna(axis=0, thresh=4).dropna(axis=1, thresh=4).
                        transpose().set_index(2, drop=True))
                data.columns = data.iloc[0]
                data = data[data.index.notnull()].rename_axis(None)
                data.index = data.index + MonthEnd(1)
                data.columns = meta["Colnames"]

                if update_path is not None:
                    full_update_path = (Path(update_path)
                                        / f"{name}_"
                                          f"{meta['Name']}").with_suffix(
                        ".csv")
                    delta, previous_data = updates._check_modified(
                        full_update_path)
                    data = updates._revise(new_data=data,
                                           prev_data=previous_data,
                                           revise_rows=revise_rows)
                data = data.apply(pd.to_numeric, errors="coerce")
                metadata._set(
                    data, area="Cuentas fiscales y deuda", currency="UYU",
                    inf_adj="No", unit="Millones", seas_adj="NSA",
                    ts_type="Flujo", cumperiods=1
                )

                if save_path is not None:
                    full_save_path = (Path(save_path)
                                      / f"{name}_"
                                        f"{meta['Name']}").with_suffix(
                        ".csv")
                    if not path.exists(path.dirname(full_save_path)):
                        mkdir(path.dirname(full_save_path))
                    data.to_csv(full_save_path)

                output.update({meta["Name"]: data})

    return output
