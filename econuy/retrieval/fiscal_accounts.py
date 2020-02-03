import datetime as dt
import re
import tempfile
from os import PathLike, path, listdir
from typing import Union

import pandas as pd
import patoolib
import requests
from bs4 import BeautifulSoup
from pandas.tseries.offsets import MonthEnd

from econuy.resources import updates, columns
from econuy.resources.lstrings import fiscal_url, fiscal_sheets


def get(update_dir: Union[str, PathLike, bool] = False, revise_rows: int = 0,
        save_dir: Union[str, PathLike, bool] = False,
        force_update: bool = False):
    """Get fiscal data.

    Parameters
    ----------
    update_dir : str, PathLike or bool (default is False)
        Path, path-like string pointing to a directory for updating, or bool,
        in which case if True, save in predefined file, or False, don't update.
    revise_rows : int (default is 0)
        How many rows of old data to replace with new data.
    save_dir : str, PathLike or bool (default is False)
        Path, path-like string pointing to a directory for saving, or bool,
        in which case if True, save in predefined file, or False, don't save.
    force_update : bool (default is False)
        If True, fetch data and update existing data even if it was modified
        within its update window (for fiscal data, 25 days)

    Returns
    -------
    output : dictionary of Pandas dataframes
        Each dataframe corresponds to a sheet in the fiscal accounts Excel
        provided by the MEF. Not all sheets are considered.

    """
    update_threshold = 25

    if update_dir is not False:
        update_path = updates._paths(update_dir, multiple=True,
                                     multname="fiscal_nfps")
        try:
            modified = dt.datetime.fromtimestamp(path.getmtime(update_path))
            delta = (dt.datetime.now() - modified).days

            if delta < update_threshold and force_update is False:
                print(f"Fiscal data was modified within {update_threshold} "
                      f"day(s). Skipping download...")
                output = {}
                for metadata in fiscal_sheets.values():
                    update_path = updates._paths(update_dir, multiple=True,
                                                 multname=metadata["Name"])
                    delta, previous_data = updates._check_modified(update_path)
                    output.update({metadata["Name"]: previous_data})
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
            for sheet, metadata in fiscal_sheets.items():
                data = (pd.read_excel(xls, sheet_name=sheet).
                        dropna(axis=0, thresh=4).dropna(axis=1, thresh=4).
                        transpose().set_index(2, drop=True))
                data.columns = data.iloc[0]
                data = data[data.index.notnull()].rename_axis(None)
                data.index = data.index + MonthEnd(1)
                data.columns = metadata["Colnames"]

                if update_dir is not False:
                    update_path = updates._paths(update_dir, multiple=True,
                                                 multname=metadata["Name"])
                    delta, previous_data = updates._check_modified(update_path)
                    data = updates._revise(new_data=data,
                                           prev_data=previous_data,
                                           revise_rows=revise_rows)
                data = data.apply(pd.to_numeric, errors="coerce")
                columns._setmeta(
                    data, area="Cuentas fiscales y deuda", currency="UYU",
                    inf_adj="No", index="No", seas_adj="NSA", ts_type="Flujo",
                    cumperiods=1
                )

                if save_dir is not False:
                    save_path = updates._paths(save_dir, multiple=True,
                                               multname=metadata["Name"])
                    data.to_csv(save_path)

                output.update({metadata["Name"]: data})

    return output
