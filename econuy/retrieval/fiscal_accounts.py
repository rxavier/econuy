import os
import re
import tempfile
import datetime as dt

import pandas as pd
from pandas.tseries.offsets import MonthEnd
import patoolib
import requests
from bs4 import BeautifulSoup

from econuy.config import ROOT_DIR
from econuy.processing import updates, columns
from econuy.resources.utils import fiscal_url, fiscal_sheets

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 25


def get(update: bool = False, revise_rows: int = 0,
        save: bool = False, force_update: bool = False):
    """Get fiscal data.

    Parameters
    ----------
    update : bool (default is False)
        If true, try to update existing data on disk.
    revise_rows : int (default is 0)
        How many rows of old data to replace with new data.
    save : bool (default is False)
        If true, save output dataframe in CSV format.
    force_update : bool (default is False)
        If True, fetch data and update existing data even if it was modified
        within its update window (for fiscal data, 25 days)

    Returns
    -------
    output : dictionary of Pandas dataframes
        Each dataframe corresponds to a sheet in the fiscal accounts Excel
        provided by the MEF. Not all sheets are considered.

    """
    if update is True:
        update_path = os.path.join(DATA_PATH, "fiscal_nfps.csv")
        modified = dt.datetime.fromtimestamp(os.path.getmtime(update_path))
        delta = (dt.datetime.now() - modified).days

        if delta < update_threshold and force_update is False:
            print(f"Fiscal data was modified within {update_threshold} day(s)."
                  f" Skipping download...")
            output = {}
            for metadata in fiscal_sheets.values():
                update_path = os.path.join(DATA_PATH,
                                           metadata['Name'] + ".csv")
                delta, previous_data = updates.check_modified(update_path)
                output.update({metadata["Name"]: previous_data})
            return output

    response = requests.get(fiscal_url)
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all(href=re.compile("\\.rar$"))
    rar = links[0]["href"]

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        f.write(requests.get(rar).content)

    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir)
        path = os.path.join(temp_dir, os.listdir(temp_dir)[0])

        output = {}
        with pd.ExcelFile(path) as xls:
            for sheet, metadata in fiscal_sheets.items():
                if update is True:
                    update_path = os.path.join(DATA_PATH,
                                               metadata['Name'] + ".csv")
                    delta, previous_data = updates.check_modified(update_path)

                    if delta < update_threshold and force_update is False:
                        print(f"{metadata['Name']}.csv was modified within "
                              f"{update_threshold} day(s). "
                              f"Skipping download...")
                        output.update({metadata["Name"]: previous_data})
                        continue

                data = (pd.read_excel(xls, sheet_name=sheet).
                        dropna(axis=0, thresh=4).dropna(axis=1, thresh=4).
                        transpose().set_index(2, drop=True))
                data.columns = data.iloc[0]
                data = data[data.index.notnull()].rename_axis(None)
                data.index = data.index + MonthEnd(1)
                data.columns = metadata["Colnames"]

                if update is True:
                    data = updates.revise(new_data=data,
                                          prev_data=previous_data,
                                          revise_rows=revise_rows)
                data = data.apply(pd.to_numeric, errors="coerce")
                columns.set_metadata(
                    data, area="Cuentas fiscales y deuda", currency="UYU",
                    inf_adj="No", index="No", seas_adj="NSA", ts_type="Flujo",
                    cumperiods=1
                )

                if save is True:
                    save_path = os.path.join(DATA_PATH,
                                             metadata["Name"] + ".csv")
                    data.to_csv(save_path, sep=" ")

                output.update({metadata["Name"]: data})

    return output
