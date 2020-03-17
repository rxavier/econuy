import datetime as dt
from os import path, PathLike
from pathlib import Path
from typing import Union

import pandas as pd

from econuy.resources import columns


def _check_modified(data_path: Union[str, PathLike], multiindex=True):
    """Check how long it has been since the file in path was modified."""
    try:
        modified_time = dt.datetime.fromtimestamp(path.getmtime(data_path))
        delta = (dt.datetime.now() - modified_time).days
        if multiindex is True:
            previous_data = pd.read_csv(data_path, index_col=0,
                                        header=list(range(9)),
                                        float_precision="high")
            columns._setmeta(previous_data)
        else:
            previous_data = pd.read_csv(data_path, index_col=0)
        previous_data.index = pd.to_datetime(previous_data.index)
    except FileNotFoundError:
        print(f"{data_path} does not exist. No data will be updated")
        delta = 9999
        previous_data = pd.DataFrame()

    return delta, previous_data


def _revise(new_data: pd.DataFrame, prev_data: pd.DataFrame,
            revise_rows: Union[int, str]):
    """Replace n rows of data at the end of a dataframe with new data."""
    if len(prev_data) == 0:
        return new_data
    frequency = pd.infer_freq(prev_data.index)
    freq_table = {"A": 3, "A-DEC": 3, "Q": 4, "Q-DEC": 4, "M": 12}
    new_data = new_data.apply(pd.to_numeric, errors="coerce")

    if isinstance(revise_rows, str) and revise_rows in "noduplicate":
        prev_data.columns = new_data.columns
        updated = prev_data.append(new_data)
        updated = updated.loc[~updated.index.duplicated(keep="last")]
        updated.sort_index(inplace=True)
        return updated

    elif isinstance(revise_rows, str) and revise_rows in "automatic":
        try:
            revise_rows = freq_table[frequency]
        except KeyError:
            revise_rows = 12
            if len(prev_data) <= 12 or len(new_data) <= 12:
                revise_rows = 3

    elif isinstance(revise_rows, int):
        revise_rows = revise_rows
    else:
        raise ValueError("`revise_rows` accepts int, 'nodup' or 'auto'")

    non_revised = prev_data[:len(prev_data)-revise_rows]
    revised = new_data[len(prev_data)-revise_rows:]
    non_revised.columns = new_data.columns
    updated = non_revised.append(revised, sort=False)

    return updated


def _rsearch(dir_file: Union[str, PathLike], search_term: str, n: int = 2):
    """Recursively search for a file starting from the n-parent folder of
    a supplied path."""
    i = 0
    while i < n:
        i += 1
        dir_file = path.dirname(dir_file)
    try:
        final_path = ([x for x in Path(dir_file).rglob(search_term)][0]
                      .absolute().as_posix())
    except IndexError:
        final_path = True
    return final_path
