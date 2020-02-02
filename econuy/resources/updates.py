import datetime as dt
from os import path, makedirs, PathLike
from pathlib import Path
from typing import Union

import pandas as pd


def _check_modified(data_path: Union[str, PathLike], multiindex=True):
    """Check how long it has been since the file in path was modified."""
    try:
        modified_time = dt.datetime.fromtimestamp(path.getmtime(data_path))
        delta = (dt.datetime.now() - modified_time).days
        if multiindex is True:
            previous_data = pd.read_csv(data_path, index_col=0,
                                        header=list(range(9)))
        else:
            previous_data = pd.read_csv(data_path, index_col=0)
        previous_data.index = pd.to_datetime(previous_data.index)
    except FileNotFoundError:
        print(f"{data_path} does not exist. No data will be updated")
        delta = 0
        previous_data = pd.DataFrame()

    return delta, previous_data


def _revise(new_data: pd.DataFrame, prev_data: pd.DataFrame, revise_rows: int):
    """Replace n rows of data at the end of a dataframe with new data."""
    if len(prev_data) == 0:
        return new_data
    non_revised = prev_data[:len(prev_data)-revise_rows]
    revised = new_data[len(prev_data)-revise_rows:]
    non_revised.columns = new_data.columns
    updated = non_revised.append(revised, sort=False)

    return updated


def _paths(filepath: Union[str, PathLike, bool], multiple: bool = False,
           name: str = None, multname: str = None):
    """Take a path-like object or bool and return a full path."""
    if multiple is False:
        if isinstance(filepath, PathLike) or isinstance(filepath, str):
            final_path = filepath
        else:
            final_path = path.join("econuy-data", name)
    else:
        if isinstance(filepath, PathLike):
            base = Path(filepath).as_posix()
            if path.isfile(base):
                base = path.dirname(base)
            final_path = path.join(base, multname + ".csv")
        elif isinstance(filepath, str):
            base = filepath
            if path.isfile(base):
                base = path.dirname(base)
            final_path = path.join(base,  multname + ".csv")
        else:
            final_path = path.join("econuy-data", multname + ".csv")
    if not path.exists(path.dirname(final_path)):
        makedirs(path.dirname(final_path))

    return final_path


def rsearch(dir_file: Union[str, PathLike], search_term: str, n: int = 2):
    """Recursively search for a file starting from the n-parent folder of
    a supplied path."""
    i = 0
    while i < n:
        i += 1
        dir_file = path.dirname(dir_file)
    final_path = ([x for x in Path(dir_file).rglob(search_term)][0]
                  .absolute().as_posix())
    return final_path
