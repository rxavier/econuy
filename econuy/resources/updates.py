import os
import datetime as dt
from typing import Union
from pathlib import Path

import pandas as pd


def check_modified(data_path: Union[str, Path]):
    """Check how long it has been since the file in path was modified."""
    modified_time = dt.datetime.fromtimestamp(os.path.getmtime(data_path))
    delta = (dt.datetime.now() - modified_time).days
    previous_data = pd.read_csv(data_path, sep=" ", index_col=0, header=[0, 1, 2, 3, 4, 5, 6, 7, 8])
    previous_data.index = pd.to_datetime(previous_data.index)

    return delta, previous_data


def revise(new_data: pd.DataFrame, prev_data: pd.DataFrame, revise_rows: int):
    """Replace n rows of data at the end of a dataframe with new data."""
    non_revised = prev_data[:len(prev_data)-revise_rows]
    revised = new_data[len(prev_data)-revise_rows:]
    non_revised.columns = new_data.columns
    updated = non_revised.append(revised, sort=False)

    return updated
