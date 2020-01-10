import os
import datetime as dt

import pandas as pd


def check_modified(data_path: str):

    modified_time = dt.datetime.fromtimestamp(os.path.getmtime(data_path))
    delta = (dt.datetime.now() - modified_time).days
    previous_data = pd.read_csv(data_path, sep=" ", index_col=0, header=[0, 1, 2, 3, 4, 5, 6, 7, 8])
    previous_data.index = pd.to_datetime(previous_data.index)

    return delta, previous_data


def revise(new_data: pd.DataFrame, prev_data: pd.DataFrame, revise: int):

    non_revised = prev_data[:len(prev_data)-revise]
    revised = new_data[len(prev_data)-revise:]
    non_revised.columns = new_data.columns
    updated = non_revised.append(revised, sort=False)

    return updated
