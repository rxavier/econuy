from os import path, PathLike, mkdir
from pathlib import Path
from typing import Union, Optional

import pandas as pd
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import ProgrammingError

from econuy.utils import metadata, sqlutil


def _check_modified(data_path: Union[str, PathLike,
                                     Connection, Engine],
                    multiindex=True,
                    table_name: Optional[str] = None,
                    index_label: Optional[str] = None):
    """Load existing data from CSV or SQL."""
    try:
        if isinstance(data_path, (Engine, Connection)):
            if multiindex is True:
                previous_data = sqlutil.read(con=data_path,
                                             table_name=table_name,
                                             index_label=index_label)
            else:
                previous_data = pd.read_sql(sql=table_name,
                                            con=data_path,
                                            index_col=index_label,
                                            parse_dates=index_label)
        else:
            if multiindex is True:
                previous_data = pd.read_csv(data_path, index_col=0,
                                            parse_dates=True,
                                            header=list(range(9)),
                                            float_precision="high")
                metadata._set(previous_data)
            else:
                previous_data = pd.read_csv(data_path, index_col=0,
                                            parse_dates=True,
                                            float_precision="high")
    except (ProgrammingError, FileNotFoundError):
        print(f"Data does not exist. No data will be updated")
        previous_data = pd.DataFrame()

    return previous_data


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

    non_revised = prev_data[:len(prev_data) - revise_rows]
    revised = new_data[len(prev_data) - revise_rows:]
    non_revised.columns = new_data.columns
    updated = non_revised.append(revised, sort=False)

    return updated


def _update_save(operation: str,
                 data_path: Union[str, PathLike, Connection, Engine],
                 name: str,
                 data: Optional[pd.DataFrame] = None,
                 index_label: str = "index",
                 multiindex: bool = True) -> Optional[pd.DataFrame]:
    if operation == "update":
        if not isinstance(data_path, (Connection, Engine)):
            full_update_path = (Path(data_path) / name).with_suffix(".csv")
        else:
            full_update_path = data_path
        return _check_modified(full_update_path, table_name=name,
                               index_label=index_label, multiindex=multiindex)

    elif operation == "save":
        if not isinstance(data_path, (Connection, Engine)):
            full_save_path = (Path(data_path) / name).with_suffix(".csv")
            if not path.exists(path.dirname(full_save_path)):
                mkdir(path.dirname(full_save_path))
            data.to_csv(full_save_path)
        else:
            full_update_path = data_path
            sqlutil.df_to_sql(data, name=name,
                              con=full_update_path,
                              index_label=index_label)
        return


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
