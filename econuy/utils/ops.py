from os import path, PathLike, mkdir
from pathlib import Path
from typing import Union, Optional

import pandas as pd
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import ProgrammingError, OperationalError

from econuy.utils import metadata, sqlutil


def _read(
    data_loc: Union[str, PathLike, Connection, Engine],
    file_fmt: str = "csv",
    multiindex: Optional[str] = "included",
    table_name: Optional[str] = None,
) -> pd.DataFrame:
    """Read a DataFrame from SQL database or CSV/Excel.

    Parameters
    ----------
    data_loc : Union[str, PathLike, Connection, Engine]
        SQL object or path to file.
    table_name : Optional[str]
        Name for input SQL table.
    file_fmt : {'csv', 'xlsx'}
        File format. Ignored if ``data_loc`` refers to a SQL object.
    multiindex : {'included', 'separate', None}
        How to handle multiindexes for metadata. ``None`` keeps only the first
        level, ``included`` keeps as DataFrame columns and ``separate`` saves
        it to another sheet (only valid for Excel-type formats).
    """
    try:
        if isinstance(data_loc, (Engine, Connection)):
            if multiindex is not None:
                previous_data = sqlutil.read(con=data_loc, table_name=table_name)
            else:
                previous_data = pd.read_sql(
                    sql=table_name, con=data_loc, index_col="index", parse_dates="index"
                )
        else:
            date_format = None
            special_date_format = ["cattle", "reserves_changes"]
            if Path(data_loc).stem in special_date_format:
                date_format = "%d/%m/%Y"
            if file_fmt == "csv":
                if multiindex == "included":
                    previous_data = pd.read_csv(
                        data_loc,
                        index_col=0,
                        header=list(range(9)),
                        float_precision="high",
                        encoding="latin1",
                    )
                    metadata._set(previous_data)
                else:
                    previous_data = pd.read_csv(
                        data_loc, index_col=0, header=0, float_precision="high", encoding="latin1"
                    )
            else:
                if multiindex == "included":
                    previous_data = pd.read_excel(
                        data_loc, index_col=0, header=list(range(9)), sheet_name="Data"
                    )
                    metadata._set(previous_data)
                elif multiindex == "separate":
                    excel = pd.ExcelFile(data_loc)
                    previous_data = pd.read_excel(excel, index_col=0, header=0, sheet_name="Data")
                    header = (
                        pd.read_excel(excel, sheet_name="Metadata", index_col=0)
                        .rename_axis("Indicador")
                        .T
                    )
                    previous_data.columns = pd.MultiIndex.from_frame(header.reset_index())
                    metadata._set(previous_data)
                else:
                    previous_data = pd.read_excel(
                        data_loc, index_col=0, header=0, sheet_name="Data"
                    )
            try:
                previous_data.index = pd.to_datetime(previous_data.index, format=date_format)
            except ValueError:
                previous_data.index = pd.to_datetime(previous_data.index, format=None)

    except (ProgrammingError, OperationalError, FileNotFoundError):
        previous_data = pd.DataFrame()

    return previous_data


def _save(
    data: pd.DataFrame,
    data_loc: Union[str, PathLike, Connection, Engine],
    table_name: str = None,
    file_fmt: str = "csv",
    multiindex: str = "included",
):
    """Save a DataFrame to SQL database or CSV/Excel.

    Parameters
    ----------
    data : pd.DataFrame
        Data to save.
    data_loc : Union[str, PathLike, Connection, Engine]
        SQL object or path to file.
    table_name : Optional[str]
        Name for output SQL table.
    file_fmt : {'csv', 'xlsx'}
        File format. Ignored if ``data_loc`` refers to a SQL object.
    multiindex : {'included', 'separate', None}
        How to handle multiindexes for metadata. ``None`` keeps only the first
        level, ``included`` keeps as DataFrame columns and ``separate`` saves
        it to another sheet (only valid for Excel-type formats).
    """
    if isinstance(data_loc, (Engine, Connection)):
        sqlutil.df_to_sql(data, name=table_name, con=data_loc)
    else:
        data_proc = data.copy()
        if file_fmt == "csv":
            if multiindex != "included":
                data_proc.columns = data_proc.columns.get_level_values(0)
                data_proc.to_csv(data_loc, encoding="latin1")
            else:
                data_proc.to_csv(data_loc, encoding="latin1")
        else:
            if multiindex == "included":
                data_proc.to_excel(data_loc, sheet_name="Data")
            elif multiindex == "separate":
                metadata = data_proc.columns.to_frame().set_index("Indicador").T
                data_proc.columns = data_proc.columns.get_level_values(0)
                with pd.ExcelWriter(data_loc) as f:
                    data_proc.to_excel(f, sheet_name="Data")
                    metadata.to_excel(f, sheet_name="Metadata")
            else:
                data_proc.columns = data_proc.columns.get_level_values(0)
                data_proc.to_excel(data_loc, sheet_name="Data")

    return


def _revise(new_data: pd.DataFrame, prev_data: pd.DataFrame, revise_rows: Union[int, str]):
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

    non_revised = prev_data[: len(prev_data) - revise_rows]
    revised = new_data[len(prev_data) - revise_rows :]
    non_revised.columns = new_data.columns
    updated = non_revised.append(revised, sort=False)

    return updated


def _io(
    operation: str,
    data_loc: Union[str, PathLike, Connection, Engine],
    name: str,
    data: Optional[pd.DataFrame] = None,
    file_fmt: str = "csv",
    multiindex: Optional[str] = "included",
) -> Optional[pd.DataFrame]:
    """Save/read a DataFrame to/from SQL database or CSV/Excel.

    Parameters
    ----------
    operation : {'save', 'read'}
        Whether to save or read.
    data : Optional[pd.DataFrame]
        Data to save.
    data_loc : Union[str, PathLike, Connection, Engine]
        SQL object or path to folder.
    name : str
        Name for SQL table or IO file.
    file_fmt : {'csv', 'xlsx'}
        File format. Ignored if ``data_loc`` refers to a SQL object.
    multiindex : {'included', 'separate', None}
        How to handle multiindexes for metadata. ``None`` keeps only the first
        level, ``included`` keeps as DataFrame columns and ``separate`` saves
        it to another sheet (only valid for Excel-type formats).
    """
    valid_fmt = ["csv", "xlsx", "xls"]
    valid_operation = ["read", "save"]
    if file_fmt not in valid_fmt:
        raise ValueError(f"'file_fmt' must be one of {', '.join(valid_fmt)}.")
    if operation not in valid_operation:
        raise ValueError(f"'operation' must be one of {', '.join(valid_operation)}.")
    if multiindex not in ["included", "separate", None]:
        raise ValueError("'multiindex' must be one of 'included', 'separate' or None.")

    suffix = f".{file_fmt}"
    if operation == "read":
        if isinstance(data_loc, (str, PathLike)):
            full_update_loc = (Path(data_loc) / name).with_suffix(suffix)
        else:
            full_update_loc = data_loc
        return _read(full_update_loc, table_name=name, multiindex=multiindex, file_fmt=file_fmt)

    elif operation == "save":
        if isinstance(data_loc, (str, PathLike)):
            full_save_loc = (Path(data_loc) / name).with_suffix(suffix)
            if not path.exists(path.dirname(full_save_loc)):
                mkdir(path.dirname(full_save_loc))
        else:
            full_save_loc = data_loc

        _save(
            data=data,
            data_loc=full_save_loc,
            file_fmt=file_fmt,
            multiindex=multiindex,
            table_name=name,
        )
        return
