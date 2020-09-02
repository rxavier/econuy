from os import PathLike, path, listdir
from pathlib import Path
from typing import Union, Optional, Iterable

import pandas as pd
import sqlalchemy as sqla
from pandas.errors import ParserError
from sqlalchemy import select, table, column, and_


def read(con: sqla.engine.base.Connection,
         command: Optional[str] = None,
         table_name: Optional[str] = None,
         cols: Union[str, Iterable[str], None] = None,
         start_date: Optional[str] = None, end_date: Optional[str] = None,
         index_label: str = "index",
         **kwargs) -> pd.DataFrame:
    """
    Convenience wrapper around `pandas.read_sql_query <https://pandas.pydata.
    org/pandas-docs/stable/reference/api/pandas.read_sql_query.html>`_.

    Deals with multiindex column names.

    Parameters
    ----------
    con : sqlalchemy.engine.base.Connection
        Connection to SQL database.
    command : str, sqlalchemy.sql.Selectable or None, default None
        Command to pass to `pandas.read_sql_query <https://pandas.pydata.org/
        pandas-docs/stable/reference/api/pandas.read_sql_query.html>`_. If this
        parameter is not None, `table`, `cols`, `start_date` and `end_date`
        will be ignored.
    table_name : str or None, default None
        String representing which table should be retrieved from the database.
    cols : str, iterable or None, default None
        Column(s) to retrieve. By default, gets all all columns.
    start_date : str or None, default None
        Dates to filter. Inclusive.
    end_date : str or None, default None
        Dates to filter. Inclusive.
    index_label : str, default `index`
        Passed to `pandas.read_sql_query <https://pandas.pydata.org/
        pandas-docs/stable/reference/api/pandas.read_sql_query.html>`_.
        Name of the column in the table which should be used as dataframe
        index.
    **kwargs
        Keyword arguments passed to `pandas.read_sql_query <https://pandas.
        pydata.org/pandas-docs/stable/reference/api/
        pandas.read_sql_query.html>`_.

    Returns
    -------
    SQL queried table : pd.DataFrame

    """
    if command is not None:
        output = pd.read_sql_query(sql=command, con=con,
                                   index_col=index_label, **kwargs)
    else:
        if all(v is None for v in [cols, start_date, end_date]):
            output = pd.read_sql(sql=table_name, con=con,
                                 index_col=index_label,
                                 parse_dates=index_label, **kwargs)
        else:
            if isinstance(cols, Iterable) and not isinstance(cols, str):
                cols_sql = [column(x) for x in cols]
                cols_sql.append(column("index"))
            elif isinstance(cols, str) and cols != "*":
                cols_sql = [column(cols)]
                cols_sql.append(column("index"))
            else:
                cols_sql = "*"
            command = select(cols_sql).select_from(table(table_name))
            dates = column(index_label)
            if start_date is not None:
                if end_date is not None:
                    command = command.where(and_(dates >= f"{start_date}",
                                                 dates <= f"{end_date}"))
                else:
                    command = command.where(dates >= f"{start_date}")
            elif end_date is not None:
                command = command.where(dates <= f"{end_date}")

            output = pd.read_sql(sql=command, con=con,
                                 index_col=index_label,
                                 parse_dates=index_label, **kwargs)
        metadata = pd.read_sql(sql=f"{table_name}_metadata", con=con,
                               index_col="index")
        if isinstance(cols, Iterable) and cols != "*":
            if isinstance(cols, str):
                cols = [cols]
            metadata = metadata.loc[metadata["Indicador"].isin(cols)]
            metadata = metadata.set_index("Indicador").loc[cols].reset_index()

        output.columns = pd.MultiIndex.from_frame(metadata)
        output.rename_axis(None, inplace=True)

    return output


def df_to_sql(df: pd.DataFrame, name: str,
              con: sqla.engine.base.Connection, if_exists: str = "replace",
              index_label: str = "index") -> None:
    """Flatten MultiIndex index columns before creating SQL table
    from dataframe."""
    data = df.copy()
    if isinstance(data.columns, pd.MultiIndex):
        metadata = data.columns.to_frame(index=False)
        metadata.to_sql(name=f"{name}_metadata", con=con, if_exists=if_exists)
        data.columns = data.columns.get_level_values(level=0)

    data.to_sql(name=name, con=con, if_exists=if_exists,
                index_label=index_label)

    return


def insert_csvs(con: sqla.engine.base.Connection,
                directory: Union[str, Path, PathLike]) -> None:
    """Insert all CSV files in data directory into a SQL database."""
    if path.isfile(directory):
        directory = path.dirname(directory)
    for file in [x for x in listdir(directory) if x.endswith(".csv")]:
        full_path = Path(directory) / file
        try:
            data = pd.read_csv(full_path, index_col=0,
                               header=list(range(9)), float_precision="high",
                               parse_dates=True)
        except ParserError:
            data = pd.read_csv(full_path, index_col=0, float_precision="high",
                               parse_dates=True)
        df_to_sql(df=data, name=Path(file).with_suffix("").as_posix(),
                  con=con, index_label="index", if_exists="replace")
        print(f"Inserted {file} into {con.engine.url}.")

    return
