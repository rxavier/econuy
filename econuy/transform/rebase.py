from typing import Union
from datetime import datetime

import pandas as pd

from econuy.utils import metadata


def rebase(
    df: pd.DataFrame,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime, None] = None,
    base: Union[int, float] = 100.0,
) -> pd.DataFrame:
    """
    Scale to a period or range of periods.

    See Also
    --------
    :mod:`~econuy.core.Pipeline.rebase`.

    """
    all_metadata = df.columns.droplevel("Indicador")
    if all(x == all_metadata[0] for x in all_metadata):
        return _rebase(df=df, end_date=end_date, start_date=start_date, base=base)
    else:
        columns = []
        for column_name in df.columns:
            df_column = df[[column_name]]
            converted = _rebase(df=df_column, end_date=end_date, start_date=start_date, base=base)
            columns.append(converted)
        return pd.concat(columns, axis=1)


def _rebase(
    df: pd.DataFrame,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime, None] = None,
    base: float = 100.0,
) -> pd.DataFrame:
    if end_date is None:
        start_date = df.iloc[df.index.get_indexer([start_date], method="nearest")].index[0]
        indexed = df.apply(lambda x: x / x.loc[start_date] * base)
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if not isinstance(base, int):
            if base.is_integer():
                base = int(base)
        m_start = start_date.strftime("%Y-%m")
        metadata._set(indexed, unit=f"{m_start}={base}")

    else:
        indexed = df.apply(lambda x: x / x[start_date:end_date].mean() * base)
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        m_start = start_date.strftime("%Y-%m")
        m_end = end_date.strftime("%Y-%m")
        if not isinstance(base, int):
            if base.is_integer():
                base = int(base)
        if m_start == m_end:
            metadata._set(indexed, unit=f"{m_start}={base}")
        else:
            metadata._set(indexed, unit=f"{m_start}_{m_end}={base}")

    return indexed
