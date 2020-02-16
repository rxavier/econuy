from datetime import date
from typing import Union

import pandas as pd

from econuy.resources import columns


def base_index(df: pd.DataFrame, start_date: Union[str, date],
               end_date: Union[str, date, None] = None,
               base: float = 100) -> pd.DataFrame:
    """Rebase all dataframe columns to a date or range of dates.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    start_date : string or datetime.date
        Date to which series will be rebased.
    end_date : string or datetime.date, default None
        If specified, series will be rebased to the average between
        ``start_date`` and ``end_date``.
    base : float, default 100
        Float for which ``start_date`` == ``base`` or average between
        ``start_date`` and ``end_date`` == ``base``.

    Returns
    -------
    Input dataframe with a base period index : pd.DataFrame

    """
    if end_date is None:
        indexed = df.apply(lambda x: x / x[start_date] * base)
        columns._setmeta(indexed, index=start_date)

    else:
        indexed = df.apply(lambda x: x / x[start_date:end_date].mean() * base)
        columns._setmeta(indexed, index=f"{start_date}_{end_date}")

    return indexed
