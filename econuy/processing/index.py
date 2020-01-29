from datetime import date
from typing import Union

import pandas as pd

from econuy.processing import columns


def base_index(df: pd.DataFrame, start_date: Union[str, date],
               end_date: Union[str, date, None] = None, base: float = 100):
    """Rebase all dataframe columns to a date or range of dates.

    Parameters
    ----------
    df : Pandas dataframe
    start_date : string or date
        Date to which series will be rebased.
    end_date : string or date (default is `None`)
        If specified, series will be rebased to the average between start_date
        and end_date.
    base : float (default is 100)
        Float for which start_date == float or average between start_date and
        end_date == float.

    Returns
    -------
    indexed : Pandas dataframe

    """
    if end_date is None:
        indexed = df.apply(lambda x: x / x[start_date] * base)
        columns.set_metadata(indexed, index=start_date)

    else:
        indexed = df.apply(lambda x: x / x[start_date:end_date].mean() * base)
        columns.set_metadata(indexed, index=f"{start_date}_{end_date}")

    return indexed
