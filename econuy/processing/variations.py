import pandas as pd

from econuy.processing import freqs
from econuy.resources import columns


def chg_diff(df: pd.DataFrame, operation: str = "chg",
             period_op: str = "last") -> pd.DataFrame:
    """
    Wrapper for the `pct_change <https://pandas.pydata.org/pandas-docs/stable/
    reference/api/pandas.DataFrame.pct_change.html>`_ and `diff <https://pandas
    .pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.diff.html>`_
    Pandas methods.

    Calculate percentage change or difference for dataframes. The ``period``
    argument takes into account the frequency of the dataframe, i.e.,
    ``inter`` (for interannual) will calculate pct change/differences with
    ``periods=4`` for quarterly frequency, but ``periods=12`` for monthly
    frequency.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    operation : {'chg', 'dif'}
        ``chg`` for percent change or ``diff`` for differences.
    period_op : {'last', 'inter', 'annual'}
        Period with which to calculate change or difference. ``last`` for
        previous period (last month for monthly data), ``inter`` for same
        period last year, ``annual`` for same period last year but taking
        annual averages/sums.

    Returns
    -------
    Percent change or differences dataframe : pd.DataFrame

    Raises
    ------
    ValueError
        If the dataframe is not of frequency M (month end), Q (quarter end) or
        A (year end).

    """
    inferred_freq = pd.infer_freq(df.index)

    type_change = {"last":
                   {"chg": [lambda x: x.pct_change(periods=1),
                            "% variación"],
                    "diff": [lambda x: x.diff(periods=1, min_periods=1),
                             "Cambio"]},
                   "inter":
                   {"chg": [lambda x: x.pct_change(periods=last_year),
                            "% variación interanual"],
                    "diff": [lambda x: x.diff(periods=last_year),
                             "Cambio interanual"]},
                   "annual":
                   {"chg": [lambda x: x.pct_change(periods=last_year),
                            "% variación anual"],
                    "diff": [lambda x: x.diff(periods=last_year),
                             "Cambio anual"]}}

    if inferred_freq == "M":
        last_year = 12
    elif inferred_freq == "Q" or inferred_freq == "Q-DEC":
        last_year = 4
    elif inferred_freq == "A":
        last_year = 1
    else:
        raise ValueError("The dataframe needs to have a frequency of M "
                         "(month end), Q (quarter end) or A (year end)")

    if period_op == "annual":

        if df.columns.get_level_values("Tipo")[0] == "Stock":
            output = df.apply(type_change[period_op][operation][0]).multiply(100)
        else:
            output = freqs.rolling(df, operation="sum")
            output = output.apply(type_change[period_op][operation][0]).multiply(100)

        columns._setmeta(output, index=type_change[period_op][operation][1])

    else:
        output = df.apply(type_change[period_op][operation][0]).multiply(100)
        columns._setmeta(output, index=type_change[period_op][operation][1])

    return output
