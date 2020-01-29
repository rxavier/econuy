from typing import Optional

import pandas as pd

from econuy.processing import columns

PD_FREQUENCIES = {"A": 1,
                  "Q-DEC": 4,
                  "M": 12}


def freq_resample(df: pd.DataFrame, target: str, operation: str = "sum",
                  interpolation: str = "linear"):
    """Wrapper for the resample method in Pandas.

    Resample taking into account dataframe `type` so that stock data is not
    averaged or summed when resampling; only last value of target frequency
    is considered.

    Parameters
    ----------
    df : Pandas dataframe
    target : str
        Target frequency to resample to. See
        https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    operation : str (default is 'sum')
        'sum', 'average' and 'upsample' are allowed.
    interpolation : bool (default is True)
        Method to use when missing data are produced as a result of resampling.
        See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.interpolate.html

    Returns
    -------
    resampled_df : Pandas dataframe

    Raises
    ------
    ValueError:
        If `operation` is not one of available options and if the input
        dataframe does not have a Type level in its column multiindex.

    """
    if df.columns.get_level_values("Tipo")[0] == "Flujo":
        if operation == "sum":
            resampled_df = df.resample(target).sum()
        elif operation == "average":
            resampled_df = df.resample(target).mean()
        elif operation == "upsample":
            resampled_df = df.resample(target).asfreq()
            resampled_df = resampled_df.interpolate(method=interpolation)
        else:
            raise ValueError("Only sum, average and upsample "
                             "are accepted operations")

        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        if cum_periods != 1:
            input_notna = df.iloc[:, 0].count()
            output_notna = resampled_df.iloc[:, 0].count()
            cum_adj = round(output_notna / input_notna)
            columns.set_metadata(resampled_df,
                                 cumperiods=int(cum_periods * cum_adj))

    elif df.columns.get_level_values("Tipo")[0] == "Stock":
        resampled_df = df.resample(target, convention="end").asfreq()
        resampled_df = resampled_df.interpolate(method=interpolation)
    else:
        raise ValueError("Dataframe needs to have a Type")

    columns.set_metadata(resampled_df)

    return resampled_df


def rolling(df: pd.DataFrame, periods: Optional[int] = None,
            operation: str = "sum"):
    """Wrapper for the rolling method in Pandas.

    If `periods` is None, try to infer the frequency and set `periods`
    according to the following logic: `{'A': 1, 'Q-DEC': 4, 'M': 12}`.

    Parameters
    ----------
    df : Pandas dataframe
    periods : Optional[int] (default is None)
    operation : str (default is 'sum')
        'sum' and 'average' are allowed.

    Returns
    -------
    rolling_df : Pandas dataframe

    """
    window_operation = {
        "sum": lambda x: x.rolling(window=periods,
                                   min_periods=periods).sum(),
        "average": lambda x: x.rolling(window=periods,
                                       min_periods=periods).mean()
    }

    if df.columns.get_level_values("Tipo")[0] == "Stock":
        raise Warning("Rolling operations shouldn't be "
                      "calculated on stock variables")

    if periods is None:
        inferred_freq = pd.infer_freq(df.index)
        periods = PD_FREQUENCIES[inferred_freq]

    rolling_df = df.apply(window_operation[operation])

    columns.set_metadata(rolling_df, cumperiods=periods)

    return rolling_df
