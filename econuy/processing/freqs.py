from typing import Optional

import pandas as pd

from econuy.resources import columns

PD_FREQUENCIES = {"A": 1,
                  "Q-DEC": 4,
                  "M": 12}


def freq_resample(df: pd.DataFrame, target: str, operation: str = "sum",
                  interpolation: str = "linear") -> pd.DataFrame:
    """
    Wrapper for the `resample method <https://pandas.pydata.org/pandas-docs
    stable/reference/api/pandas.DataFrame.resample.html>`_ in Pandas.

    Resample taking into account dataframe ``Type`` so that stock data is not
    averaged or summed when resampling; only last value of target frequency
    is considered.

    Parameters
    ----------
    df : Pandas dataframe
        Input dataframe.
    target : str
        Target frequency to resample to. See
        `Pandas offset aliases <https://pandas.pydata.org/pandas-docs/stable/
        user_guide/timeseries.html#offset-aliases>`_
    operation : {'sum', 'average', 'upsample'}
        Operation to use for resampling.
    interpolation : bool, default True
        Method to use when missing data are produced as a result of resampling.
        See `Pandas interpolation method <https://pandas.pydata.org/pandas-docs
        /stable/reference/api/pandas.Series.interpolate.html>`_

    Returns
    -------
    Input dataframe at the frequency defined in ``target`` : pd.DataFrame

    Raises
    ------
    ValueError:
        If ``operation`` is not one of available options and if the input
        dataframe does not have a ``Type`` level in its column multiindex.

    """
    if df.columns.get_level_values("Tipo")[0] == "-":
        print("Dataframe has no Type, setting to 'Flujo'")
        df.columns = df.columns.set_levels(["Flujo"], level="Tipo")

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
            columns._setmeta(resampled_df,
                             cumperiods=int(cum_periods * cum_adj))

    elif df.columns.get_level_values("Tipo")[0] == "Stock":
        resampled_df = df.resample(target, convention="end").asfreq()
        resampled_df = resampled_df.interpolate(method=interpolation)
    else:
        raise ValueError("Dataframe needs to have a valid Type ('Flujo', "
                         "'Stock' or '-'")

    columns._setmeta(resampled_df)

    return resampled_df


def rolling(df: pd.DataFrame, periods: Optional[int] = None,
            operation: str = "sum") -> pd.DataFrame:
    """
    Wrapper for the `rolling method <hhttps://pandas.pydata.org/pandas-docs/
    stable/reference/api/pandas.DataFrame.rolling.html>`_ in Pandas.

    If ``periods`` is ``None``, try to infer the frequency and set ``periods``
    according to the following logic: ``{'A': 1, 'Q-DEC': 4, 'M': 12}``.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    periods : int, default None
        How many periods the window should cover.
    operation : {'sum', 'average'}
        Operation used to calculate rolling windows.

    Returns
    -------
    Input dataframe with rolling windows : pd.DataFrame

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

    columns._setmeta(rolling_df, cumperiods=periods)

    return rolling_df
