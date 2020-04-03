import platform
from datetime import date
from os import PathLike, getcwd, path
from pathlib import Path
from typing import Union, Optional, Tuple
import warnings

import numpy as np
import pandas as pd
from statsmodels.api import tsa
from statsmodels.tools.sm_exceptions import X13Error
from statsmodels.tsa import x13

from econuy.utils import metadata, updates
from econuy.retrieval import cpi, national_accounts, nxr


def convert_usd(df: pd.DataFrame,
                update_path: Union[str, PathLike, None] = None,
                save_path: Union[str, PathLike, None] = None) -> pd.DataFrame:
    """
    Convert dataframe from UYU to USD.

    Convert a dataframe's columns from Uruguayan pesos to US dollars. Call the
    :func:`econuy.retrieval.nxr.get_monthly` function to obtain nominal
    exchange rates, and take into account whether the input dataframe's
    ``Type``, as defined by its multiindex, is flow or stock, in order to `
    choose end of period or monthly average NXR. Also take into account the
    input dataframe's frequency and whether columns represent rolling averages
    or sums.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.

    Returns
    -------
    Input dataframe measured in US dollars : pd.DataFrame

    """
    inferred_freq = pd.infer_freq(df.index)
    nxr_data = nxr.get_monthly(update_path=update_path, revise_rows=6,
                               save_path=save_path, force_update=False)

    if df.columns.get_level_values("Tipo")[0] == "Flujo":
        metadata._set(nxr_data, ts_type="Flujo")
        nxr_freq = resample(nxr_data, target=inferred_freq,
                            operation="average").iloc[:, [0]]
        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        nxr_freq = rolling(nxr_freq, periods=cum_periods,
                           operation="average")

    elif df.columns.get_level_values("Tipo")[0] == "Stock":
        metadata._set(nxr_data, ts_type="Stock")
        nxr_freq = resample(nxr_data, target=inferred_freq,
                            operation="average").iloc[:, [1]]
    else:
        raise ValueError("Dataframe needs to have a valid 'Type'.")

    nxr_to_use = nxr_freq[nxr_freq.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / nxr_to_use)
    metadata._set(converted_df, currency="USD")

    return converted_df


def convert_real(df: pd.DataFrame, start_date: Union[str, date, None] = None,
                 end_date: Union[str, date, None] = None,
                 update_path: Union[str, PathLike, None] = None,
                 save_path: Union[str, PathLike, None] = None) -> pd.DataFrame:
    """
    Convert dataframe to real prices.

    Convert a dataframe's columns to real prices. Call the
    :func:`econuy.retrieval.cpi.get` function to obtain the consumer price
    index. take into account the input dataframe's frequency and whether
    columns represent rolling averages or sums. Allow choosing a single period,
    a range of dates or no period as a base (i.e., period for which the
    average/sum of input dataframe and output dataframe is the same).

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    start_date : str, datetime.date or None, default None
        If set to a date-like string or a date, and ``end_date`` is None, the
        base period will be ``start_date``.
    end_date : str, datetime.date or None, default None
        If ``start_date`` is set, calculate so that the data is in constant
        prices of ``start_date-end_date``.
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.

    Returns
    -------
    Input dataframe measured at constant prices : pd.DataFrame

    """
    inferred_freq = pd.infer_freq(df.index)
    cpi_data = cpi.get(update_path=update_path, revise_rows=6,
                       save_path=save_path,
                       force_update=False)
    metadata._set(cpi_data, ts_type="Flujo")
    cpi_freq = resample(cpi_data, target=inferred_freq,
                        operation="average").iloc[:, [0]]
    cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
    cpi_freq = rolling(cpi_freq, periods=cum_periods,
                       operation="average")
    cpi_to_use = cpi_freq[cpi_freq.index.isin(df.index)].iloc[:, 0]

    if start_date is None:
        converted_df = df.apply(lambda x:
                                x / cpi_to_use)
        col_text = "Const."
    elif end_date is None:
        converted_df = df.apply(lambda x:
                                x / cpi_to_use * cpi_to_use[start_date])
        col_text = f"Const. {start_date}"
    else:
        converted_df = df.apply(
            lambda x: x / cpi_to_use * cpi_to_use[start_date:end_date].mean()
        )
        col_text = f"Const. {start_date}_{end_date}"

    metadata._set(converted_df, inf_adj=col_text)

    return converted_df


def convert_gdp(df: pd.DataFrame,
                update_path: Union[str, PathLike, None] = None,
                save_path: Union[str, PathLike, None] = None) -> pd.DataFrame:
    """
    Calculate dataframe as percentage of GDP.

    Convert a dataframe's columns to percentage of GDP. Call the
    :func:`econuy.retrieval.national_accounts._lin_gdp` function to obtain UYU
    and USD quarterly GDP series. Take into account the input dataframe's
    currency for chossing UYU or USD GDP. If frequency of input dataframe is
    higher than quarterly, GDP will be upsampled and linear interpolation will
    be performed to complete missing data.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.

    Returns
    -------
    Input dataframe as a percentage of GDP : pd.DataFrame

    Raises
    ------
    ValueError
        If frequency of input dataframe not any of 'M', 'MS', 'Q', 'Q-DEC', 'A'
        or 'A-DEC'.

    """
    inferred_freq = pd.infer_freq(df.index)
    gdp = national_accounts._lin_gdp(update_path=update_path,
                                     save_path=save_path, force_update=False)

    if inferred_freq in ["M", "MS"]:
        gdp = resample(gdp, target=inferred_freq,
                       operation="upsample", interpolation="linear")
    elif inferred_freq in ["Q", "Q-DEC", "A", "A-DEC"]:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()
    else:
        raise ValueError("Frequency of input dataframe not any of 'M', 'MS', "
                         "'Q', 'Q-DEC', 'A' or 'A-DEC'.")

    if df.columns.get_level_values("Moneda")[0] == "USD":
        gdp = gdp.iloc[:, 1].to_frame()
    else:
        gdp = gdp.iloc[:, 0].to_frame()

    gdp_to_use = gdp[gdp.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / gdp_to_use).multiply(100)

    metadata._set(converted_df, unit="% PBI")

    return converted_df


def resample(df: pd.DataFrame, target: str, operation: str = "sum",
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
    ValueError
        If ``operation`` is not one of available options and if the input
        dataframe does not have a ``Type`` level in its column multiindex.

    Warns
    -----
    UserWarning
        If input dataframe has ``-`` as frequency in its metadata, warn and
        set to flow.

    """
    if df.columns.get_level_values("Tipo")[0] == "-":
        warnings.warn("Dataframe has no Type, setting to 'Flujo'", UserWarning)
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
            raise ValueError("Only 'sum', 'average' and 'upsample' "
                             "are accepted operations")

        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        if cum_periods != 1:
            input_notna = df.iloc[:, 0].count()
            output_notna = resampled_df.iloc[:, 0].count()
            cum_adj = round(output_notna / input_notna)
            metadata._set(resampled_df,
                          cumperiods=int(cum_periods * cum_adj))

    else:
        resampled_df = df.resample(target, convention="end").asfreq()
        resampled_df = resampled_df.interpolate(method=interpolation)

    metadata._set(resampled_df)

    return resampled_df


def rolling(df: pd.DataFrame, periods: Optional[int] = None,
            operation: str = "sum") -> pd.DataFrame:
    """
    Wrapper for the `rolling method <https://pandas.pydata.org/pandas-docs/
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

    Warns
    -----
    UserWarning
        If the input dataframe is a stock time series.

    """
    pd_frequencies = {"A": 1,
                      "A-DEC": 1,
                      "Q": 4,
                      "Q-DEC": 4,
                      "M": 12}

    window_operation = {
        "sum": lambda x: x.rolling(window=periods,
                                   min_periods=periods).sum(),
        "average": lambda x: x.rolling(window=periods,
                                       min_periods=periods).mean()
    }

    if df.columns.get_level_values("Tipo")[0] == "Stock":
        warnings.warn("Rolling operations shouldn't be "
                      "calculated on stock variables", UserWarning)

    if periods is None:
        inferred_freq = pd.infer_freq(df.index)
        periods = pd_frequencies[inferred_freq]

    rolling_df = df.apply(window_operation[operation])

    metadata._set(rolling_df, cumperiods=periods)

    return rolling_df


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
        metadata._set(indexed, unit=f"{start_date}={base}")

    else:
        indexed = df.apply(lambda x: x / x[start_date:end_date].mean() * base)
        metadata._set(indexed, unit=f"{start_date}_{end_date}={base}")

    return indexed


# The `_open_and_read` function needs to be monkey-patched to specify the
# encoding or decomposition will fail on Windows
def _new_open_and_read(fname):
    with open(fname, 'r', encoding='utf8') as fin:
        fout = fin.read()
    return fout


x13._open_and_read = _new_open_and_read


def decompose(df: pd.DataFrame, trading: bool = True, outlier: bool = True,
              x13_binary: Union[str, PathLike] = "search",
              search_parents: int = 1) -> Optional[Tuple[pd.DataFrame,
                                                         pd.DataFrame]]:
    """
    Apply X13 decomposition. Return trend and seasonally adjusted dataframes.

    Decompose the series in a Pandas dataframe using the US Census X13
    methodology. Will try different combinations of the ``trading`` and
    ``outlier`` arguments if an X13 error is raised. Requires providing the X13
    binary. Please refer to the README for instructions on where to get this
    binary.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    trading : bool, default True
        Whether to automatically detect trading days.
    outlier : bool, default True
        Whether to automatically detect outliers.
    x13_binary: str or os.PathLike, default 'search'
        Location of the X13 binary. If ``search`` is used, will attempt to find
        the binary in the project structure.
    search_parents: int, default 1
        If ``search`` is chosen for ``x13_binary``, this parameter controls how
        many parent directories to go up before recursively searching for the
        binary.

    Returns
    -------
    Decomposed dataframes : Tuple[pd.DataFrame, pd.DataFrame] or None
        Tuple containing the trend component and the seasonally adjusted
        series.

    Raises
    ------
    ValueError
        If the path provided for the X13 binary does not point to a file.

    """
    if x13_binary == "search":
        search_term = "x13as"
        if platform.system() == "Windows":
            search_term += ".exe"
        binary_path = updates._rsearch(dir_file=getcwd(), n=search_parents,
                                       search_term=search_term)
    elif isinstance(x13_binary, str):
        binary_path = x13_binary
    else:
        binary_path = Path(x13_binary).as_posix()

    if path.isfile(binary_path) is False:
        raise ValueError("X13 binary missing. Please refer to the README "
                         "for instructions on where to get binaries for "
                         "Windows and Unix, and how to compile it for macOS.")

    df_proc = df.copy()
    old_columns = df_proc.columns
    df_proc.columns = df_proc.columns.get_level_values(level=0)
    df_proc.index = pd.to_datetime(df_proc.index, errors="coerce")

    trends = []
    seas_adjs = []
    for column in range(len(df_proc.columns)):
        series = df_proc.iloc[:, column].dropna()
        try:
            decomposition = tsa.x13_arima_analysis(
                series, outlier=outlier, trading=trading, forecast_periods=0,
                x12path=binary_path, prefer_x13=True
            )
            trend = decomposition.trend.reindex(df_proc.index)
            seas_adj = decomposition.seasadj.reindex(df_proc.index)

        except X13Error:
            if outlier is True:
                try:
                    print(f"X13 error found while processing "
                          f"'{df_proc.columns[column]}' with selected "
                          f"parameters. Trying with outlier=False...")
                    return decompose(df=df, outlier=False)

                except X13Error:
                    try:
                        print(f"X13 error found while processing "
                              f"'{df_proc.columns[column]}' with "
                              f"trading=True. Trying with trading=False...")
                        return decompose(df=df, outlier=False,
                                         trading=False)

                    except X13Error:
                        print(f"X13 error found while processing "
                              f"'{df_proc.columns[column]}'. "
                              f"Filling with nan.")
                        trend = pd.Series(np.nan, index=df_proc.index)
                        seas_adj = pd.Series(np.nan, index=df_proc.index)

            elif trading is True:
                try:
                    print(f"X13 error found while processing "
                          f"'{df_proc.columns[column]}' "
                          f"with trading=True. Trying with "
                          f"trading=False...")
                    return decompose(df=df, trading=False)

                except X13Error:
                    print(f"X13 error found while processing "
                          f"'{df_proc.columns[column]}'. Filling with nan.")
                    trend = pd.Series(np.nan, index=df_proc.index)
                    seas_adj = pd.Series(np.nan, index=df_proc.index)

            else:
                try:
                    return decompose(df=df)

                except X13Error:
                    print(f"X13 error found while processing "
                          f"'{df_proc.columns[column]}'. "
                          f"Filling with nan.")
                    trend = pd.Series(np.nan, index=df_proc.index)
                    seas_adj = pd.Series(np.nan, index=df_proc.index)

        trends.append(trend)
        seas_adjs.append(seas_adj)

    trends = pd.concat(trends, axis=1)
    seas_adjs = pd.concat(seas_adjs, axis=1)

    trends.columns = old_columns
    seas_adjs.columns = old_columns

    metadata._set(trends, seas_adj="Tendencia")

    metadata._set(seas_adjs, seas_adj="SA")

    return trends, seas_adjs


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
    operation : {'chg', 'diff'}
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
        If the dataframe is not of frequency ``M`` (month), ``Q`` or
        ``Q-DEC`` (quarter), or ``A`` or ``A-DEC`` (year).

    """
    inferred_freq = pd.infer_freq(df.index)

    type_change = {"last":
                   {"chg": [lambda x: x.pct_change(periods=1),
                            "% variación"],
                    "diff": [lambda x: x.diff(periods=1),
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
    elif inferred_freq == "A" or inferred_freq == "A-DEC":
        last_year = 1
    else:
        raise ValueError("The dataframe needs to have a frequency of M "
                         "(month end), Q (quarter end) or A (year end)")

    if period_op == "annual":

        if df.columns.get_level_values("Tipo")[0] == "Stock":
            output = df.apply(type_change[period_op][operation][0])
        else:
            output = rolling(df, operation="sum")
            output = output.apply(
                type_change[period_op][operation][0])

        metadata._set(output, unit=type_change[period_op][operation][1])

    else:
        output = df.apply(type_change[period_op][operation][0])
        metadata._set(output, unit=type_change[period_op][operation][1])

    if operation == "chg":
        output = output.multiply(100)

    return output
