import platform
import warnings
from datetime import date, datetime
from os import PathLike, getcwd, path
from pathlib import Path
from typing import Union, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy.engine.base import Connection, Engine
from statsmodels.tools.sm_exceptions import X13Error, X13Warning
from statsmodels.tsa import x13
from statsmodels.tsa.x13 import x13_arima_analysis as x13a
from statsmodels.tsa.seasonal import STL, seasonal_decompose

from econuy.retrieval import prices, economic_activity
from econuy.utils import metadata


def convert_usd(df: pd.DataFrame,
                update_loc: Union[str, PathLike, Engine,
                                  Connection, None] = None,
                save_loc: Union[str, PathLike, Engine,
                                Connection, None] = None,
                only_get: bool = True) -> pd.DataFrame:
    """
    Convert dataframe from UYU to USD.

    Convert a dataframe's columns from Uruguayan pesos to US dollars. Call the
    :func:`econuy.retrieval.nxr.get_monthly` function to obtain nominal
    exchange rates, and take into account whether the input dataframe's
    ``Type``, as defined by its multiindex, is flow or stock, in order to `
    choose end of period or monthly average NXR. Also take into account the
    input dataframe's frequency and whether columns represent rolling averages
    or sums.

    If input dataframe's frequency is higher than monthly (daily, business,
    etc.), resample to monthly frequency.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Input dataframe measured in US dollars : pd.DataFrame

    """
    if df.columns.get_level_values("Moneda")[0] == "USD":
        warnings.warn("Input dataframe already in dollars. No transformations"
                      " made", UserWarning)
        return df

    inferred_freq = pd.infer_freq(df.index)
    if inferred_freq in ["D", "B", "C", "W", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").mean()
        inferred_freq = pd.infer_freq(df.index)

    nxr_data = prices.nxr_monthly(update_loc=update_loc, save_loc=save_loc,
                                  only_get=only_get)

    if df.columns.get_level_values("Tipo")[0] == "Stock":
        metadata._set(nxr_data, ts_type="Stock")
        nxr_freq = resample(nxr_data, target=inferred_freq,
                            operation="average").iloc[:, [1]]
    else:
        metadata._set(nxr_data, ts_type="Flujo")
        nxr_freq = resample(nxr_data, target=inferred_freq,
                            operation="average").iloc[:, [0]]
        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        nxr_freq = rolling(nxr_freq, periods=cum_periods,
                           operation="average")

    nxr_to_use = nxr_freq[nxr_freq.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / nxr_to_use)
    metadata._set(converted_df, currency="USD")

    return converted_df


def convert_real(df: pd.DataFrame, start_date: Union[str, date, None] = None,
                 end_date: Union[str, date, None] = None,
                 update_loc: Union[str, PathLike, Engine,
                                   Connection, None] = None,
                 save_loc: Union[str, PathLike, Engine,
                                 Connection, None] = None,
                 only_get: bool = True) -> pd.DataFrame:
    """
    Convert dataframe to real prices.

    Convert a dataframe's columns to real prices. Call the
    :func:`econuy.retrieval.cpi.get` function to obtain the consumer price
    index. take into account the input dataframe's frequency and whether
    columns represent rolling averages or sums. Allow choosing a single period,
    a range of dates or no period as a base (i.e., period for which the
    average/sum of input dataframe and output dataframe is the same).

    If input dataframe's frequency is higher than monthly (daily, business,
    etc.), resample to monthly frequency.

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
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Input dataframe measured at constant prices : pd.DataFrame

    """
    if "Const" in df.columns.get_level_values("Inf. adj.")[0]:
        warnings.warn("Input dataframe already in real terms. No "
                      "transformations made", UserWarning)
        return df

    inferred_freq = pd.infer_freq(df.index)
    if inferred_freq in ["D", "B", "C", "W", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").mean()
        inferred_freq = pd.infer_freq(df.index)

    cpi_data = prices.cpi(update_loc=update_loc, save_loc=save_loc,
                          only_get=only_get)

    metadata._set(cpi_data, ts_type="Flujo")
    cpi_freq = resample(cpi_data, target=inferred_freq,
                        operation="average").iloc[:, [0]]
    cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
    cpi_to_use = rolling(cpi_freq, periods=cum_periods,
                         operation="average").squeeze()

    if start_date is None:
        converted_df = df.apply(lambda x:
                                x / cpi_to_use)
        col_text = "Const."
    elif end_date is None:
        month = df.iloc[df.index.get_loc(start_date, method="nearest")].name
        converted_df = df.apply(
            lambda x: x / cpi_to_use
            * cpi_to_use.loc[month])
        m_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
        col_text = f"Const. {m_start}"
    else:
        converted_df = df.apply(
            lambda x: x / cpi_to_use * cpi_to_use[start_date:end_date].mean()
        )
        m_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
        m_end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m")
        if m_start == m_end:
            col_text = f"Const. {m_start}"
        else:
            col_text = f"Const. {m_start}_{m_end}"

    converted_df = converted_df.reindex(df.index)
    metadata._set(converted_df, inf_adj=col_text)

    return converted_df


def convert_gdp(df: pd.DataFrame,
                update_loc: Union[str, PathLike, Engine,
                                  Connection, None] = None,
                save_loc: Union[str, PathLike, Engine,
                                Connection, None] = None,
                only_get: bool = True) -> pd.DataFrame:
    """
    Calculate dataframe as percentage of GDP.

    Convert a dataframe's columns to percentage of GDP. Call the
    :func:`econuy.retrieval.national_accounts._lin_gdp` function to obtain UYU
    and USD quarterly GDP series. Take into account the input dataframe's
    currency for chossing UYU or USD GDP. If frequency of input dataframe is
    higher than quarterly, GDP will be upsampled and linear interpolation will
    be performed to complete missing data.

    If input dataframe's "Acum." level is not 12 for monthly frequency or 4
    for quarterly frequency, internally calculate rolling input dataframe.

    If input dataframe's frequency is higher than monthly (daily, business,
    etc.), resample to monthly frequency.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Input dataframe as a percentage of GDP : pd.DataFrame

    Raises
    ------
    ValueError
        If frequency of input dataframe not any of 'D', 'C', 'W', 'B', 'M',
        'MS', 'Q', 'Q-DEC', 'A' or 'A-DEC'.

    """
    if df.columns.get_level_values("Unidad")[0] == "% PBI":
        warnings.warn("Input dataframe already in percent of GDP. No "
                      "transformations made", UserWarning)
        return df

    inferred_freq = pd.infer_freq(df.index)
    gdp = economic_activity._lin_gdp(update_loc=update_loc,
                                     save_loc=save_loc, only_get=only_get)
    cum = df.columns.get_level_values("Acum. períodos")[0]
    if inferred_freq in ["M", "MS"]:
        gdp = resample(gdp, target=inferred_freq,
                       operation="upsample", interpolation="linear")
        if cum != 12 and df.columns.get_level_values("Tipo")[0] == "Flujo":
            converter = int(12 / cum)
            df = rolling(df, periods=converter, operation="sum")
    elif inferred_freq in ["Q", "Q-DEC"]:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()
        if cum != 4 and df.columns.get_level_values("Tipo")[0] == "Flujo":
            converter = int(4 / cum)
            df = rolling(df, periods=converter, operation="sum")
    elif inferred_freq in ["A", "A-DEC"]:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()
    elif inferred_freq in ["D", "B", "C", "W", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").mean()
        gdp = resample(gdp, target="M",
                       operation="upsample", interpolation="linear")
    else:
        raise ValueError("Frequency of input dataframe not any of 'D', 'C', "
                         "'W', 'B', 'M', 'MS', 'Q', 'Q-DEC', 'A' or 'A-DEC'.")

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

    Trim partial bins, i.e. do not calculate the resampled
    period if it is not complete (unless the input dataframe has no defined
    frequency, in which case no trimming is done).

    Parameters
    ----------
    df : Pandas dataframe
        Input dataframe.
    target : str
        Target frequency to resample to. See
        `Pandas offset aliases <https://pandas.pydata.org/pandas-docs/stable/
        user_guide/timeseries.html#offset-aliases>`_
    operation : {'sum', 'average', 'upsample', 'end'}
        Operation to use for resampling.
    interpolation : str, default 'linear'
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
        If input dataframe has ``-`` as type in its metadata, warn and
        set to flow.

    """
    pd_frequencies = {"A": 1,
                      "A-DEC": 1,
                      "Q": 4,
                      "Q-DEC": 4,
                      "M": 12,
                      "W": 52.143,
                      "W-SUN": 52.143,
                      "2W": 26.071,
                      "2W-SUN": 26.071,
                      "B": 240,
                      "D": 365}

    if operation == "sum":
        resampled_df = df.resample(target).sum()
    elif operation == "average":
        resampled_df = df.resample(target).mean()
    elif operation == "end":
        resampled_df = df.resample(target).last()
    elif operation == "upsample":
        resampled_df = df.resample(target).last()
        resampled_df = resampled_df.interpolate(method=interpolation)
    else:
        raise ValueError("Only 'sum', 'average', 'end' and 'upsample' "
                         "are accepted operations")

    cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
    if cum_periods != 1:
        input_notna = df.iloc[:, 0].count()
        output_notna = resampled_df.iloc[:, 0].count()
        cum_adj = round(output_notna / input_notna)
        metadata._set(resampled_df,
                      cumperiods=int(cum_periods * cum_adj))

    if operation in ["sum", "average", "end"]:
        infer_base = pd.infer_freq(df.index)
        try:
            base_freq = pd_frequencies[infer_base]
            target_freq = pd_frequencies[target]
            if target_freq < base_freq:
                count = int(base_freq / target_freq)
                proc = df.resample(target).count()
                proc = proc.loc[proc.iloc[:, 0] >= count]
                resampled_df = resampled_df.reindex(proc.index)
        except KeyError:
            warnings.warn("No bin trimming performed because frequencies "
                          "could not be assigned a numeric value", UserWarning)

    metadata._set(resampled_df)
    resampled_df = resampled_df.dropna(how="all")

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
                      "M": 12,
                      "MS": 12,
                      "W": 52,
                      "W-SUN": 52,
                      "2W": 26,
                      "2W-SUN": 26,
                      "B": 260,
                      "D": 365}

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
        month = df.iloc[df.index.get_loc(start_date, method="nearest")].name
        indexed = df.apply(
            lambda x: x
            / x.loc[month] * base)
        m_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
        metadata._set(indexed, unit=f"{m_start}={base}")

    else:
        indexed = df.apply(lambda x: x / x[start_date:end_date].mean() * base)
        m_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
        m_end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m")
        if m_start == m_end:
            metadata._set(indexed, unit=f"{m_start}={base}")
        else:
            metadata._set(indexed, unit=f"{m_start}_{m_end}={base}")

    return indexed


# The `_open_and_read` function needs to be monkey-patched to specify the
# encoding or decomposition will fail on Windows
def _new_open_and_read(fname):
    with open(fname, 'r', encoding='utf8') as fin:
        fout = fin.read()
    return fout


x13._open_and_read = _new_open_and_read


def decompose(df: pd.DataFrame, flavor: str = "both", method: str = "x13",
              force_x13: bool = False, fallback: str = "loess",
              outlier: bool = True, trading: bool = True,
              x13_binary: Union[str, PathLike, None] = "search",
              search_parents: int = 1, ignore_warnings: bool = True,
              **kwargs) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Apply seasonal decomposition.

    Decompose the series in a Pandas dataframe using either X13 ARIMA, Loess
    or moving averages. X13 can be forced in case of failure by alternating
    the underlying function's parameters. If not, it will fall back to one of
    the other methods. If the X13 method is chosen, the X13 binary has to be
    provided. Please refer to the README for instructions on where to get this
    binary.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    flavor : {'both', 'seas', 'trend'}
        Return both seasonally adjusted and trend dataframes or choose between
        them.
    method : {'x13', 'loess', 'ma'}
        Decomposition method. ``X13`` refers to X13 ARIMA from the US Census,
        ``loess`` refers to Loess decomposition and ``ma`` refers to moving
        average decomposition, in all cases as implemented by
        `statsmodels <https://www.statsmodels.org/dev/tsa.html>`_.
    force_x13 : bool, default False
        Whether to try different ``outlier`` and ``trading`` parameters
        in statsmodels' `x13 arima analysis <https://www.statsmodels.org/dev/
        generated/statsmodels.tsa.x13.x13_arima_analysis.html>`_ for each
        series that fails. If ``False``, jump to the ``fallback`` method for
        the whole dataframe at the first error.
    fallback : {'loess', 'ma'}
        Decomposition method to fall back to if ``method="x13"`` fails and
        ``force_x13=False``.
    trading : bool, default True
        Whether to automatically detect trading days in X13 ARIMA.
    outlier : bool, default True
        Whether to automatically detect outliers in X13 ARIMA.
    x13_binary: str, os.PathLike or None, default 'search'
        Location of the X13 binary. If ``search`` is used, will attempt to find
        the binary in the project structure. If ``None``, Statsmodels will
        handle it.
    search_parents: int, default 1
        If ``x13_binary=search``, this parameter controls how many parent
        directories to go up before recursively searching for the binary.
    ignore_warnings : bool, default True
        Whether to suppress X13Warnings from statsmodels.
    kwargs
        Keyword arguments passed to statsmodels' ``x13_arima_analysis``,
        ``STL`` and ``seasonal_decompose``.

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
    if method not in ["x13", "loess", "ma"]:
        raise ValueError("method can only be 'x13', 'loess' or 'ma'.")
    if fallback not in ["loess", "ma"]:
        raise ValueError("method can only be 'loess' or 'ma'.")

    df_proc = df.copy()
    old_columns = df_proc.columns
    df_proc.columns = df_proc.columns.get_level_values(level=0)
    df_proc.index = pd.to_datetime(df_proc.index, errors="coerce")

    binary_path = None
    if method == "x13":
        if x13_binary == "search":
            search_term = "x13as"
            if platform.system() == "Windows":
                search_term += ".exe"
            binary_path = _rsearch(dir_file=getcwd(), n=search_parents,
                                   search_term=search_term)
        elif isinstance(x13_binary, str):
            binary_path = x13_binary
        elif isinstance(x13_binary, PathLike):
            binary_path = Path(x13_binary).as_posix()
        else:
            binary_path = None
        if isinstance(binary_path, str) and path.isfile(
                binary_path) is False:
            raise ValueError(
                "X13 binary missing. Please refer to the README "
                "for instructions on where to get binaries for "
                "Windows and Unix, and how to compile it for "
                "macOS.")

    if method == "x13":
        try:
            with warnings.catch_warnings():
                if ignore_warnings is True:
                    action = "ignore"
                else:
                    action = "default"
                warnings.filterwarnings(action=action, category=X13Warning)
                results = df_proc.apply(
                    lambda x: x13a(x.dropna(), outlier=outlier,
                                   trading=trading, x12path=binary_path,
                                   prefer_x13=True, **kwargs)
                )
            trends = results.apply(lambda x: x.trend.reindex(df_proc.index)).T
            seas_adjs = results.apply(lambda x: x.seasadj.
                                      reindex(df_proc.index)).T

        except X13Error:
            if force_x13 is True:
                if outlier is True:
                    try:
                        warnings.warn("X13 error found with selected "
                                      "parameters. Trying with outlier=False.",
                                      UserWarning)
                        return decompose(df=df, method=method,
                                         outlier=False,
                                         flavor=flavor, fallback=fallback,
                                         force_x13=force_x13,
                                         x13_binary=x13_binary,
                                         search_parents=search_parents,
                                         **kwargs)
                    except X13Error:
                        try:
                            warnings.warn("X13 error found with trading=True. "
                                          "Trying with trading=False.",
                                          UserWarning)
                            return decompose(df=df, method=method,
                                             outlier=False, trading=False,
                                             flavor=flavor,
                                             fallback=fallback,
                                             force_x13=force_x13,
                                             x13_binary=x13_binary,
                                             search_parents=search_parents,
                                             **kwargs)
                        except X13Error:
                            warnings.warn("No combination of parameters "
                                          "successful. Filling with NaN.",
                                          UserWarning)
                            trends = pd.DataFrame(
                                data=np.full(df_proc.shape, np.nan),
                                index=df_proc.index, columns=df_proc.columns
                            )
                            seas_adjs = trends.copy()

                elif trading is True:
                    try:
                        warnings.warn("X13 error found with trading=True. "
                                      "Trying with trading=False...",
                                      UserWarning)
                        return decompose(df=df, method=method,
                                         trading=False, flavor=flavor,
                                         fallback=fallback,
                                         force_x13=force_x13,
                                         x13_binary=x13_binary,
                                         search_parents=search_parents,
                                         **kwargs)
                    except X13Error:
                        warnings.warn("No combination of parameters "
                                      "successful. Filling with NaN.",
                                      UserWarning)
                        trends = pd.DataFrame(
                            data=np.full(df_proc.shape, np.nan),
                            index=df_proc.index, columns=df_proc.columns
                        )
                        seas_adjs = trends.copy()

            else:
                if fallback == "loess":
                    results = df_proc.apply(
                        lambda x: STL(x.dropna()).fit(), result_type="expand")
                elif fallback == "ma":
                    results = df_proc.apply(
                        lambda x: seasonal_decompose(
                            x.dropna(), extrapolate_trend="freq"),
                        result_type="expand")
                trends = results.apply(lambda x:
                                       x.trend.reindex(df_proc.index)).T
                seas_adjs = results.apply(
                    lambda x: (x.observed
                               - x.seasonal).reindex(df_proc.index)).T

    else:
        if method == "loess":
            results = df_proc.apply(
                lambda x: STL(x.dropna()).fit(), result_type="expand")
        if method == "ma":
            results = df_proc.apply(
                lambda x: seasonal_decompose(x.dropna(),
                                             extrapolate_trend="freq"),
                result_type="expand")
        trends = results.apply(lambda x:
                               x.trend.reindex(df_proc.index)).T
        seas_adjs = results.apply(
            lambda x: (x.observed - x.seasonal).reindex(df_proc.index)).T

    trends.columns = old_columns
    seas_adjs.columns = old_columns
    metadata._set(trends, seas_adj="Tendencia")
    metadata._set(seas_adjs, seas_adj="SA")
    output = pd.DataFrame()
    if flavor == "both":
        output = (trends, seas_adjs)
    elif flavor == "seas":
        output = seas_adjs
    elif flavor == "trend":
        output = trends

    return output


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
                    "diff": [lambda x: x.diff(periods=1), "Cambio"]},
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
