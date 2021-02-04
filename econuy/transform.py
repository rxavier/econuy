import platform
import warnings
from datetime import datetime
from os import PathLike, getcwd, path
from pathlib import Path
from typing import Union, Optional, Tuple, Dict

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
                only_get: bool = True, errors: str = "raise") -> pd.DataFrame:
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
    errors : {'raise', 'coerce', 'ignore'}
        What to do when a column in the input dataframe is not expressed in
        Uruguayan pesos. ``raise`` will raise a ValueError, ``coerce`` will
        force the entire column into ``np.nan`` and ``ignore`` will leave the
        input column as is.

    Returns
    -------
    Input dataframe measured in US dollars : pd.DataFrame

    Raises
    ------
    ValueError
        If the ``errors`` parameter does not have a valid argument.
    ValueError
        If the input dataframe's columns do not have the appropiate levels.

    """
    if errors not in ["raise", "coerce", "ignore"]:
        raise ValueError("'errors' must be one of 'raise', "
                         "'coerce' or 'ignore'.")
    if "Moneda" not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the "
                         "'Moneda' level.")

    checks = [x == "UYU" for x in df.columns.get_level_values("Moneda")]
    if any(checks):
        if not all(checks) and errors == "raise":
            error_df = df.loc[:, [not check for check in checks]]
            msg = (f"{error_df.columns[0][0]} does not have the "
                   f"appropiate metadata.")
            return error_handler(df=df, errors=errors, msg=msg)
        nxr_data = prices.nxr_monthly(update_loc=update_loc, save_loc=save_loc,
                                      only_get=only_get)
        all_metadata = df.columns.droplevel("Indicador")
        if all(x == all_metadata[0] for x in all_metadata):
            return _convert_usd(df=df, nxr=nxr_data)
        else:
            columns = []
            for column_name, check in zip(df.columns, checks):
                df_column = df[[column_name]]
                if check is False:
                    msg = (f"{column_name[0]} does not have the " 
                           f"appropiate metadata.")
                    columns.append(error_handler(df=df_column, errors=errors,
                                                 msg=msg))
                else:
                    converted = _convert_usd(df=df_column, nxr=nxr_data)
                    columns.append(converted)
            return pd.concat(columns, axis=1)
    else:
        return error_handler(df=df, errors=errors)


def _convert_usd(df: pd.DataFrame,
                 nxr: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if nxr is None:
        nxr = prices.nxr_monthly()

    inferred_freq = pd.infer_freq(df.index)
    if inferred_freq in ["D", "B", "C", "W", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").mean()
        inferred_freq = pd.infer_freq(df.index)

    if df.columns.get_level_values("Tipo")[0] == "Stock":
        metadata._set(nxr, ts_type="Stock")
        nxr_freq = resample(nxr, rule=inferred_freq,
                            operation="mean").iloc[:, [1]]
    else:
        metadata._set(nxr, ts_type="Flujo")
        nxr_freq = resample(nxr, rule=inferred_freq,
                            operation="mean").iloc[:, [0]]
        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        nxr_freq = rolling(nxr_freq, window=cum_periods,
                           operation="mean")

    nxr_to_use = nxr_freq.reindex(df.index).iloc[:, 0]
    converted_df = df.div(nxr_to_use, axis=0)
    metadata._set(converted_df, currency="USD")

    return converted_df


def convert_real(df: pd.DataFrame,
                 start_date: Union[str, datetime, None] = None,
                 end_date: Union[str, datetime, None] = None,
                 update_loc: Union[str, PathLike, Engine,
                                   Connection, None] = None,
                 save_loc: Union[str, PathLike, Engine,
                                 Connection, None] = None,
                 only_get: bool = True,
                 errors: str = "raise") -> pd.DataFrame:
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
    errors : {'raise', 'coerce', 'ignore'}
        What to do when a column in the input dataframe is not expressed in
        nominal Uruguayan pesos. ``raise`` will raise a ValueError, ``coerce``
        will force the entire column into ``np.nan`` and ``ignore`` will leave
        the input column as is.

    Returns
    -------
    Input dataframe measured at constant prices : pd.DataFrame

    Raises
    ------
    ValueError
        If the ``errors`` parameter does not have a valid argument.
    ValueError
        If the input dataframe's columns do not have the appropiate levels.

    """
    if errors not in ["raise", "coerce", "ignore"]:
        raise ValueError("'errors' must be one of 'raise', "
                         "'coerce' or 'ignore'.")
    if "Inf. adj." not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the "
                         "'Inf. adj.' level.")

    checks = [x == "UYU" and "Const." not in y
              for x, y in zip(df.columns.get_level_values("Moneda"),
                              df.columns.get_level_values("Inf. adj."))]
    if any(checks):
        if not all(checks) and errors == "raise":
            error_df = df.loc[:, [not check for check in checks]]
            msg = (f"{error_df.columns[0][0]} does not have the "
                   f"appropiate metadata.")
            return error_handler(df=df, errors=errors, msg=msg)
        cpi_data = prices.cpi(update_loc=update_loc, save_loc=save_loc,
                              only_get=only_get)
        all_metadata = df.columns.droplevel("Indicador")
        if all(x == all_metadata[0] for x in all_metadata):
            return _convert_real(df=df, start_date=start_date,
                                 end_date=end_date, cpi=cpi_data)
        else:
            columns = []
            for column_name, check in zip(df.columns, checks):
                df_column = df[[column_name]]
                if check is False:
                    msg = (f"{column_name[0]} does not have the " 
                           f"appropiate metadata.")
                    columns.append(error_handler(df=df_column, errors=errors,
                                                 msg=msg))
                else:
                    converted = _convert_real(df=df_column,
                                              start_date=start_date,
                                              end_date=end_date, cpi=cpi_data)
                    columns.append(converted)
            return pd.concat(columns, axis=1)
    else:
        return error_handler(df=df, errors=errors)


def _convert_real(df: pd.DataFrame,
                  start_date: Union[str, datetime, None] = None,
                  end_date: Union[str, datetime, None] = None,
                  cpi: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if cpi is None:
        cpi = prices.cpi()

    inferred_freq = pd.infer_freq(df.index)
    if inferred_freq in ["D", "B", "C", "W", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").mean()
        inferred_freq = pd.infer_freq(df.index)

    metadata._set(cpi, ts_type="Flujo")
    cpi_freq = resample(cpi, rule=inferred_freq,
                        operation="mean").iloc[:, [0]]
    cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
    cpi_to_use = rolling(cpi_freq, window=cum_periods,
                         operation="mean").squeeze()

    if start_date is None:
        converted_df = df.div(cpi_to_use, axis=0)
        col_text = "Const."
    elif end_date is None:
        month = df.iloc[df.index.get_loc(start_date, method="nearest")].name
        converted_df = df.div(cpi_to_use, axis=0) * cpi_to_use.loc[month]
        m_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
        col_text = f"Const. {m_start}"
    else:
        converted_df = df.div(cpi_to_use, axis=0) * cpi_to_use[start_date:
                                                               end_date].mean()
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
                only_get: bool = True,
                errors: str = "raise") -> pd.DataFrame:
    """
    Calculate dataframe as percentage of GDP.

    Convert a dataframe's columns to percentage of GDP. Call the
    :func:`econuy.retrieval.national_accounts._lin_gdp` function to obtain UYU
    and USD quarterly GDP series. Take into account the input dataframe's
    currency for chossing UYU or USD GDP. If frequency of input dataframe is
    higher than quarterly, GDP will be upsampled and linear interpolation will
    be performed to complete missing data.

    If input dataframe's "Acum." level is not 12 for monthly frequency or 4
    for quarterly frequency, calculate rolling input dataframe.

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
    errors : {'raise', 'coerce', 'ignore'}
        What to do when a column in the input dataframe does not refer to
        Uruguayan data or is already in % of GDP. ``raise`` will raise a
        ValueError, ``coerce`` will force the entire column into ``np.nan`` and
        ``ignore`` will leave the input column as is.

    Returns
    -------
    Input dataframe as a percentage of GDP : pd.DataFrame

    Raises
    ------
    ValueError
        If the ``method`` parameter does not have a valid argument.
    ValueError
        If the input dataframe's columns do not have the appropiate levels.

    """
    if errors not in ["raise", "coerce", "ignore"]:
        raise ValueError("'errors' must be one of 'raise', 'coerce' or "
                         "'ignore'.")
    if any(x not in df.columns.names for x in ["Área", "Unidad"]):
        raise ValueError("Input dataframe's multiindex requires the 'Área' "
                         "and 'Unidad' levels.")

    checks = [x not in ["Regional", "Global"] and "%PBI" not in y
              for x, y in zip(df.columns.get_level_values("Área"),
                              df.columns.get_level_values("Unidad"))]
    if any(checks):
        if not all(checks) and errors == "raise":
            error_df = df.loc[:, [not check for check in checks]]
            msg = (f"{error_df.columns[0][0]} does not have the "
                   f"appropiate metadata.")
            return error_handler(df=df, errors=errors, msg=msg)
        gdp_data = economic_activity._lin_gdp(update_loc=update_loc,
                                              save_loc=save_loc,
                                              only_get=only_get)
        all_metadata = df.columns.droplevel("Indicador")
        if all(x == all_metadata[0] for x in all_metadata):
            return _convert_gdp(df=df, gdp=gdp_data)
        else:
            columns = []
            for column_name, check in zip(df.columns, checks):
                df_column = df[[column_name]]
                if check is False:
                    msg = (f"{column_name[0]} does not have the " 
                           f"appropiate metadata.")
                    columns.append(error_handler(df=df_column, errors=errors,
                                                 msg=msg))
                else:
                    converted = _convert_gdp(df=df_column, gdp=gdp_data)
                    columns.append(converted)
            return pd.concat(columns, axis=1)
    else:
        return error_handler(df=df, errors=errors)


def _convert_gdp(df: pd.DataFrame,
                 gdp: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if gdp is None:
        gdp = economic_activity._lin_gdp()

    inferred_freq = pd.infer_freq(df.index)
    cum = df.columns.get_level_values("Acum. períodos")[0]
    if inferred_freq in ["M", "MS"]:
        gdp = resample(gdp, rule=inferred_freq,
                       operation="upsample", interpolation="linear")
        if cum != 12 and df.columns.get_level_values("Tipo")[0] == "Flujo":
            converter = int(12 / cum)
            df = rolling(df, window=converter, operation="sum")
    elif inferred_freq in ["Q", "Q-DEC"]:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()
        if cum != 4 and df.columns.get_level_values("Tipo")[0] == "Flujo":
            converter = int(4 / cum)
            df = rolling(df, window=converter, operation="sum")
    elif inferred_freq in ["A", "A-DEC"]:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()
    elif inferred_freq in ["D", "B", "C", "W", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").mean()
        gdp = resample(gdp, rule="M",
                       operation="upsample", interpolation="linear")
    else:
        raise ValueError("Frequency of input dataframe not any of 'D', 'C', "
                         "'W', 'B', 'M', 'MS', 'Q', 'Q-DEC', 'A' or 'A-DEC'.")

    if df.columns.get_level_values("Moneda")[0] == "USD":
        gdp = gdp.iloc[:, 1].to_frame()
    else:
        gdp = gdp.iloc[:, 0].to_frame()

    gdp_to_use = gdp.reindex(df.index).iloc[:, 0]
    converted_df = df.div(gdp_to_use, axis=0).multiply(100)

    metadata._set(converted_df, unit="% PBI")

    return converted_df


def resample(df: pd.DataFrame, rule: Union[pd.DateOffset, pd.Timedelta, str],
             operation: str = "sum",
             interpolation: str = "linear") -> pd.DataFrame:
    """
    Wrapper for the `resample method <https://pandas.pydata.org/pandas-docs
    stable/reference/api/pandas.DataFrame.resample.html>`_ in Pandas that
    integrates with econuy dataframes' metadata.

    Trim partial bins, i.e. do not calculate the resampled
    period if it is not complete, unless the input dataframe has no defined
    frequency, in which case no trimming is done.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    rule : pd.DateOffset, pd.Timedelta or str
        Target frequency to resample to. See
        `Pandas offset aliases <https://pandas.pydata.org/pandas-docs/stable/
        user_guide/timeseries.html#offset-aliases>`_
    operation : {'sum', 'mean', 'last', 'upsample'}
        Operation to use for resampling.
    interpolation : str, default 'linear'
        Method to use when missing data are produced as a result of
        resampling, for example when upsampling to a higher frequency. See
        `Pandas interpolation methods <https://pandas.pydata.org/pandas-docs
        /stable/reference/api/pandas.Series.interpolate.html>`_

    Returns
    -------
    Input dataframe at the frequency defined in ``rule`` : pd.DataFrame

    Raises
    ------
    ValueError
        If ``operation`` is not one of available options.
    ValueError
        If the input dataframe's columns do not have the appropiate levels.

    Warns
    -----
    UserWarning
        If input frequencies cannot be assigned a numeric value, preventing
        incomplete bin trimming.

    """
    if operation not in ["sum", "mean", "upsample", "last"]:
        raise ValueError("Invalid 'operation' option.")
    if "Acum. períodos" not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the "
                         "'Acum. períodos' level.")

    all_metadata = df.columns.droplevel("Indicador")
    if all(x == all_metadata[0] for x in all_metadata):
        return _resample(df=df, rule=rule, operation=operation,
                         interpolation=interpolation)
    else:
        columns = []
        for column_name in df.columns:
            df_column = df[[column_name]]
            converted = _resample(df=df_column, rule=rule, operation=operation,
                                  interpolation=interpolation)
            columns.append(converted)
        return pd.concat(columns, axis=1)


def _resample(df: pd.DataFrame, rule: Union[pd.DateOffset, pd.Timedelta, str],
              operation: str = "sum",
              interpolation: str = "linear") -> pd.DataFrame:
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
        resampled_df = df.resample(rule).sum()
    elif operation == "mean":
        resampled_df = df.resample(rule).mean()
    elif operation == "last":
        resampled_df = df.resample(rule).last()
    else:
        resampled_df = df.resample(rule).last()
        resampled_df = resampled_df.interpolate(method=interpolation)

    cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
    if cum_periods != 1:
        input_notna = df.iloc[:, 0].count()
        output_notna = resampled_df.iloc[:, 0].count()
        cum_adj = round(output_notna / input_notna)
        metadata._set(resampled_df,
                      cumperiods=int(cum_periods * cum_adj))

    if operation in ["sum", "mean", "last"]:
        infer_base = pd.infer_freq(df.index)
        try:
            base_freq = pd_frequencies[infer_base]
            target_freq = pd_frequencies[rule]
            if target_freq < base_freq:
                count = int(base_freq / target_freq)
                proc = df.resample(rule).count()
                antimask = np.where(proc >= count, False, True)
                resampled_df = resampled_df.mask(antimask, np.nan)
        except KeyError:
            warnings.warn("No bin trimming performed because frequencies "
                          "could not be assigned a numeric value", UserWarning)

    metadata._set(resampled_df)
    resampled_df = resampled_df.dropna(how="all")

    return resampled_df


def rolling(df: pd.DataFrame, window: Optional[int] = None,
            operation: str = "sum") -> pd.DataFrame:
    """
    Wrapper for the `rolling method <https://pandas.pydata.org/pandas-docs/
    stable/reference/api/pandas.DataFrame.rolling.html>`_ in Pandas that
    integrates with econuy dataframes' metadata.

    If ``periods`` is ``None``, try to infer the frequency and set ``periods``
    according to the following logic: ``{'A': 1, 'Q-DEC': 4, 'M': 12}``, that
    is, each period will be calculated as the sum or mean of the last year.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    window : int, default None
        How many periods the window should cover.
    operation : {'sum', 'mean'}
        Operation used to calculate rolling windows.

    Returns
    -------
    Input dataframe with rolling windows : pd.DataFrame

    Raises
    ------
    ValueError
        If ``operation`` is not one of available options.
    ValueError
        If the input dataframe's columns do not have the appropiate levels.

    Warns
    -----
    UserWarning
        If the input dataframe is a stock time series, for which rolling
        operations are not recommended.

    """
    if operation not in ["sum", "mean"]:
        raise ValueError("Invalid 'operation' option.")
    if "Tipo" not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the "
                         "'Tipo' level.")

    all_metadata = df.columns.droplevel("Indicador")
    if all(x == all_metadata[0] for x in all_metadata):
        return _rolling(df=df, window=window, operation=operation)
    else:
        columns = []
        for column_name in df.columns:
            df_column = df[[column_name]]
            converted = _rolling(df=df_column, window=window,
                                 operation=operation)
            columns.append(converted)
        return pd.concat(columns, axis=1)


def _rolling(df: pd.DataFrame, window: Optional[int] = None,
             operation: str = "sum") -> pd.DataFrame:
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

    window_operation = {"sum": lambda x: x.rolling(window=window,
                                                   min_periods=window).sum(),
                        "mean": lambda x: x.rolling(window=window,
                                                    min_periods=window).mean()}

    if df.columns.get_level_values("Tipo")[0] == "Stock":
        warnings.warn("Rolling operations should not be "
                      "calculated on stock variables", UserWarning)

    if window is None:
        inferred_freq = pd.infer_freq(df.index)
        window = pd_frequencies[inferred_freq]

    rolling_df = df.apply(window_operation[operation])

    metadata._set(rolling_df, cumperiods=window)

    return rolling_df


def rebase(df: pd.DataFrame, start_date: Union[str, datetime],
           end_date: Union[str, datetime, None] = None,
           base: float = 100.0) -> pd.DataFrame:
    """Rebase all dataframe columns to a date or range of dates.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    start_date : string or datetime.datetime
        Date to which series will be rebased.
    end_date : string or datetime.datetime, default None
        If specified, series will be rebased to the average between
        ``start_date`` and ``end_date``.
    base : float, default 100
        Float for which ``start_date`` == ``base`` or average between
        ``start_date`` and ``end_date`` == ``base``.

    Returns
    -------
    Input dataframe with a base period index : pd.DataFrame

    """
    all_metadata = df.columns.droplevel("Indicador")
    if all(x == all_metadata[0] for x in all_metadata):
        return _rebase(df=df, end_date=end_date,
                       start_date=start_date, base=base)
    else:
        columns = []
        for column_name in df.columns:
            df_column = df[[column_name]]
            converted = _rebase(df=df_column, end_date=end_date,
                                start_date=start_date, base=base)
            columns.append(converted)
        return pd.concat(columns, axis=1)


def _rebase(df: pd.DataFrame, start_date: Union[str, datetime],
            end_date: Union[str, datetime, None] = None,
            base: float = 100.0) -> pd.DataFrame:
    if end_date is None:
        start_date = df.iloc[df.index.get_loc(start_date,
                                              method="nearest")].name
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


# The `_open_and_read` function needs to be monkey-patched to specify the
# encoding or decomposition will fail on Windows
def _new_open_and_read(fname):
    with open(fname, 'r', encoding='utf8') as fin:
        fout = fin.read()
    return fout


x13._open_and_read = _new_open_and_read


def decompose(df: pd.DataFrame, component: str = "both", method: str = "x13",
              force_x13: bool = False, fallback: str = "loess",
              outlier: bool = True, trading: bool = True,
              x13_binary: Union[str, PathLike, None] = "search",
              search_parents: int = 1, ignore_warnings: bool = True,
              errors: str = "raise",
              **kwargs) -> Union[Dict[str, pd.DataFrame],
                                 pd.DataFrame]:
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
    component : {'both', 'seas', 'trend'}
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
        the binary in the project structure. If ``None``, statsmodels will
        handle it.
    search_parents: int, default 1
        If ``x13_binary=search``, this parameter controls how many parent
        directories to go up before recursively searching for the binary.
    ignore_warnings : bool, default True
        Whether to suppress X13Warnings from statsmodels.
    errors : {'raise', 'coerce', 'ignore'}
        What to do when a column in the input dataframe is already seasonally
        adjusted. ``raise`` will raise a ValueError, ``coerce`` will force the
        entire column into ``np.nan`` and ``ignore`` will leave the input
        column as is.
    kwargs
        Keyword arguments passed to statsmodels' ``x13_arima_analysis``,
        ``STL`` and ``seasonal_decompose``.

    Returns
    -------
    Decomposed dataframes : Dict[str, pd.DataFrame] or pd.DataFrame
        Dictionary containing the trend component and the seasonally adjusted
        series, or Pandas dataframe containing the chosen component.

    Raises
    ------
    ValueError
        If the ``method`` parameter does not have a valid argument.
    ValueError
        If the ``component`` parameter does not have a valid argument.
    ValueError
        If the ``fallback`` parameter does not have a valid argument.
    ValueError
        If the ``errors`` parameter does not have a valid argument.
    FileNotFoundError
        If the path provided for the X13 binary does not point to a file and
        ``method='x13'``.

    """
    if errors not in ["raise", "coerce", "ignore"]:
        raise ValueError("method can only be 'x13', 'loess' or 'ma'.")
    if method not in ["x13", "loess", "ma"]:
        raise ValueError("method can only be 'x13', 'loess' or 'ma'.")
    if fallback not in ["loess", "ma"]:
        raise ValueError("method can only be 'loess' or 'ma'.")
    if component not in ["trend", "seas", "both"]:
        raise ValueError("component can only be 'trend', 'seas' or 'both'.")
    if "Seas. Adj." not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the "
                         "'Seas. Adj.' level.")

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
            raise FileNotFoundError(
                "X13 binary missing. Please refer to the README "
                "for instructions on where to get binaries for "
                "Windows and Unix, and how to compile it for "
                "macOS.")

    checks = [x not in ["Tendencia", "SA"]
              for x in df.columns.get_level_values("Seas. Adj.")]
    passing = df.loc[:, checks]
    not_passing = df.loc[:, [not x for x in checks]]
    if any(checks):
        if not all(checks) and errors == "raise":
            error_df = df.loc[:, [not check for check in checks]]
            msg = (f"{error_df.columns[0][0]} does not have the "
                   f"appropiate metadata.")
            return error_handler(df=df, errors=errors, msg=msg)
        passing_output = _decompose(passing, component=component,
                                    method=method, force_x13=force_x13,
                                    fallback=fallback, outlier=outlier,
                                    trading=trading, x13_binary=binary_path,
                                    ignore_warnings=ignore_warnings,
                                    errors=errors, **kwargs)
        if not_passing.shape[1] != 0:
            not_passing_output = error_handler(df=not_passing, errors=errors)
        else:
            not_passing_output = not_passing
        if isinstance(passing_output, pd.DataFrame):
            output = pd.concat([passing_output, not_passing_output], axis=1)
            output = output[df.columns.get_level_values(0)]
            return output
        elif isinstance(passing_output, Dict):
            output = {}
            for name, data in passing_output.items():
                aux = pd.concat([data, not_passing_output], axis=1)
                output[name] = aux[df.columns.get_level_values(0)]
            return output
    else:
        return error_handler(df=df, errors=errors)


def _decompose(df: pd.DataFrame, component: str = "both", method: str = "x13",
               force_x13: bool = False, fallback: str = "loess",
               outlier: bool = True, trading: bool = True,
               x13_binary: Union[str, PathLike, None] = None,
               ignore_warnings: bool = True, errors: str = "raise",
               **kwargs) -> Union[Tuple[pd.DataFrame, pd.DataFrame],
                                  pd.DataFrame]:
    if method not in ["x13", "loess", "ma"]:
        raise ValueError("method can only be 'x13', 'loess' or 'ma'.")
    if fallback not in ["loess", "ma"]:
        raise ValueError("method can only be 'loess' or 'ma'.")

    df_proc = df.copy()
    old_columns = df_proc.columns
    df_proc.columns = df_proc.columns.get_level_values(level=0)
    df_proc.index = pd.to_datetime(df_proc.index, errors="coerce")
    trends = pd.DataFrame(data=np.nan, index=df_proc.index, 
                          columns=old_columns)
    seas_adjs = trends.copy()

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
                                   trading=trading, x12path=x13_binary,
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
                                         component=component,
                                         fallback=fallback,
                                         force_x13=force_x13,
                                         x13_binary=x13_binary,
                                         **kwargs)
                    except X13Error:
                        try:
                            warnings.warn("X13 error found with trading=True. "
                                          "Trying with trading=False.",
                                          UserWarning)
                            return decompose(df=df, method=method,
                                             outlier=False, trading=False,
                                             component=component,
                                             fallback=fallback,
                                             force_x13=force_x13,
                                             x13_binary=x13_binary,
                                             **kwargs)
                        except X13Error:
                            warnings.warn("No combination of parameters "
                                          "successful. No decomposition "
                                          "performed.",
                                          UserWarning)
                            trends = error_handler(df=df_proc, errors=errors)
                            seas_adjs = trends.copy()

                elif trading is True:
                    try:
                        warnings.warn("X13 error found with trading=True. "
                                      "Trying with trading=False...",
                                      UserWarning)
                        return decompose(df=df, method=method,
                                         trading=False, component=component,
                                         fallback=fallback,
                                         force_x13=force_x13,
                                         x13_binary=x13_binary,
                                         **kwargs)
                    except X13Error:
                        warnings.warn("No combination of parameters "
                                      "successful. Filling with NaN.",
                                      UserWarning)
                        trends = error_handler(df=df_proc, errors=errors)
                        seas_adjs = trends.copy()

            else:
                if fallback == "loess":
                    results = df_proc.apply(
                        lambda x: STL(x.dropna()).fit(), result_type="expand")
                else:
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
        else:
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
    if component == "both":
        output = {"trend": trends, "seas": seas_adjs}
    elif component == "seas":
        output = seas_adjs
    elif component == "trend":
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
             period: str = "last") -> pd.DataFrame:
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
    period : {'last', 'inter', 'annual'}
        Period with which to calculate change or difference. ``last`` for
        previous period (last month for monthly data), ``inter`` for same
        period last year, ``annual`` for same period last year but taking
        annual sums.

    Returns
    -------
    Percent change or differences dataframe : pd.DataFrame

    Raises
    ------
    ValueError
        If the dataframe is not of frequency ``M`` (month), ``Q`` or
        ``Q-DEC`` (quarter), or ``A`` or ``A-DEC`` (year).
    ValueError
        If the ``operation`` parameter does not have a valid argument.
    ValueError
        If the ``period`` parameter does not have a valid argument.
    ValueError
        If the input dataframe's columns do not have the appropiate levels.

    """
    if operation not in ["chg", "diff"]:
        raise ValueError("Invalid 'operation' option.")
    if period not in ["last", "inter", "annual"]:
        raise ValueError("Invalid 'period' option.")
    if "Tipo" not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the "
                         "'Tipo' level.")

    all_metadata = df.columns.droplevel("Indicador")
    if all(x == all_metadata[0] for x in all_metadata):
        return _chg_diff(df=df, operation=operation, period=period)
    else:
        columns = []
        for column_name in df.columns:
            df_column = df[[column_name]]
            converted = _chg_diff(df=df_column, operation=operation,
                                  period=period)
            columns.append(converted)
        return pd.concat(columns, axis=1)


def _chg_diff(df: pd.DataFrame, operation: str = "chg",
              period: str = "last") -> pd.DataFrame:
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

    if period == "annual":

        if df.columns.get_level_values("Tipo")[0] == "Stock":
            output = df.apply(type_change[period][operation][0])
        else:
            output = rolling(df, operation="sum")
            output = output.apply(
                type_change[period][operation][0])

        metadata._set(output, unit=type_change[period][operation][1])

    else:
        output = df.apply(type_change[period][operation][0])
        metadata._set(output, unit=type_change[period][operation][1])

    if operation == "chg":
        output = output.multiply(100)

    return output


def error_handler(df: pd.DataFrame, errors: str,
                  msg: str = None) -> pd.DataFrame:
    if errors == "coerce":
        return pd.DataFrame(data=np.nan, index=df.index, columns=df.columns)
    elif errors == "ignore":
        return df
    elif errors == "raise":
        if msg is None:
            msg = ""
        raise ValueError(msg)
