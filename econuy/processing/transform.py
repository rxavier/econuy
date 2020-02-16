from datetime import date
from os import PathLike
from typing import Union, Optional

import pandas as pd

from econuy.processing import freqs
from econuy.resources import columns
from econuy.retrieval import cpi, national_accounts, nxr


def convert_usd(df: pd.DataFrame,
                update: Union[str, PathLike, None] = None,
                save: Union[str, PathLike, None] = None) -> pd.DataFrame:
    """
    Convert dataframe from UYU to USD.

    Convert a dataframe's columns from Uruguayan pesos to US dollars. Call the
    :func:`econuy.retrieval.nxr.get` function to obtain nominal
    exchange rates, and take into account whether the input dataframe's
    ``Type``, as defined by its multiindex, is flow or stock, in order to `
    choose end of period or monthly average NXR. Also take into account the
    input dataframe's frequency and whether columns represent rolling averages
    or sums.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.

    Returns
    -------
    Input dataframe measured in US dollars : pd.DataFrame

    """
    inferred_freq = pd.infer_freq(df.index)
    nxr_data = nxr.get(update=update, revise_rows=6,
                       save=save, force_update=False)

    if df.columns.get_level_values("Tipo")[0] == "Flujo":
        columns._setmeta(nxr_data, ts_type="Flujo")
        nxr_freq = freq_resample(nxr_data, target=inferred_freq,
                                 operation="average").iloc[:, [1]]
        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        nxr_freq = rolling(nxr_freq, periods=cum_periods,
                           operation="average")

    else:
        columns._setmeta(nxr_data, ts_type="Stock")
        nxr_freq = freq_resample(nxr_data, target=inferred_freq,
                                 operation="average").iloc[:, [3]]

    nxr_to_use = nxr_freq[nxr_freq.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / nxr_to_use)
    columns._setmeta(converted_df, currency="USD")

    return converted_df


def convert_real(df: pd.DataFrame, start_date: Union[str, date, None] = None,
                 end_date: Union[str, date, None] = None,
                 update: Union[str, PathLike, None] = None,
                 save: Union[str, PathLike, None] = None) -> pd.DataFrame:
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
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.

    Returns
    -------
    Input dataframe measured at constant prices : pd.DataFrame

    """
    inferred_freq = pd.infer_freq(df.index)
    cpi_data = cpi.get(update=update, revise_rows=6, save=save,
                       force_update=False)
    columns._setmeta(cpi_data, ts_type="Flujo")
    cpi_freq = freq_resample(cpi_data, target=inferred_freq,
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

    columns._setmeta(converted_df, inf_adj=col_text)

    return converted_df


def convert_gdp(df: pd.DataFrame, hifreq: bool = True,
                update: Union[str, PathLike, None] = None,
                save: Union[str, PathLike, None] = None) -> pd.DataFrame:
    """
    Calculate dataframe as percentage of GDP.

    Convert a dataframe's columns to percentage of GDP. Call the
    :func:`econuy.retrieval.national_accounts._lin_gdp` function to obtain UYU
    and USD quarterly GDP series. Take into account the input dataframe's
    currency for chossing UYU or USD GDP. If ``hifreq`` is set to ``True``,
    GDP will be upsampled and linear interpolation will be performed to
    complete missing data.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    hifreq : bool, default True
        If True, the input dataframe's frequency is assumed to be 'higher' than
        quarterly (``Q`` or ``Q-DEC``) and will trigger GDP upsampling.
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.

    Returns
    -------
    Input dataframe as a percentage of GDP : pd.DataFrame

    """
    inferred_freq = pd.infer_freq(df.index)
    gdp = national_accounts._lin_gdp(update=update,
                                     save=save, force_update=False)

    if hifreq is True:
        gdp = freq_resample(gdp, target=inferred_freq,
                            operation="upsample", interpolation="linear")
    else:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()

    if df.columns.get_level_values("Unidad/Moneda")[0] == "USD":
        gdp = gdp.iloc[:, 1].to_frame()
    else:
        gdp = gdp.iloc[:, 0].to_frame()

    gdp_to_use = gdp[gdp.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / gdp_to_use).multiply(100)

    columns._setmeta(converted_df, currency="% PBI")

    return converted_df


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
    pd_frequencies = {"A": 1,
                      "Q-DEC": 4,
                      "M": 12}

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
        periods = pd_frequencies[inferred_freq]

    rolling_df = df.apply(window_operation[operation])

    columns._setmeta(rolling_df, cumperiods=periods)

    return rolling_df