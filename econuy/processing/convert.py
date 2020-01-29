from datetime import date
from typing import Union

import pandas as pd

from econuy.retrieval import cpi, national_accounts, nxr
from econuy.processing import freqs, columns


def usd(df: pd.DataFrame):
    """Convert dataframe from UYU to USD.

    Convert a dataframe's columns from Uruguayan pesos to US dollars. Call the
    `nxr.get()` function to obtain nominal exchange rates, and take into
    account whether the input dataframe's `Type`, as defined by its multiindex,
    is flow or stock, in order to choose end of period or monthly average NXR.
    Also take into account the input dataframe's frequency and whether columns
    represent rolling averages or sums.

    Parameters
    ----------
    df : Pandas dataframe

    Returns
    -------
    converted_df : Pandas dataframe

    """
    inferred_freq = pd.infer_freq(df.index)
    nxr_data = nxr.get(update="nxr.csv", revise_rows=6,
                       save="nxr.csv", force_update=False)

    if df.columns.get_level_values("Tipo")[0] == "Flujo":
        columns.set_metadata(nxr_data, ts_type="Flujo")
        nxr_freq = freqs.freq_resample(nxr_data, target=inferred_freq,
                                       operation="average").iloc[:, [1]]
        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        nxr_freq = freqs.rolling(nxr_freq, periods=cum_periods,
                                 operation="average")

    else:
        columns.set_metadata(nxr_data, ts_type="Stock")
        nxr_freq = freqs.freq_resample(nxr_data, target=inferred_freq,
                                       operation="average").iloc[:, [3]]

    nxr_to_use = nxr_freq[nxr_freq.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / nxr_to_use)
    columns.set_metadata(converted_df, currency="USD")

    return converted_df


def real(df: pd.DataFrame, start_date: Union[str, date, None] = None,
         end_date: Union[str, date, None] = None):
    """Convert dataframe to real prices.

    Convert a dataframe's columns to real prices. Call the `cpi.get()`
    function to obtain the consumer price index. take into account the input
    dataframe's frequency and whether columns represent rolling averages or
    sums. Allow choosing a single period, a range of dates or no period as a
    base (i.e., period for which the average/sum of input dataframe and output
    dataframe is the same).

    Parameters
    ----------
    df : Pandas dataframe
    start_date : str, date or None (default is None)
        If set to a date-like string or a date, and `end_date` is None, the
        base period will be `start_date`.
    end_date : str, date or None (default is None)

    Returns
    -------
    converted_df : Pandas dataframe

    """
    inferred_freq = pd.infer_freq(df.index)

    cpi_data = cpi.get(update="cpi.csv", revise_rows=6, save="cpi.csv",
                       force_update=False)
    columns.set_metadata(cpi_data, ts_type="Flujo")
    cpi_freq = freqs.freq_resample(cpi_data, target=inferred_freq,
                                   operation="average").iloc[:, [0]]
    cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
    cpi_freq = freqs.rolling(cpi_freq, periods=cum_periods,
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

    columns.set_metadata(converted_df, inf_adj=col_text)

    return converted_df


def pcgdp(df: pd.DataFrame, hifreq: bool = True):
    """Calculate dataframe as percentage of GDP.

    Convert a dataframe's columns to percentage of GDP. Call the
    `national_accounts.lin_gdp()` function to obtain UYU and USD quarterly GDP
    series. Take into account the input dataframe's currency for chossing UYU
    or USD GDP. If `hifreq` is set to `True`, GDP will be upsampled and linear
    interpolation will be performed to complete missing data.

    Parameters
    ----------
    df : Pandas dataframe
    hifreq : bool (default is True)
        If True, the input dataframe's frequency is assumed to be 'higher' than
        quarterly (`Q` or `Q-DEC`) and will trigger GDP upsampling.

    Returns
    -------
    converted_df : Pandas dataframe

    """
    inferred_freq = pd.infer_freq(df.index)
    gdp = national_accounts.lin_gdp(update="lin_gdp.csv",
                                    save="lin_gdp.csv", force_update=False)

    if hifreq is True:
        gdp = freqs.freq_resample(gdp, target=inferred_freq,
                                  operation="upsample", interpolation="linear")
    else:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()

    if df.columns.get_level_values("Unidad/Moneda")[0] == "USD":
        gdp = gdp.iloc[:, 1].to_frame()
    else:
        gdp = gdp.iloc[:, 0].to_frame()

    gdp_to_use = gdp[gdp.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / gdp_to_use).multiply(100)

    columns.set_metadata(converted_df, currency="% PBI")

    return converted_df
