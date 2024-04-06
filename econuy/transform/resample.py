import warnings
from typing import Union

import pandas as pd
import numpy as np

from econuy.utils import metadata


def resample(
    df: pd.DataFrame,
    rule: Union[pd.DateOffset, pd.Timedelta, str],
    operation: str = "sum",
    interpolation: str = "linear",
    warn: bool = False,
) -> pd.DataFrame:
    """
    Resample to target frequencies.

    See Also
    --------
    :mod:`~econuy.core.Pipeline.resample`

    """
    if operation not in ["sum", "mean", "upsample", "last"]:
        raise ValueError("Invalid 'operation' option.")
    if "Acum. períodos" not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the " "'Acum. períodos' level.")

    all_metadata = df.columns.droplevel("Indicador")
    if all(x == all_metadata[0] for x in all_metadata):
        return _resample(
            df=df, rule=rule, operation=operation, interpolation=interpolation, warn=warn
        )
    else:
        columns = []
        for column_name in df.columns:
            df_column = df[[column_name]]
            converted = _resample(
                df=df_column,
                rule=rule,
                operation=operation,
                interpolation=interpolation,
                warn=warn,
            )
            columns.append(converted)
        return pd.concat(columns, axis=1)


def _resample(
    df: pd.DataFrame,
    rule: Union[pd.DateOffset, pd.Timedelta, str],
    operation: str = "sum",
    interpolation: str = "linear",
    warn: bool = False,
) -> pd.DataFrame:
    pd_frequencies = {
        "A": 1,
        "A-DEC": 1,
        "YE-DEC": 1,
        "Q": 4,
        "Q-DEC": 4,
        "QE-DEC": 4,
        "M": 12,
        "ME": 12,
        "W": 52.143,
        "W-SUN": 52.143,
        "2W": 26.071,
        "2W-SUN": 26.071,
        "B": 240,
        "D": 365,
    }

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
        metadata._set(resampled_df, cumperiods=int(cum_periods * cum_adj))

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
            if warn:
                warnings.warn(
                    "No bin trimming performed because frequencies "
                    "could not be assigned a numeric value",
                    UserWarning,
                )

    metadata._set(resampled_df)
    resampled_df = resampled_df.dropna(how="all")

    return resampled_df
