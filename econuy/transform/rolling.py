import warnings
from typing import Optional

import pandas as pd

from econuy.utils import metadata


def rolling(
    df: pd.DataFrame, window: Optional[int] = None, operation: str = "sum"
) -> pd.DataFrame:
    """
    Calculate rolling averages or sums.

    See Also
    --------
    :mod:`~econuy.core.Pipeline.rolling`.

    """
    if operation not in ["sum", "mean"]:
        raise ValueError("Invalid 'operation' option.")
    if "Tipo" not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the " "'Tipo' level.")

    all_metadata = df.columns.droplevel("Indicador")
    if all(x == all_metadata[0] for x in all_metadata):
        return _rolling(df=df, window=window, operation=operation)
    else:
        columns = []
        for column_name in df.columns:
            df_column = df[[column_name]]
            converted = _rolling(df=df_column, window=window, operation=operation)
            columns.append(converted)
        return pd.concat(columns, axis=1)


def _rolling(
    df: pd.DataFrame, window: Optional[int] = None, operation: str = "sum"
) -> pd.DataFrame:
    pd_frequencies = {
        "A": 1,
        "A-DEC": 1,
        "YE-DEC": 1,
        "Q": 4,
        "QE-DEC": 4,
        "QE-DEC": 4,
        "M": 12,
        "ME": 12,
        "MS": 12,
        "W": 52,
        "W-SUN": 52,
        "2W": 26,
        "2W-SUN": 26,
        "B": 260,
        "D": 365,
    }

    window_operation = {
        "sum": lambda x: x.rolling(window=window, min_periods=window).sum(),
        "mean": lambda x: x.rolling(window=window, min_periods=window).mean(),
    }

    if df.columns.get_level_values("Tipo")[0] == "Stock":
        warnings.warn(
            "Rolling operations should not be " "calculated on stock variables", UserWarning
        )

    if window is None:
        inferred_freq = pd.infer_freq(df.index)
        window = pd_frequencies[inferred_freq]

    rolling_df = df.apply(window_operation[operation])

    metadata._set(rolling_df, cumperiods=window)

    return rolling_df
