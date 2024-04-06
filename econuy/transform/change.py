import pandas as pd

from econuy.utils import metadata
from econuy.transform import rolling


def chg_diff(
    df: pd.DataFrame, operation: str = "chg", period: str = "last"
) -> pd.DataFrame:
    """
    Calculate pct change or difference.

    See Also
    --------
    :mod:`~econuy.core.Pipeline.chg_diff`.

    """
    if operation not in ["chg", "diff"]:
        raise ValueError("Invalid 'operation' option.")
    if period not in ["last", "inter", "annual"]:
        raise ValueError("Invalid 'period' option.")
    if "Tipo" not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the " "'Tipo' level.")

    all_metadata = df.columns.droplevel("Indicador")
    if all(x == all_metadata[0] for x in all_metadata):
        return _chg_diff(df=df, operation=operation, period=period)
    else:
        columns = []
        for column_name in df.columns:
            df_column = df[[column_name]]
            converted = _chg_diff(df=df_column, operation=operation, period=period)
            columns.append(converted)
        return pd.concat(columns, axis=1)


def _chg_diff(
    df: pd.DataFrame, operation: str = "chg", period: str = "last"
) -> pd.DataFrame:
    inferred_freq = pd.infer_freq(df.index)

    type_change = {
        "last": {
            "chg": [lambda x: x.pct_change(periods=1), "% variación"],
            "diff": [lambda x: x.diff(periods=1), "Cambio"],
        },
        "inter": {
            "chg": [
                lambda x: x.pct_change(periods=last_year),
                "% variación interanual",
            ],
            "diff": [lambda x: x.diff(periods=last_year), "Cambio interanual"],
        },
        "annual": {
            "chg": [lambda x: x.pct_change(periods=last_year), "% variación anual"],
            "diff": [lambda x: x.diff(periods=last_year), "Cambio anual"],
        },
    }

    if inferred_freq in ["ME", "ME"]:
        last_year = 12
    elif inferred_freq in ["Q", "QE-DEC", "QE-DEC"]:
        last_year = 4
    elif inferred_freq in ["A", "A-DEC", "YE-DEC"]:
        last_year = 1
    else:
        raise ValueError(
            "The dataframe needs to have a frequency of M "
            "(month end), Q (quarter end) or A (year end)"
        )

    if period == "annual":
        if df.columns.get_level_values("Tipo")[0] == "Stock":
            output = df.apply(type_change[period][operation][0])
        else:
            output = rolling(df, operation="sum")
            output = output.apply(type_change[period][operation][0])

        metadata._set(output, unit=type_change[period][operation][1])

    else:
        output = df.apply(type_change[period][operation][0])
        metadata._set(output, unit=type_change[period][operation][1])

    if operation == "chg":
        output = output.multiply(100)

    return output
