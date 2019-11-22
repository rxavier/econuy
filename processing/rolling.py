import pandas as pd

from processing import colnames

PD_FREQUENCIES = {"A": 1,
                  "Q-DEC": 4,
                  "M": 12}


def rolling(df, periods=None, operation="sum"):

    window_operation = {"sum": lambda x: x.rolling(window=periods, min_periods=periods).sum(),
                        "average": lambda x: x.rolling(window=periods, min_periods=periods).mean()}

    if df.columns.get_level_values("Tipo")[0] == "Stock":
        raise Warning("Rolling operations shouldn't be calculated on stock variables")

    if periods is None:
        inferred_freq = pd.infer_freq(df.index)
        periods = PD_FREQUENCIES[inferred_freq]

    rolling_df = df.apply(window_operation[operation])

    colnames.set_colnames(rolling_df, cumperiods=periods)

    return rolling_df
