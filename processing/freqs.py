import pandas as pd

from processing import colnames

PD_FREQUENCIES = {"A": 1,
                  "Q-DEC": 4,
                  "M": 12}


def freq_resample(df, target, operation="sum", interpolation="linear"):

    if df.columns.get_level_values("Tipo")[0] == "Flujo":

        if operation == "sum":
            resampled_df = df.resample(target).sum()
        elif operation == "average":
            resampled_df = df.resample(target).mean()
        elif operation == "upsample":
            resampled_df = df.resample(target).asfreq().interpolate(method=interpolation)
        else:
            raise ValueError("Only sum, average and upsample are accepted operations")

        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        if cum_periods != 1:
            input_notna = df.iloc[:, 0].count()
            output_notna = resampled_df.iloc[:, 0].count()
            cum_adj = round(output_notna/input_notna)
            colnames.set_colnames(resampled_df, cumperiods=int(cum_periods*cum_adj))

    elif df.columns.get_level_values("Tipo")[0] == "Stock":

        resampled_df = df.resample(target, convention="end").asfreq().interpolate(method=interpolation)

    else:
        raise ValueError("Dataframe needs to have a Type")

    colnames.set_colnames(resampled_df)

    return resampled_df


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
