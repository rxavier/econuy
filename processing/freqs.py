from processing import colnames


def freq_resample(df, target, operation="sum", interpolation="linear"):

    if df.columns.get_level_values("Type")[0] == "Flow":

        if operation == "sum":
            resampled_df = df.resample(target).sum()
        elif operation == "average":
            resampled_df = df.resample(target).mean()
        elif operation == "upsample":
            resampled_df = df.resample(target).asfreq().interpolate(method=interpolation)
        else:
            raise Warning("Only sum, average and upsample are accepted operations")
            return

        cum_periods = df.columns.get_level_values("Cumulative periods")[0]
        if cum_periods != 1:
            input_notna = df.iloc[:, 0].count()
            output_notna = resampled_df.iloc[:, 0].count()
            cum_adj = round(output_notna/input_notna)
            colnames.set_colnames(resampled_df, cumperiods=int(cum_periods*cum_adj))

    elif df.columns.get_level_values("Type")[0] == "Stock":

        resampled_df = df.resample(target, convention="end").asfreq().interpolate(method=interpolation)

    else:
        raise Warning("Dataframe needs to have a Type")
        return

    colnames.set_colnames(resampled_df)

    return resampled_df
