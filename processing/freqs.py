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

    elif df.columns.get_level_values("Type")[0] == "Stock":

        resampled_df = df.resample(target, convention="end").asfreq().interpolate(method=interpolation)

    else:
        raise Warning("Dataframe needs to have a Type")
        return

    colnames.set_colnames(resampled_df)

    return resampled_df
