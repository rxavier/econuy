import warnings
from typing import Union

import pandas as pd
import numpy as np


def _resample(
    dataset,
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
    data = dataset.data
    indicators = dataset.indicators
    metadata = dataset.metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata[indicators[0]]

    if operation == "sum":
        resampled_df = data.resample(rule).sum()
    elif operation == "mean":
        resampled_df = data.resample(rule).mean()
    elif operation == "last":
        resampled_df = data.resample(rule).last()
    else:
        resampled_df = data.resample(rule).last()
        resampled_df = resampled_df.interpolate(method=interpolation)

    cum_periods = single_metadata["cumulative_periods"]

    if cum_periods != 1:
        input_notna = data.iloc[:, 0].count()
        output_notna = resampled_df.iloc[:, 0].count()
        cum_adj = round(output_notna / input_notna)
        metadata.update_dataset_metadata({"cumulative_periods": cum_adj})

    if operation in ["sum", "mean", "last"]:
        infer_base = pd.infer_freq(data.index)
        try:
            base_freq = pd_frequencies[infer_base]
            target_freq = pd_frequencies[rule]
            if target_freq < base_freq:
                count = int(base_freq / target_freq)
                proc = data.resample(rule).count()
                antimask = np.where(proc >= count, False, True)
                resampled_df = resampled_df.mask(antimask, np.nan)
        except KeyError:
            if warn:
                warnings.warn(
                    "No bin trimming performed because frequencies "
                    "could not be assigned a numeric value",
                    UserWarning,
                )

    resampled_df = resampled_df.dropna(how="all")

    return resampled_df, metadata
