import warnings
from typing import Union, Tuple

import pandas as pd
import numpy as np


def _resample(
    data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    rule: Union[pd.DateOffset, pd.Timedelta, str],
    operation: str = "sum",
    interpolation: str = "linear",
) -> Tuple[pd.DataFrame, "Metadata"]:  # type: ignore # noqa: F821
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
    indicators = metadata.indicator_ids
    metadata = metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata.indicator_metadata[indicators[0]]

    if operation == "sum":
        output = data.resample(rule).sum()
    elif operation == "mean":
        output = data.resample(rule).mean()
    elif operation == "last":
        output = data.resample(rule).last()
    else:
        output = data.resample(rule).last()
        output = output.interpolate(method=interpolation)

    cum_periods = single_metadata["cumulative_periods"]

    if cum_periods != 1:
        input_notna = data.iloc[:, 0].count()
        output_notna = output.iloc[:, 0].count()
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
                output = output.mask(antimask, np.nan)
        except KeyError:
            warnings.warn(
                "No bin trimming performed because frequencies "
                "could not be assigned a numeric value",
                UserWarning,
            )
    metadata.add_transformation_step(
        {
            "resample": {
                "rule": rule,
                "operation": operation,
                "interpolation": interpolation,
            }
        }
    )
    output = output.dropna(how="all")

    return output, metadata
