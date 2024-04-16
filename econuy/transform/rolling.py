import warnings
from typing import Optional, Tuple

import pandas as pd


def _rolling(
    data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    window: Optional[int] = None,
    operation: str = "sum",
) -> Tuple[pd.DataFrame, "Metadata"]:  # type: ignore # noqa: F821
    indicators = metadata.indicators
    metadata = metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata[indicators[0]]
    time_series_type = single_metadata["time_series_type"]
    pd_frequencies = {
        "YE": 1,
        "YE-DEC": 1,
        "QE": 4,
        "QE-DEC": 4,
        "ME": 12,
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

    if time_series_type == "Stock":
        warnings.warn(
            "Rolling operations should not be " "calculated on stock variables",
            UserWarning,
        )

    if window is None:
        inferred_freq = pd.infer_freq(data.index)
        window = pd_frequencies[inferred_freq]

    rolling_df = data.apply(window_operation[operation])
    metadata.update_dataset_metadata({"cumulative_periods": window})

    return rolling_df, metadata
