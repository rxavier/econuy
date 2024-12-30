from typing import Tuple

import pandas as pd


def _chg_diff(
    data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    operation: str = "chg",
    period: str = "last",
) -> Tuple[pd.DataFrame, "Metadata"]:  # type: ignore # noqa: F821
    from econuy.transform.rolling import _rolling

    indicators = metadata.indicator_ids
    metadata = metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata.indicator_metadata[indicators[0]]
    time_series_type = single_metadata["time_series_type"]
    inferred_freq = pd.infer_freq(data.index)

    type_change = {
        "last": {
            "chg": [lambda x: x.pct_change(periods=1), "Pct. change"],
            "diff": [lambda x: x.diff(periods=1), "Change"],
        },
        "inter": {
            "chg": [
                lambda x: x.pct_change(periods=last_year),
                "Pct. change YoY",
            ],
            "diff": [lambda x: x.diff(periods=last_year), "Change YoY"],
        },
        "annual": {
            "chg": [lambda x: x.pct_change(periods=last_year), "Pct. change annual"],
            "diff": [lambda x: x.diff(periods=last_year), "Change annual"],
        },
    }

    if inferred_freq in ["ME"]:
        last_year = 12
    elif inferred_freq in ["QE", "QE-DEC"]:
        last_year = 4
    elif inferred_freq in ["YE", "YE-DEC"]:
        last_year = 1
    else:
        raise ValueError(
            "The dataframe needs to have a frequency of ME "
            "(month end), QQ (quarter end) or YE (year end)"
        )

    if period == "annual":
        if time_series_type == "Stock":
            output = data.apply(type_change[period][operation][0])
        else:
            output, metadata = _rolling(data, metadata, operation="sum")
            output = output.apply(type_change[period][operation][0])
    else:
        output = data.apply(type_change[period][operation][0])

    if operation == "chg":
        output = output.multiply(100)

    metadata.update_dataset_metadata({"unit": type_change[period][operation][1]})
    metadata.add_transformation_step(
        {"chg_diff": {"operation": operation, "period": period}}
    )

    return output, metadata
