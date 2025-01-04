from typing import Union, Literal
from datetime import datetime

import pandas as pd

from econuy.utils.transform import error_handler


def _convert_usd(
    data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    error_handling: Literal["raise", "coerce", "ignore"] = "raise",
) -> pd.DataFrame:
    from econuy.load import load_dataset

    indicators = metadata.indicator_ids
    metadata = metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata.indicator_metadata[indicators[0]]

    if single_metadata["currency"] != "UYU":
        output = error_handler(data, errors=error_handling, msg="Currency is not UYU")
        return output, metadata

    nxr = load_dataset("nxr_monthly")
    time_series_type = single_metadata["time_series_type"]
    inferred_freq = pd.infer_freq(data.index)
    target_freq = inferred_freq

    # For now we only support converting monthly or lower frequency data, so we resample first.
    if inferred_freq in ["D", "B", "C", "W", "W-SUN", None]:
        if time_series_type == "Flow":
            data = data.resample("ME").sum()
        else:
            data = data.resample("ME").last()
        target_freq = "ME"

    if time_series_type == "Stock":
        nxr_freq = nxr.resample(target_freq, operation="last").data.iloc[:, [1]]
    else:
        cum_periods = single_metadata["cumulative_periods"]
        nxr_freq = (
            nxr.resample(target_freq, operation="mean")
            .rolling(window=cum_periods, operation="mean")
            .data.iloc[:, [0]]
        )

    nxr_to_use = nxr_freq.reindex(data.index).iloc[:, 0]
    output = data.div(nxr_to_use, axis=0)
    metadata.update_dataset_metadata({"currency": "USD"})
    metadata.add_transformation_step({"convert": {"flavor": "usd"}})

    return output, metadata


def _convert_real(
    data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    start_date: Union[str, datetime, None] = None,
    end_date: Union[str, datetime, None] = None,
    error_handling: Literal["raise", "coerce", "ignore"] = "raise",
) -> pd.DataFrame:
    from econuy.load import load_dataset

    indicators = metadata.indicator_ids
    metadata = metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata.indicator_metadata[indicators[0]]

    if single_metadata["currency"] != "UYU":
        output = error_handler(data, errors=error_handling, msg="Currency is not UYU")
        return output, metadata
    elif "Constant" in str(single_metadata["inflation_adjustment"]):
        output = error_handler(
            data, errors=error_handling, msg="Already inflation adjusted"
        )
        return output, metadata

    cpi = load_dataset("cpi")

    inferred_freq = pd.infer_freq(data.index)
    target_freq = inferred_freq
    if inferred_freq in ["D", "B", "C", "W", "W-SUN", None]:
        if single_metadata["time_series_type"] == "Flow":
            data = data.resample("ME").sum()
        else:
            data = data.resample("ME").mean()
        target_freq = pd.infer_freq(data.index)

    cum_periods = single_metadata["cumulative_periods"]
    cpi_to_use = (
        cpi.resample(target_freq, operation="mean")
        .rolling(cum_periods, operation="mean")
        .data.iloc[:, 0]
    )

    start_date = (
        datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(start_date, str)
        else start_date
    )
    end_date = (
        datetime.strptime(end_date, "%Y-%m-%d")
        if isinstance(end_date, str)
        else end_date
    )

    if start_date is None:
        converted_df = data.div(cpi_to_use, axis=0)
        col_text = "Constant prices"
    elif end_date is None:
        # start_date = pd.to_datetime(start_date)
        month = data.index.to_series().sub(pd.to_datetime(start_date)).abs().idxmin()
        converted_df = data.div(cpi_to_use, axis=0) * cpi_to_use.loc[month]
        m_start = start_date.strftime("%Y-%m")
        col_text = f"Constant prices {m_start}"
    else:
        converted_df = (
            data.div(cpi_to_use, axis=0) * cpi_to_use[start_date:end_date].mean()
        )
        m_start = start_date.strftime("%Y-%m")
        m_end = end_date.strftime("%Y-%m")
        if m_start == m_end:
            col_text = f"Constant prices {m_start}"
        else:
            col_text = f"Constant prices {m_start}_{m_end}"

    converted_df = converted_df.reindex(data.index)
    metadata.update_dataset_metadata({"inflation_adjustment": col_text})
    start_date_str = start_date.strftime("%Y-%m-%d") if start_date is not None else None
    end_date_str = end_date.strftime("%Y-%m-%d") if end_date is not None else None
    metadata.add_transformation_step(
        {
            "convert": {
                "flavor": "real",
                "start_date": start_date_str,
                "end_date": end_date_str,
            }
        }
    )

    return converted_df, metadata


def _convert_gdp(
    data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    error_handling: Literal["raise", "coerce", "ignore"] = "raise",
) -> pd.DataFrame:
    from econuy.load import load_dataset

    indicators = metadata.indicator_ids
    metadata = metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata.indicator_metadata[indicators[0]]

    if single_metadata["currency"] not in ["UYU", "USD"]:
        output = error_handler(
            data, errors=error_handling, msg="Currency is not UYU or USD"
        )
        return output, metadata
    elif single_metadata["unit"] == "% GDP":
        output = error_handler(data, errors=error_handling, msg="Already in % GDP")
        return output, metadata
    elif single_metadata["inflation_adjustment"] is not None:
        output = error_handler(
            data, errors=error_handling, msg="Data is inflation adjusted"
        )
        return output, metadata

    gdp = load_dataset("gdp_denominator").data

    inferred_freq = pd.infer_freq(data.index)
    target_freq = inferred_freq
    cum_periods = single_metadata["cumulative_periods"]
    ts_type = single_metadata["time_series_type"]

    if inferred_freq in ["M", "MS", "ME"]:
        gdp = gdp.resample(target_freq).interpolate("linear")
        if cum_periods != 12 and ts_type == "Flow":
            converter = int(12 / cum_periods)
            data = data.rolling(window=converter).sum()
    elif inferred_freq in ["Q", "QE-DEC", "QE-DEC"]:
        gdp = gdp.resample(inferred_freq).asfreq()
        if cum_periods != 4 and ts_type == "Flow":
            converter = int(4 / cum_periods)
            data = data.rolling(window=converter).sum()
    elif inferred_freq in ["A", "A-DEC", "YE-DEC"]:
        gdp = gdp.resample(inferred_freq).asfreq()
    elif inferred_freq in ["D", "B", "C", "W", "W-SUN", None]:
        if ts_type == "Flow":
            data = data.resample("ME").sum()
        else:
            data = data.resample("ME").mean()
        gdp = gdp.resample("ME").interpolate("linear")
    else:
        raise ValueError(
            "Frequency of input dataframe not any of 'D', 'C', "
            "'W', 'B', 'M', 'MS', 'Q', 'QE-DEC', 'A' or 'A-DEC'."
        )

    currency = single_metadata["currency"]
    if currency == "USD":
        gdp = gdp.iloc[:, 1].to_frame()
    else:
        gdp = gdp.iloc[:, 0].to_frame()

    gdp_to_use = gdp.reindex(data.index).iloc[:, 0]
    converted_df = data.div(gdp_to_use, axis=0).multiply(100)

    metadata.update_dataset_metadata({"unit": "% GDP"})
    metadata.add_transformation_step({"convert": {"flavor": "gdp"}})

    return converted_df, metadata
