import warnings
from typing import Union, Dict, Tuple, Literal, Optional

import pandas as pd
from statsmodels.tools.sm_exceptions import X13Error, X13Warning
from statsmodels.tsa import x13 as x13_sm
from statsmodels.tsa.x13 import x13_arima_analysis as x13a
from statsmodels.tsa.seasonal import STL, seasonal_decompose, MSTL

from econuy.utils import x13 as x13_utils
from econuy.utils.transform import error_handler


# The `_open_and_read` function needs to be monkey-patched to specify the
# encoding or decomposition will fail on Windows
def _new_open_and_read(fname):
    with open(fname, "r", encoding="utf8") as fin:
        fout = fin.read()
    return fout


x13_sm._open_and_read = _new_open_and_read


def _decompose(
    data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    component: Literal["t-c", "sa"] = "sa",
    method: Literal["x13", "loess", "mloess", "moving_averages"] = "x13",
    fallback: str = "loess",
    fn_kwargs: Optional[Dict] = None,
    ignore_warnings: bool = True,
    error_handling: Literal["raise", "coerce", "ignore"] = "raise",
) -> Union[Tuple[pd.DataFrame, pd.DataFrame], pd.DataFrame]:
    indicators = metadata.indicator_ids
    metadata = metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata.indicator_metadata[indicators[0]]

    if single_metadata["seasonal_adjustment"] is not None:
        output = error_handler(
            data,
            errors=error_handling,
            msg="Data has already been seasonally adjusted.",
        )
        return output, metadata

    data_proc = data.copy()
    columns = data_proc.columns
    trends_array = []
    seas_adjs_array = []
    for col in data_proc.columns:
        col_df = data_proc[col].dropna()
        if method == "x13":
            x13_binary_path = x13_utils._get_binary()
            try:
                with warnings.catch_warnings():
                    if ignore_warnings is True:
                        action = "ignore"
                    else:
                        action = "default"
                    warnings.filterwarnings(action=action, category=X13Warning)
                    results = x13a(
                        col_df,
                        x12path=x13_binary_path,
                        prefer_x13=True,
                        **fn_kwargs,
                    )

                    trends = results.trend.reindex(data_proc.index).T
                    seas_adjs = results.seasadj.reindex(data_proc.index).T

            except X13Error:
                print(f"X13 error. Falling back to {fallback}")
                if fallback == "loess":
                    results = STL(col_df).fit()
                elif fallback == "mloess":
                    results = MSTL(col_df).fit()
                else:
                    results = seasonal_decompose(col_df, extrapolate_trend="freq")
                trends = results.trend.reindex(data_proc.index).T
                seas_adjs = (
                    (results.observed - results.seasonal).reindex(data_proc.index).T
                )

        else:
            if method == "loess":
                results = STL(col_df, **fn_kwargs).fit()
            elif method == "mloess":
                results = MSTL(col_df, **fn_kwargs).fit()
            else:
                results = seasonal_decompose(
                    col_df, extrapolate_trend="freq", **fn_kwargs
                )

            trends = results.trend.reindex(data_proc.index).T
            seas_adjs = (results.observed - results.seasonal).reindex(data_proc.index).T

        trends_array.append(trends)
        seas_adjs_array.append(seas_adjs)
    trends = pd.concat(trends_array, axis=1)
    seas_adjs = pd.concat(seas_adjs_array, axis=1)
    trends.columns = columns
    seas_adjs.columns = columns

    if component == "sa":
        output = seas_adjs
        metadata_value = "Seasonally adjusted"
    elif component == "t-c":
        output = trends
        metadata_value = "Trend-cycle"

    metadata.update_dataset_metadata({"seasonal_adjustment": metadata_value})
    metadata.add_transformation_step(
        {"decompose": {"component": component, "method": method}}
    )

    return output, metadata
