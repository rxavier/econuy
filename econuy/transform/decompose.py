import warnings
from pathlib import Path
from typing import Union, Dict, Tuple
from os import PathLike, getcwd, path

import pandas as pd
from statsmodels.tools.sm_exceptions import X13Error, X13Warning
from statsmodels.tsa import x13 as x13_sm
from statsmodels.tsa.x13 import x13_arima_analysis as x13a
from statsmodels.tsa.seasonal import STL, seasonal_decompose

from econuy.utils import metadata, x13
from econuy.utils.transform import error_handler


# The `_open_and_read` function needs to be monkey-patched to specify the
# encoding or decomposition will fail on Windows
def _new_open_and_read(fname):
    with open(fname, "r", encoding="utf8") as fin:
        fout = fin.read()
    return fout


x13_sm._open_and_read = _new_open_and_read


def decompose(
    df: pd.DataFrame,
    component: str = "both",
    method: str = "x13",
    force_x13: bool = False,
    fallback: str = "loess",
    outlier: bool = True,
    trading: bool = True,
    x13_binary: Union[str, PathLike, None] = "search",
    search_parents: int = 0,
    ignore_warnings: bool = True,
    errors: str = "raise",
    **kwargs,
) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Apply seasonal decomposition.

    By default returns both trend and seasonally adjusted components,
    unlike the class method referred below.

    See Also
    --------
    :mod:`~econuy.core.Pipeline.decompose`.

    """
    if errors not in ["raise", "coerce", "ignore"]:
        raise ValueError("method can only be 'x13', 'loess' or 'ma'.")
    if method not in ["x13", "loess", "ma"]:
        raise ValueError("method can only be 'x13', 'loess' or 'ma'.")
    if fallback not in ["loess", "ma"]:
        raise ValueError("method can only be 'loess' or 'ma'.")
    if component not in ["trend", "seas", "both"]:
        raise ValueError("component can only be 'trend', 'seas' or 'both'.")
    if "Seas. Adj." not in df.columns.names:
        raise ValueError(
            "Input dataframe's multiindex requires the " "'Seas. Adj.' level."
        )

    binary_path = None
    if method == "x13":
        if x13_binary == "search":
            binary_path = x13._search_binary(
                start_path=getcwd(), n=search_parents, download_path=getcwd()
            )
        elif isinstance(x13_binary, str):
            binary_path = x13_binary
        elif isinstance(x13_binary, PathLike):
            binary_path = Path(x13_binary).as_posix()
        else:
            binary_path = None
        if isinstance(binary_path, str) and path.isfile(binary_path) is False:
            raise FileNotFoundError(
                "X13 binary missing. Try using 'x13_binary=search'."
            )

    checks = [
        x not in ["Tendencia", "SA"] for x in df.columns.get_level_values("Seas. Adj.")
    ]
    passing = df.loc[:, checks]
    not_passing = df.loc[:, [not x for x in checks]]
    if any(checks):
        if not all(checks) and errors == "raise":
            error_df = df.loc[:, [not check for check in checks]]
            msg = f"{error_df.columns[0][0]} does not have the " f"appropiate metadata."
            return error_handler(df=df, errors=errors, msg=msg)
        passing_output = _decompose(
            passing,
            component=component,
            method=method,
            force_x13=force_x13,
            fallback=fallback,
            outlier=outlier,
            trading=trading,
            x13_binary=binary_path,
            ignore_warnings=ignore_warnings,
            errors=errors,
            **kwargs,
        )
        if not_passing.shape[1] != 0:
            not_passing_output = error_handler(df=not_passing, errors=errors)
        else:
            not_passing_output = not_passing
        if isinstance(passing_output, pd.DataFrame):
            output = pd.concat([passing_output, not_passing_output], axis=1)
            output = output[df.columns.get_level_values(0)]
            return output
        elif isinstance(passing_output, Dict):
            output = {}
            for name, data in passing_output.items():
                aux = pd.concat([data, not_passing_output], axis=1)
                output[name] = aux[df.columns.get_level_values(0)]
            return output
    else:
        return error_handler(df=df, errors=errors)


def _decompose(
    df: pd.DataFrame,
    component: str = "both",
    method: str = "x13",
    force_x13: bool = False,
    fallback: str = "loess",
    outlier: bool = True,
    trading: bool = True,
    x13_binary: Union[str, PathLike, None] = None,
    ignore_warnings: bool = True,
    errors: str = "raise",
    **kwargs,
) -> Union[Tuple[pd.DataFrame, pd.DataFrame], pd.DataFrame]:
    if method not in ["x13", "loess", "ma"]:
        raise ValueError("method can only be 'x13', 'loess' or 'ma'.")
    if fallback not in ["loess", "ma"]:
        raise ValueError("method can only be 'loess' or 'ma'.")

    df_proc = df.copy()
    old_columns = df_proc.columns
    df_proc.columns = df_proc.columns.get_level_values(level=0)
    df_proc.index = pd.to_datetime(df_proc.index, errors="coerce")

    trends_array = []
    seas_adjs_array = []
    for col in df_proc.columns:
        col_df = df_proc[col].dropna()
        if method == "x13":
            try:
                with warnings.catch_warnings():
                    if ignore_warnings is True:
                        action = "ignore"
                    else:
                        action = "default"
                    warnings.filterwarnings(action=action, category=X13Warning)
                    results = x13a(
                        col_df,
                        outlier=outlier,
                        trading=trading,
                        x12path=x13_binary,
                        prefer_x13=True,
                        **kwargs,
                    )

                    trends = results.trend.reindex(df_proc.index).T
                    seas_adjs = results.seasadj.reindex(df_proc.index).T

            except X13Error:
                if force_x13 is True:
                    if outlier is True:
                        try:
                            warnings.warn(
                                "X13 error found with selected "
                                "parameters. Trying with outlier=False.",
                                UserWarning,
                            )
                            results = x13a(
                                col_df,
                                outlier=False,
                                trading=trading,
                                x12path=x13_binary,
                                prefer_x13=True,
                                **kwargs,
                            )
                        except X13Error:
                            try:
                                warnings.warn(
                                    "X13 error found with trading=True. "
                                    "Trying with trading=False.",
                                    UserWarning,
                                )
                                results = x13a(
                                    col_df,
                                    outlier=False,
                                    trading=False,
                                    x12path=x13_binary,
                                    prefer_x13=True,
                                    **kwargs,
                                )
                                trends = results.trend.reindex(df_proc.index).T
                                seas_adjs = results.seasadj.reindex(df_proc.index).T
                            except X13Error:
                                warnings.warn(
                                    "No combination of parameters "
                                    "successful. No decomposition "
                                    "performed.",
                                    UserWarning,
                                )
                                trends = error_handler(df=col_df, errors=errors)
                                seas_adjs = trends.copy()

                    elif trading is True:
                        try:
                            warnings.warn(
                                "X13 error found with trading=True. "
                                "Trying with trading=False...",
                                UserWarning,
                            )
                            results = x13a(
                                col_df,
                                trading=False,
                                x12path=x13_binary,
                                prefer_x13=True,
                                **kwargs,
                            )
                            trends = results.trend.reindex(df_proc.index).T
                            seas_adjs = results.seasadj.reindex(df_proc.index).T
                        except X13Error:
                            warnings.warn(
                                "No combination of parameters "
                                "successful. Filling with NaN.",
                                UserWarning,
                            )
                            trends = error_handler(df=col_df, errors=errors)
                            seas_adjs = trends.copy()

                else:
                    if fallback == "loess":
                        results = STL(col_df).fit()
                    else:
                        results = seasonal_decompose(col_df, extrapolate_trend="freq")
                    trends = results.trend.reindex(df_proc.index).T
                    seas_adjs = (
                        (results.observed - results.seasonal).reindex(df_proc.index).T
                    )

        else:
            if method == "loess":
                results = STL(col_df).fit()
            else:
                results = seasonal_decompose(col_df, extrapolate_trend="freq")
            trends = results.trend.reindex(df_proc.index).T
            seas_adjs = (results.observed - results.seasonal).reindex(df_proc.index).T

        trends_array.append(trends)
        seas_adjs_array.append(seas_adjs)
    trends = pd.concat(trends_array, axis=1)
    seas_adjs = pd.concat(seas_adjs_array, axis=1)
    trends.columns = old_columns
    seas_adjs.columns = old_columns
    metadata._set(trends, seas_adj="Tendencia")
    metadata._set(seas_adjs, seas_adj="SA")
    if component == "both":
        output = {"trend": trends, "seas": seas_adjs}
    elif component == "seas":
        output = seas_adjs
    elif component == "trend":
        output = trends

    return output
