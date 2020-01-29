import os
import platform

import pandas as pd
import numpy as np
from statsmodels.api import tsa
from statsmodels.tools.sm_exceptions import X13Error

from econuy.config import ROOT_DIR
from econuy.processing import columns

X13_PATH = os.path.join(ROOT_DIR, "resources", "x13as")

if platform.system() == "Windows":
    X13_PATH = X13_PATH + ".exe"


def decompose(df: pd.DataFrame, trading: bool = True, outlier: bool = True):
    """Apply X13 decomposition. Return trend and seasonally adjusted dataframe.

    Decompose the series in a Pandas dataframe using the US Census X13
    methodology. Will try different combinations of the `trading` and `outlier`
    arguments if an X13 error is raised. Requires having the X13 binary in the
    `resources` folder. Please refer to the README for instructions on where
    to get this binary.

    Parameters
    ----------
    df : Pandas dataframe
    trading : bool (default is True)
        Whether to automatically detect trading days.
    outlier : bool (default is True)
        Whether to automatically detect outliers.

    Returns
    -------
    trend, seas_adj : Pandas dataframe
        Dataframes of the same shape of the input dataframe, containing the
        trend component and the seasonally adjusted series.

    """
    if os.path.isfile(X13_PATH) is False:
        print("X13 binary missing. Place the relevant binary for your system"
              "within the 'resources' directory. Please refer to the README"
              "for instructions on where to get binaries for Windows and Unix,"
              "and how to compile it for macOS.")
        return

    df_proc = df.copy()
    old_columns = df_proc.columns
    df_proc.columns = df_proc.columns.get_level_values(level=0)
    df_proc.index = pd.to_datetime(df_proc.index, errors="coerce")

    trends = []
    seas_adjs = []
    for column in range(len(df_proc.columns)):
        series = df_proc.iloc[:, column].dropna()
        try:
            decomposition = tsa.x13_arima_analysis(
                series, outlier=outlier, trading=trading, forecast_years=0,
                x12path=X13_PATH, prefer_x13=True
            )
            trend = decomposition.trend.reindex(df_proc.index)
            seas_adj = decomposition.seasadj.reindex(df_proc.index)

        except X13Error:
            if outlier is True:
                try:
                    print(f"X13 error found while processing "
                          f"'{df_proc.columns[column]}' with selected "
                          f"parameters. Trying with outlier=False...")
                    return decompose(df=df, outlier=False)

                except X13Error:
                    try:
                        print(f"X13 error found while processing "
                              f"'{df_proc.columns[column]}' with "
                              f"trading=True. Trying with trading=False...")
                        return decompose(df=df, outlier=False,
                                         trading=False)

                    except X13Error:
                        print(f"X13 error found while processing "
                              f"'{df_proc.columns[column]}'. "
                              f"Filling with nan.")
                        trend = pd.Series(np.nan, index=df_proc.index)
                        seas_adj = pd.Series(np.nan, index=df_proc.index)

            elif trading is True:
                try:
                    print(f"X13 error found while processing "
                          f"'{df_proc.columns[column]}' "
                          f"with trading=True. Trying with "
                          f"trading=False...")
                    return decompose(df=df, trading=False)

                except X13Error:
                    print(f"X13 error found while processing "
                          f"'{df_proc.columns[column]}'. Filling with nan.")
                    trend = pd.Series(np.nan, index=df_proc.index)
                    seas_adj = pd.Series(np.nan, index=df_proc.index)

            else:
                try:
                    return decompose(df=df)

                except X13Error:
                    print(f"X13 error found while processing "
                          f"'{df_proc.columns[column]}'. "
                          f"Filling with nan.")
                    trend = pd.Series(np.nan, index=df_proc.index)
                    seas_adj = pd.Series(np.nan, index=df_proc.index)

        trends.append(trend)
        seas_adjs.append(seas_adj)

    trends = pd.concat(trends, axis=1)
    seas_adjs = pd.concat(seas_adjs, axis=1)

    trends.columns = old_columns
    seas_adjs.columns = old_columns

    columns.set_metadata(trends, seas_adj="Tendencia")

    columns.set_metadata(seas_adjs, seas_adj="SA")

    return trends, seas_adjs
