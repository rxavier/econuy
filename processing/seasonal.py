import os

import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tools.sm_exceptions import X13Error

from config import ROOT_DIR
from processing import colnames

X13_PATH = os.path.join(ROOT_DIR, "resources", "x13as")


def decompose(df, trading=True, outlier=True):

    df_proc = df.copy()
    old_columns = df_proc.columns
    df_proc.columns = df_proc.columns.get_level_values(level=0)
    df_proc.index = pd.to_datetime(df_proc.index, errors="coerce")

    trends = []
    seas_adjs = []
    for column in range(len(df_proc.columns)):

        series = df_proc.iloc[:, column].dropna()

        try:
            decomposition = sm.tsa.x13_arima_analysis(series, outlier=outlier, trading=trading, forecast_years=0,
                                                      x12path=X13_PATH, prefer_x13=True)
            trend = decomposition.trend.reindex(df_proc.index)
            seas_adj = decomposition.seasadj.reindex(df_proc.index)

        except X13Error:

            if outlier is True:
                try:
                    print(f"X13 error found while processing '{df_proc.columns[column]}' with selected parameters. "
                          f"Trying with outlier=False...")
                    decomposition = sm.tsa.x13_arima_analysis(series, outlier=False, trading=trading, forecast_years=0,
                                                              x12path=X13_PATH, prefer_x13=True)
                    trend = decomposition.trend.reindex(df_proc.index)
                    seas_adj = decomposition.seasadj.reindex(df_proc.index)

                except X13Error:

                    if trading is True:
                        try:
                            print(f"X13 error found while processing '{df_proc.columns[column]}' with trading=True. "
                                  f"Trying with trading=False...")
                            decomposition = sm.tsa.x13_arima_analysis(series, outlier=False, trading=False,
                                                                      forecast_years=0, x12path=X13_PATH,
                                                                      prefer_x13=True)
                            trend = decomposition.trend.reindex(df_proc.index)
                            seas_adj = decomposition.seasadj.reindex(df_proc.index)

                        except X13Error:
                            print(f"X13 error found while processing '{df_proc.columns[column]}'. Filling with nan.")
                            trend = pd.Series(np.nan, index=df_proc.index)
                            seas_adj = pd.Series(np.nan, index=df_proc.index)

            elif trading is True:

                try:
                    print(f"X13 error found while processing '{df_proc.columns[column]}' with selected parameters. "
                          f"Trying with trading=False...")
                    decomposition = sm.tsa.x13_arima_analysis(series, outlier=outlier, trading=False, forecast_years=0,
                                                              x12path=X13_PATH, prefer_x13=True)
                    trend = decomposition.trend.reindex(df_proc.index)
                    seas_adj = decomposition.seasadj.reindex(df_proc.index)

                except X13Error:
                    print(f"X13 error found while processing '{df_proc.columns[column]}'. Filling with nan.")
                    trend = pd.Series(np.nan, index=df_proc.index)
                    seas_adj = pd.Series(np.nan, index=df_proc.index)

            else:

                try:
                    decomposition = sm.tsa.x13_arima_analysis(series, outlier=outlier, trading=trading, forecast_years=0,
                                                              x12path=X13_PATH, prefer_x13=True)
                    trend = decomposition.trend.reindex(df_proc.index)
                    seas_adj = decomposition.seasadj.reindex(df_proc.index)

                except X13Error:
                    print(f"X13 error found while processing '{df_proc.columns[column]}'. Filling with nan.")
                    trend = pd.Series(np.nan, index=df_proc.index)
                    seas_adj = pd.Series(np.nan, index=df_proc.index)

        trends.append(trend)
        seas_adjs.append(seas_adj)

    trends = pd.concat(trends, axis=1)
    seas_adjs = pd.concat(seas_adjs, axis=1)

    trends.columns = old_columns
    seas_adjs.columns = old_columns

    colnames.set_colnames(trends, seas_adj="Tendencia")

    colnames.set_colnames(seas_adjs, seas_adj="SA")

    return trends, seas_adjs
