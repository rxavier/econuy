import pandas as pd
import os
import statsmodels.api as sm

from config import ROOT_DIR
from processing import colnames

X13_PATH = os.path.join(ROOT_DIR, "x13as")


def decompose(df, exec_path):

    old_columns = df.columns
    df.columns = df.columns.get_level_values(level=0)
    df.index = pd.to_datetime(df.index, errors="coerce")

    trends = []
    seas_adjs = []
    for column in range(len(df.columns)):
        series = df.iloc[:, column]
        decomposition = sm.tsa.x13_arima_analysis(series, x12path=exec_path, forecast_years=0)
        trend = decomposition.trend
        seas_adj = decomposition.seasadj

        trends.append(trend)
        seas_adjs.append(seas_adj)

    trends = pd.concat(trends, axis=1)
    seas_adjs = pd.concat(seas_adjs, axis=1)

    trends.columns = old_columns
    seas_adjs.columns = old_columns

    colnames.set_colnames(trends, seas_adj="Trend")

    colnames.set_colnames(seas_adjs, seas_adj="SA")

    return trends, seas_adjs
