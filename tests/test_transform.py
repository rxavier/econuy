from os import path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from econuy.session import Session
from econuy.utils import metadata

CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(CUR_DIR, "test-data")
TEST_CON = create_engine("sqlite://").connect()


def create_dummy_df(
    freq,
    periods=200,
    area="Test",
    currency="Test",
    inf_adj="Test",
    unit="Test",
    seas_adj="Test",
    ts_type="Test",
    cumperiods=1,
):
    dates = pd.date_range("2000-01-31", periods=periods, freq=freq)
    cols = ["A", "B", "C"]
    data = np.hstack(
        [
            np.random.uniform(-100, 100, [periods, 1]),
            np.random.uniform(1, 50, [periods, 1]),
            np.random.uniform(-100, -50, [periods, 1]),
        ]
    )
    output = pd.DataFrame(index=dates, columns=cols, data=data)
    metadata._set(
        output,
        area=area,
        currency=currency,
        inf_adj=inf_adj,
        unit=unit,
        seas_adj=seas_adj,
        ts_type=ts_type,
        cumperiods=cumperiods,
    )
    return output


@pytest.mark.parametrize(
    "freq,ts_type,operation,period,pd_per,pd_rol",
    [
        ("A-DEC", "Flujo", "chg", "last", 1, 1),
        ("Q-DEC", "Stock", "diff", "inter", 4, 1),
        ("M", "Flujo", "chg", "annual", 12, 12),
    ],
)
def test_chg_diff(freq, ts_type, operation, period, pd_per, pd_rol):
    df = create_dummy_df(freq=freq, ts_type=ts_type)
    s = Session(TEST_CON)
    s._datasets.update({"dummy": df})
    s.chg_diff(select="all", operation=operation, period=period)
    compare = s.datasets["dummy"]
    if ts_type == "Flujo" and period == "annual" and pd_rol > 1:
        df = df.rolling(window=pd_rol, min_periods=pd_rol).sum()
    if operation == "chg":
        df = df.pct_change(pd_per).mul(100)
    else:
        df = df.diff(pd_per)
    compare.index, compare.columns = df.index, df.columns
    assert df.equals(compare)


@pytest.mark.parametrize(
    "freq,ts_type,operation,window,pd_rol",
    [("M", "Flujo", "sum", 3, 3), ("Q-DEC", "Flujo", "mean", None, 4)],
)
def test_rolling(freq, ts_type, operation, window, pd_rol):
    df = create_dummy_df(freq=freq, ts_type=ts_type)
    s = Session()
    s._datasets["dummy"] = df
    s.rolling(window=window, operation=operation, select="dummy")
    compare = s.datasets["dummy"]
    if operation == "sum":
        df = df.rolling(window=pd_rol, min_periods=pd_rol).sum()
    else:
        df = df.rolling(window=pd_rol, min_periods=pd_rol).mean()
    compare.index, compare.columns = df.index, df.columns
    assert df.equals(compare)


@pytest.mark.parametrize(
    "freq,ts_type,periods,cumperiods,rule,operation",
    [
        ("M", "Flujo", 192, 1, "A-DEC", "sum"),
        ("Q-DEC", "Flujo", 102, 2, "A-DEC", "mean"),
        ("Q-DEC", "Stock", 102, 1, "A-DEC", "last"),
        ("A-DEC", "Flujo", 102, 1, "M", "upsample"),
    ],
)
def test_resample(freq, ts_type, periods, cumperiods, rule, operation):
    df = create_dummy_df(freq=freq, ts_type=ts_type, periods=periods, cumperiods=cumperiods)
    s = Session()
    s._datasets["dummy"] = df
    s.resample(rule=rule, operation=operation, select=0)
    compare = s._datasets["dummy"]
    if operation == "sum":
        df = df.resample(rule=rule).sum()
    elif operation == "mean":
        df = df.resample(rule=rule).mean()
    elif operation == "last":
        df = df.resample(rule=rule).last()
    else:
        df = df.resample(rule=rule).last().interpolate("linear")

    pd_freqs = {"M": 12, "Q-DEC": 4, "A-DEC": 1}
    if periods % pd_freqs[freq] != 0 and operation != "upsample":
        rows = periods // pd_freqs[freq]
        df = df.iloc[:rows, :]
    compare.index, compare.columns = df.index, df.columns
    assert df.equals(compare)


@pytest.mark.parametrize(
    "component,method,fallback,trading,outlier",
    [
        ("trend", "x13", "loess", True, True),
        ("seas", "x13", "ma", True, False),
        ("trend", "loess", "ma", False, True),
        ("trend", "ma", "loess", False, False),
    ],
)
def test_decompose(component, method, fallback, trading, outlier):
    df = pd.DataFrame(
        index=pd.date_range("2000-01-01", periods=100, freq="Q-DEC"),
        data=np.random.exponential(2, 100).cumsum(),
        columns=["Exponential"],
    )
    df["Real"] = df["Exponential"]
    df.loc[df.index.month == 12, "Real"] = df.loc[df.index.month == 12, "Real"].multiply(
        np.random.uniform(1.06, 1.14)
    )
    df.loc[df.index.month == 6, "Real"] = df.loc[df.index.month == 6, "Real"].multiply(
        np.random.uniform(0.94, 0.96)
    )
    df.loc[df.index.month == 3, "Real"] = df.loc[df.index.month == 3, "Real"].multiply(
        np.random.uniform(1.04, 1.06)
    )
    df.drop("Exponential", axis=1, inplace=True)
    df = df.add(np.random.normal(0, 1, 100), axis=0)
    metadata._set(df, seas_adj="NSA")
    s = Session()
    s._datasets["test"] = df
    s.decompose(
        component=component,
        trading=trading,
        method=method,
        outlier=outlier,
        fallback=fallback,
        ignore_warnings=True,
    )
    assert s._datasets["test"].std().values <= df.std().values


@pytest.mark.parametrize(
    "freq,start_date,end_date,base",
    [("M", "2004-01-01", None, 100), ("Q-DEC", "2004-01-01", "2005-01-01", 100.1)],
)
def test_rebase(freq, start_date, end_date, base):
    df = create_dummy_df(freq=freq)
    s = Session()
    s._datasets["dummy"] = df
    s.rebase(start_date=start_date, end_date=end_date, base=base, select=[0])
    compare = s._datasets["dummy"]
    if end_date is None:
        start = df.iloc[df.index.get_loc(start_date, method="nearest")].name
        df = df / df.loc[start] * base
    else:
        df = df / df.loc[start_date:end_date].mean() * base
    compare.index, compare.columns = df.index, df.columns
    assert df.equals(compare)


@pytest.mark.parametrize(
    "freq,ts_type,periods,cumperiods",
    [
        ("D", "Flujo", 600, 1),
        ("M", "Stock", 48, 1),
        ("W-SUN", "Stock", 200, 1),
        ("Q-DEC", "Flujo", 24, 4),
    ],
)
def test_convert_usd(freq, ts_type, periods, cumperiods):
    df = create_dummy_df(
        freq=freq, periods=periods, ts_type=ts_type, currency="UYU", cumperiods=cumperiods
    )
    s = Session(location=TEST_DIR, download=False)
    s._datasets["dummy"] = df
    s.convert(flavor="usd")
    s.get("nxr_monthly")
    compare = s.datasets["dummy"]
    nxr = s.datasets["nxr_monthly"]
    proc_freq = freq

    if freq in ["D", "W", "B", "W-SUN"]:
        if ts_type == "Stock":
            df = df.resample("M").last()
        else:
            df = df.resample("M").sum()
        proc_freq = "M"

    if ts_type == "Stock":
        nxr = nxr.resample(proc_freq).last().iloc[:, [1]]
    else:
        nxr = nxr.resample(proc_freq).mean().iloc[:, [0]]
        nxr = nxr.rolling(window=cumperiods, min_periods=cumperiods).mean()

    nxr = nxr.reindex(df.index).iloc[:, 0]
    df = df.div(nxr, axis=0)
    compare.index, compare.columns = df.index, df.columns
    assert df.equals(compare)


@pytest.mark.parametrize(
    "freq,periods,ts_type,start_date,end_date",
    [
        ("M", 100, "Flujo", None, None),
        ("D", 1200, "Flujo", "2002-01-01", None),
        ("Q-DEC", 40, "Stock", "2002-01-01", "2002-12-31"),
    ],
)
def test_convert_real(freq, periods, ts_type, start_date, end_date):
    df = create_dummy_df(freq=freq, periods=periods, ts_type=ts_type, currency="UYU")
    s = Session(location=TEST_DIR, download=False)
    s._datasets["dummy"] = df
    s.convert(flavor="real", start_date=start_date, end_date=end_date)
    s.get("cpi")
    compare = s.datasets["dummy"]
    cpi = s.datasets["cpi"]
    proc_freq = freq

    if freq in ["D", "W", "B", "W-SUN"]:
        if ts_type == "Stock":
            df = df.resample("M").last()
        else:
            df = df.resample("M").sum()
        proc_freq = "M"
    save_index = df.index
    cpi = cpi.resample(rule=proc_freq).mean().iloc[:, 0]

    if start_date is None:
        df = df.div(cpi, axis=0)
    elif end_date is None:
        month = df.iloc[df.index.get_loc(start_date, method="nearest")].name
        df = df.div(cpi, axis=0) * cpi.loc[month]
    else:
        df = df.div(cpi, axis=0) * cpi[start_date:end_date].mean()
    df = df.reindex(save_index)
    compare.index, compare.columns = df.index, df.columns
    assert df.equals(compare)


@pytest.mark.parametrize(
    "freq,ts_type,periods,cumperiods,currency",
    [
        ("D", "Flujo", 600, 1, "UYU"),
        ("M", "Stock", 48, 1, "USD"),
        ("A-DEC", "Stock", 10, 1, "USD"),
        ("Q-DEC", "Flujo", 24, 4, "UYU"),
    ],
)
def test_convert_gdp(freq, ts_type, periods, cumperiods, currency):
    df = create_dummy_df(
        freq=freq, periods=periods, ts_type=ts_type, currency=currency, cumperiods=cumperiods
    )
    s = Session(location=TEST_DIR, download=False)
    s._datasets["dummy"] = df
    s.convert(flavor="gdp")
    s.get("_lin_gdp")
    compare = s.datasets["dummy"]
    gdp = s.datasets["_lin_gdp"]

    if freq in ["M", "MS"]:
        gdp = gdp.resample(rule=freq).interpolate("linear")
        if cumperiods != 12 and ts_type == "Flujo":
            converter = int(12 / cumperiods)
            df = df.rolling(window=converter).sum()
    elif freq in ["Q", "Q-DEC"]:
        gdp = gdp.resample(freq, convention="end").asfreq()
        if cumperiods != 4 and ts_type == "Flujo":
            converter = int(4 / cumperiods)
            df = df.rolling(window=converter).sum()
    elif freq in ["A", "A-DEC"]:
        gdp = gdp.resample(freq, convention="end").asfreq()
    else:
        if ts_type == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").mean()
        gdp = gdp.resample(rule="M").interpolate("linear")

    if currency == "USD":
        gdp = gdp.iloc[:, 1].to_frame()
    else:
        gdp = gdp.iloc[:, 0].to_frame()

    gdp = gdp.reindex(df.index).iloc[:, 0]
    df = df.div(gdp, axis=0).multiply(100)
    compare.index, compare.columns = df.index, df.columns
    div = df / compare
    final = div[((div > 1.01) | (div < 0.99)).any(1)]
    assert len(final) == 0
