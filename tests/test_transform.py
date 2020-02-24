from os import path

import numpy as np
import pandas as pd
import pytest

from econuy import transform
from econuy.session import Session
from econuy.resources import columns

CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(path.dirname(CUR_DIR), "test-data")


def dummy_df(freq, area="Test", currency="Test",
             inf_adj="Test", index="Test", seas_adj="Test",
             ts_type="Test", cumperiods=1):
    dates = pd.date_range("2000-01-31", periods=200, freq=freq)
    cols = ["A", "B", "C"]
    data = np.hstack([np.random.uniform(-100, 100, [200, 1]),
                      np.random.uniform(1, 50, [200, 1]),
                      np.random.uniform(-100, -50, [200, 1])])
    output = pd.DataFrame(index=dates, columns=cols, data=data)
    columns._setmeta(output, area=area, currency=currency,
                     inf_adj=inf_adj, index=index, seas_adj=seas_adj,
                     ts_type=ts_type, cumperiods=cumperiods)
    return output


def test_diff():
    data_m = dummy_df(freq="M")
    session = Session(loc_dir=TEST_DIR, dataset=data_m)
    trf_last = session.chg_diff(operation="diff", period_op="last").dataset
    trf_last.columns = data_m.columns
    assert trf_last.equals(data_m.diff(periods=1))
    data_q1 = dummy_df(freq="Q-DEC")
    data_q2 = dummy_df(freq="Q-DEC")
    data_dict = {"data_q1": data_q1, "data_q2": data_q2}
    session = Session(loc_dir=TEST_DIR, dataset=data_dict)
    trf_inter = session.chg_diff(operation="diff", period_op="inter").dataset
    trf_inter["data_q1"].columns = trf_inter["data_q2"].columns = data_q1.columns
    assert trf_inter["data_q1"].equals(data_q1.diff(periods=4))
    assert trf_inter["data_q2"].equals(data_q2.diff(periods=4))
    data_a = dummy_df(freq="A", ts_type="Flow")
    trf_annual = transform.chg_diff(data_a, operation="diff", period_op="last")
    trf_annual.columns = data_a.columns
    assert trf_annual.equals(data_a.diff(periods=1))
    data_q_annual = dummy_df(freq="Q-DEC", ts_type="Flujo")
    trf_q_annual = transform.chg_diff(data_q_annual, operation="diff",
                                      period_op="annual")
    trf_q_annual.columns = data_q_annual.columns
    assert trf_q_annual.equals(data_q_annual.
                               rolling(window=4, min_periods=4).
                               sum().
                               diff(periods=4))
    data_q_annual = dummy_df(freq="Q-DEC", ts_type="Stock")
    trf_q_annual = transform.chg_diff(data_q_annual, operation="diff",
                                      period_op="annual")
    trf_q_annual.columns = data_q_annual.columns
    assert trf_q_annual.equals(data_q_annual.diff(periods=4))
    with pytest.raises(ValueError):
        data_wrong = data_m.iloc[np.random.randint(0, 200, 100)]
        transform.chg_diff(data_wrong)


def test_chg():
    data_m = dummy_df(freq="M")
    session = Session(loc_dir=TEST_DIR, dataset=data_m)
    trf_last = session.chg_diff(operation="chg", period_op="last").dataset
    trf_last.columns = data_m.columns
    assert trf_last.equals(data_m.pct_change(periods=1).multiply(100))
    data_q1 = dummy_df(freq="Q-DEC")
    data_q2 = dummy_df(freq="Q-DEC")
    data_dict = {"data_q1": data_q1, "data_q2": data_q2}
    session = Session(loc_dir=TEST_DIR, dataset=data_dict)
    trf_inter = session.chg_diff(operation="chg", period_op="inter").dataset
    trf_inter["data_q1"].columns = trf_inter["data_q2"].columns = data_q1.columns
    assert trf_inter["data_q1"].equals(data_q1.pct_change(periods=4).
                                       multiply(100))
    assert trf_inter["data_q2"].equals(data_q2.pct_change(periods=4).
                                       multiply(100))
    data_a = dummy_df(freq="A", ts_type="Flow")
    trf_annual = transform.chg_diff(data_a, operation="chg", period_op="last")
    trf_annual.columns = data_a.columns
    assert trf_annual.equals(data_a.pct_change(periods=1).multiply(100))
    data_q_annual = dummy_df(freq="Q-DEC", ts_type="Flujo")
    trf_q_annual = transform.chg_diff(data_q_annual, operation="chg",
                                      period_op="annual")
    trf_q_annual.columns = data_q_annual.columns
    assert trf_q_annual.equals(data_q_annual.
                               rolling(window=4, min_periods=4).
                               sum().
                               pct_change(periods=4).multiply(100))


def test_rolling():
    data_m = dummy_df(freq="M", ts_type="Flujo")
    session = Session(loc_dir=TEST_DIR, dataset=data_m)
    trf_none = session.rolling(operation="sum").dataset
    trf_none.columns = data_m.columns
    assert trf_none.equals(data_m.rolling(window=12, min_periods=12).sum())
    data_q1 = dummy_df(freq="M", ts_type="Flujo")
    data_q2 = dummy_df(freq="M", ts_type="Flujo")
    data_dict = {"data_q1": data_q1, "data_q2": data_q2}
    session = Session(loc_dir=TEST_DIR, dataset=data_dict)
    trf_inter = session.rolling(operation="sum").dataset
    trf_inter["data_q1"].columns = trf_inter["data_q2"].columns = data_q1.columns
    assert trf_inter["data_q1"].equals(data_q1.rolling(window=12,
                                                       min_periods=12).sum())
    assert trf_inter["data_q2"].equals(data_q2.rolling(window=12,
                                                       min_periods=12).sum())
    with pytest.warns(UserWarning):
        data_wrong = dummy_df(freq="M", ts_type="Stock")
        transform.rolling(data_wrong, periods=4, operation="average")
