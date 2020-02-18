from econuy import frequent
from econuy import transform


def test_inflation():
    inflation = frequent.inflation(update=None, save=None, name=None)
    cpi = inflation.iloc[:, [0]]
    inter = transform.chg_diff(cpi, period_op="inter")
    assert inflation.iloc[:, [1]].equals(inter)
    monthly = transform.chg_diff(cpi, period_op="last")
    assert inflation.iloc[:, [2]].equals(monthly)
    trend, seasadj = transform.decompose(cpi, trading=True, outlier=False)
    monthly_sa = transform.chg_diff(seasadj)
    assert inflation.iloc[:, [3]].equals(monthly_sa)
    monthly_trend = transform.chg_diff(trend)
    assert inflation.iloc[:, [4]].equals(monthly_trend)
