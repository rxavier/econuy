import shutil

import pandas as pd
import pytest

from econuy import frequent
from econuy import transform
from econuy.retrieval import (cpi, nxr, fiscal_accounts,
                              labor, national_accounts)
from econuy.resources.lstrings import fiscal_metadata
from econuy.resources import columns


def test_cpi_inflation():
    inflation = frequent.inflation(update="test-data", save="test-data",
                                   name=None)
    prices = cpi.get(update=None, save="test-data", name=None)
    inter = transform.chg_diff(prices, period_op="inter")
    compare = inflation.iloc[:, [1]]
    inter.columns = compare.columns
    assert compare.equals(inter)
    monthly = transform.chg_diff(prices, period_op="last")
    compare = inflation.iloc[:, [2]]
    monthly.columns = compare.columns
    assert compare.equals(monthly)
    trend, seasadj = transform.decompose(prices, trading=True, outlier=False)
    monthly_sa = transform.chg_diff(seasadj)
    compare = inflation.iloc[:, [3]]
    monthly_sa.columns = compare.columns
    assert compare.equals(monthly_sa)
    monthly_trend = transform.chg_diff(trend)
    compare = inflation.iloc[:, [4]]
    monthly_trend.columns = compare.columns
    assert monthly_trend.equals(monthly_trend)
    shutil.rmtree("test-data")


def test_nxr():
    nxr_tfm = frequent.exchange_rate(eop=True, sell=False, seas_adj="trend",
                                     cum=1, update=None,
                                     save="test-data", name=None)
    nxr_ = nxr.get(update=None, save="test-data", name=None)
    nxr_e_s = nxr_.iloc[:, [0]]
    nxr_e_s_trend, nxr_e_s_sa = transform.decompose(nxr_e_s, trading=True,
                                                    outlier=True)
    compare = pd.concat([nxr_e_s, nxr_e_s_trend], axis=1)
    compare.columns = nxr_tfm.columns
    assert compare.equals(nxr_tfm)
    nxr_tfm = frequent.exchange_rate(eop=False, sell=True, seas_adj="seas",
                                     cum=1, update=None,
                                     save="test-data", name=None)
    nxr_e_b = nxr_.iloc[:, [3]]
    nxr_e_b_trend, nxr_e_b_sa = transform.decompose(nxr_e_b, trading=True,
                                                    outlier=True)
    compare = pd.concat([nxr_e_b, nxr_e_b_sa], axis=1)
    compare.columns = nxr_tfm.columns
    assert compare.equals(nxr_tfm)
    nxr_tfm = frequent.exchange_rate(eop=False, sell=True, seas_adj=None,
                                     cum=12, update=None,
                                     save="test-data", name=None)
    nxr_a_s = nxr_.iloc[:, [3]]
    compare = transform.rolling(nxr_a_s, periods=12, operation="average")
    compare.columns = nxr_tfm.columns
    assert compare.equals(nxr_tfm)
    shutil.rmtree("test-data")


def test_fiscal():
    fiscal_tfm = frequent.fiscal(aggregation="nfps", fss=True, unit="gdp",
                                 update=None, save="test-data", name=None,
                                 seas_adj=None)
    fiscal_ = fiscal_accounts.get(update=None, save="test-data", name=None)
    nfps = fiscal_["nfps"]
    gc = fiscal_["gc-bps"]
    proc = pd.DataFrame(index=nfps.index)
    proc["Ingresos: SPNF-SPC"] = nfps["Ingresos: SPNF"]
    proc["Egresos: Primarios SPNF-SPC"] = nfps["Egresos: Primarios SPNF"]
    proc["Egresos: Inversiones SPNF-SPC"] = nfps["Egresos: Inversiones"]
    proc["Intereses: SPNF"] = nfps["Intereses: Totales"]
    proc["Egresos: Totales SPNF"] = (proc["Egresos: Primarios SPNF-SPC"]
                                     + proc["Intereses: SPNF"])
    proc["Resultado: Primario intendencias"] = nfps[
        "Resultado: Primario intendencias"
    ]
    proc["Resultado: Primario BSE"] = nfps["Resultado: Primario BSE"]
    proc["Resultado: Primario SPNF"] = nfps["Resultado: Primario SPNF"]
    proc["Resultado: Global SPNF"] = nfps["Resultado: Global SPNF"]

    proc["Ingresos: FSS"] = gc["Ingresos: FSS"]
    proc["Intereses: FSS"] = gc["Intereses: BPS-FSS"]
    proc["Ingresos: SPNF-SPC aj. FSS"] = (proc["Ingresos: SPNF-SPC"]
                                          - proc["Ingresos: FSS"])
    proc["Intereses: SPNF aj. FSS"] = (proc["Intereses: SPNF"]
                                       - proc["Intereses: FSS"])
    proc["Egresos: Totales SPNF aj. FSS"] = (proc["Egresos: Totales SPNF"]
                                             - proc["Intereses: FSS"])
    proc["Resultado: Primario SPNF aj. FSS"] = (
            proc["Resultado: Primario SPNF"]
            - proc["Ingresos: FSS"])
    proc["Resultado: Global SPNF aj. FSS"] = (proc["Resultado: Global SPNF"]
                                              - proc["Ingresos: FSS"]
                                              + proc["Intereses: FSS"])

    cols = fiscal_metadata["nfps"][True]
    compare = proc.loc[:, cols]
    columns._setmeta(compare, area="Cuentas fiscales y deuda",
                     currency="UYU", inf_adj="No", index="No",
                     seas_adj="NSA", ts_type="Flujo", cumperiods=1)
    compare_gdp = transform.rolling(compare, periods=12, operation="sum")
    compare_gdp = transform.convert_gdp(compare_gdp, hifreq=True)

    compare_gdp.columns = fiscal_tfm.columns
    assert compare_gdp.equals(fiscal_tfm)
    fiscal_tfm = frequent.fiscal(aggregation="nfps", fss=True, unit="usd",
                                 update=None, save="test-data", name=None,
                                 seas_adj=None)
    compare_usd = transform.convert_usd(compare)
    compare_usd.columns = fiscal_tfm.columns
    assert compare_usd.equals(fiscal_tfm)
    fiscal_tfm = frequent.fiscal(aggregation="nfps", fss=True, unit="real",
                                 update=None, save="test-data", name=None,
                                 seas_adj=None)
    compare_real = transform.convert_real(compare)
    compare_real.columns = fiscal_tfm.columns
    assert compare_real.equals(fiscal_tfm)
    start_date = "2010-01-31"
    end_date = "2010-12-31"
    fiscal_tfm = frequent.fiscal(aggregation="nfps", fss=True, unit="real usd",
                                 update=None, save="test-data", name=None,
                                 seas_adj=None, start_date=start_date,
                                 end_date=end_date)
    compare_real_usd = transform.convert_real(compare, start_date=start_date,
                                              end_date=end_date)
    xr = nxr.get(update=None, save=None)
    compare_real_usd = compare_real_usd.divide(
        xr[start_date:end_date].mean()[3])
    compare_real_usd.columns = fiscal_tfm.columns
    assert compare_real_usd.equals(fiscal_tfm)
    fiscal_tfm = frequent.fiscal(aggregation="nfps", fss=True, unit=None,
                                 update=None, save="test-data", name=None,
                                 seas_adj="trend")
    compare_trend, compare_sa = transform.decompose(compare, outlier=True,
                                                    trading=True)
    compare_trend.columns = fiscal_tfm.columns
    assert compare_trend.equals(fiscal_tfm)
    fiscal_tfm = frequent.fiscal(aggregation="nfps", fss=True, unit=None,
                                 update=None, save="test-data", name=None,
                                 seas_adj=None, cum=12)
    compare_roll = transform.rolling(compare, periods=12, operation="sum")
    compare_roll.columns = fiscal_tfm.columns
    assert compare_roll.equals(fiscal_tfm)
    shutil.rmtree("test-data")


def test_labor():
    labor_tfm = frequent.labor_mkt(seas_adj="trend", update=None,
                                   save="test-data", name=None)
    labor_ = labor.get(update=None, save="test-data", name=None)
    labor_trend, labor_sa = transform.decompose(labor_, outlier=True,
                                                trading=True)
    compare = pd.concat([labor_, labor_trend], axis=1)
    compare.columns = labor_tfm.columns
    assert compare.equals(labor_tfm)
    labor_tfm = frequent.labor_mkt(seas_adj="seas", update=None,
                                   save="test-data", name=None)
    compare = pd.concat([labor_, labor_sa], axis=1)
    compare.columns = labor_tfm.columns
    assert compare.equals(labor_tfm)
    shutil.rmtree("test-data")


def test_naccounts():
    na_tfm = frequent.nat_accounts(supply=True, real=True, index=False,
                                   seas_adj=False, usd=False, cum=1,
                                   cust_seas_adj=None,
                                   variation=None, update=None,
                                   save="test-data", name=None)
    na_ = national_accounts.get(update=None, save="test-data", name=None)
    compare = na_["ind_con_nsa"]
    compare.columns = na_tfm.columns
    assert compare.equals(na_tfm)
    na_tfm = frequent.nat_accounts(supply=True, real=False, index=False,
                                   seas_adj=False, usd=False, cum=1,
                                   cust_seas_adj=None,
                                   variation=None, update=None,
                                   save="test-data", name=None)
    compare = na_["ind_cur_nsa"]
    compare.columns = na_tfm.columns
    assert compare.equals(na_tfm)
    na_tfm = frequent.nat_accounts(supply=False, real=True, index=False,
                                   seas_adj=False, usd=False, cum=1,
                                   cust_seas_adj=None,
                                   variation=None, update=None,
                                   save="test-data", name=None)
    compare = na_["gas_con_nsa"]
    compare.columns = na_tfm.columns
    assert compare.equals(na_tfm)
    na_tfm = frequent.nat_accounts(supply=False, real=True, index=False,
                                   seas_adj=False, usd=True, cum=4,
                                   cust_seas_adj=None,
                                   variation="inter", update=None,
                                   save="test-data", name=None)
    compare_mult = transform.convert_usd(compare)
    compare_mult = transform.rolling(compare_mult, periods=4, operation="sum")
    compare_mult = transform.chg_diff(compare_mult, operation="chg",
                                      period_op="inter")
    compare_mult.columns = na_tfm.columns
    assert compare_mult.equals(na_tfm)
    na_tfm = frequent.nat_accounts(supply=False, real=True, index=False,
                                   seas_adj=False, usd=False, cum=1,
                                   cust_seas_adj="trend",
                                   variation=None, update=None,
                                   save="test-data", name=None)
    compare_trend, compare_sa = transform.decompose(compare, trading=True,
                                                    outlier=True)
    compare_trend.columns = na_tfm.columns
    assert compare_trend.equals(na_tfm)
    na_tfm = frequent.nat_accounts(supply=False, real=True, index=False,
                                   seas_adj=False, usd=False, cum=1,
                                   cust_seas_adj="seas",
                                   variation=None, update=None,
                                   save="test-data", name=None)
    compare_sa.columns = na_tfm.columns
    assert compare_sa.equals(na_tfm)
    with pytest.raises(KeyError):
        frequent.nat_accounts(supply=False, real=True, index=True,
                              seas_adj=True, usd=False, cum=1,
                              cust_seas_adj=None,
                              variation=None, update=None,
                              save="test-data", name=None)
    with pytest.raises(ValueError):
        frequent.nat_accounts(supply=False, real=True, index=False,
                              seas_adj=False, usd=False, cum=1,
                              cust_seas_adj="wrong",
                              variation=None, update=None,
                              save="test-data", name=None)
    with pytest.raises(ValueError):
        frequent.nat_accounts(supply=False, real=True, index=False,
                              seas_adj=False, usd=False, cum=1,
                              cust_seas_adj=None,
                              variation="wrong", update=None,
                              save="test-data", name=None)
    shutil.rmtree("test-data")
