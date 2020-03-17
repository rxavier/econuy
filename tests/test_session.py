from os import listdir, remove, path
from typing import Tuple
from pathlib import Path

import pandas as pd
import pytest

from econuy import transform
from econuy.resources import columns
from econuy.resources.lstrings import fiscal_metadata
from econuy.retrieval import nxr
from econuy.session import Session
from .test_transform import dummy_df

CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(path.dirname(CUR_DIR), "test-data")


def remove_clutter(avoid: Tuple[str] = ("fx_ff.csv", "fx_spot_ff.csv",
                                        "reserves_chg.csv",
                                        "commodity_weights.csv")):
    [remove(path.join(TEST_DIR, x)) for x in listdir(TEST_DIR)
     if x not in avoid]
    return


def test_prices_inflation():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    inflation = session.get_tfm(dataset="inflation").dataset
    remove_clutter()
    prices = session.get(dataset="cpi").dataset
    remove_clutter()
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
    remove_clutter()


def test_exchange_rate():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    nxr_tfm = session.get_tfm(dataset="nxr", eop=True, sell=False,
                              seas_adj="trend", cum=1).dataset
    remove_clutter()
    nxr_ = session.get(dataset="nxr").dataset
    nxr_e_s = nxr_.iloc[:, [0]]
    nxr_e_s_trend, nxr_e_s_sa = transform.decompose(nxr_e_s, trading=True,
                                                    outlier=True)
    compare = pd.concat([nxr_e_s, nxr_e_s_trend], axis=1)
    compare.columns = nxr_tfm.columns
    assert compare.equals(nxr_tfm)
    remove_clutter()
    nxr_tfm = session.get_tfm(dataset="nxr", eop=False, sell=True,
                              seas_adj="seas", cum=1).dataset
    nxr_e_b = nxr_.iloc[:, [3]]
    nxr_e_b_trend, nxr_e_b_sa = transform.decompose(nxr_e_b, trading=True,
                                                    outlier=True)
    compare = pd.concat([nxr_e_b, nxr_e_b_sa], axis=1)
    compare.columns = nxr_tfm.columns
    assert compare.equals(nxr_tfm)
    remove_clutter()
    nxr_tfm = session.get_tfm(dataset="nxr", eop=False, sell=True,
                              seas_adj=None, cum=12).dataset
    nxr_a_s = nxr_.iloc[:, [3]]
    compare = transform.rolling(nxr_a_s, periods=12, operation="average")
    compare.columns = nxr_tfm.columns
    assert compare.equals(nxr_tfm)
    remove_clutter()
    with pytest.raises(ValueError):
        session.get_tfm(dataset="nxr", eop=False, sell=True,
                        seas_adj="wrong")
    remove_clutter()


def test_fiscal():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    fiscal_tfm = session.get_tfm(dataset="fiscal", aggregation="nfps",
                                 fss=True, unit="gdp").dataset
    remove_clutter()
    fiscal_ = session.get(dataset="fiscal").dataset
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
    compare_gdp = transform.convert_gdp(compare_gdp)
    compare_gdp.columns = fiscal_tfm.columns
    assert compare_gdp.equals(fiscal_tfm)
    remove_clutter()
    fiscal_tfm = session.get_tfm(dataset="fiscal", aggregation="nfps",
                                 fss=True, unit="usd", seas_adj=None).dataset
    compare_usd = transform.convert_usd(compare)
    compare_usd.columns = fiscal_tfm.columns
    assert compare_usd.equals(fiscal_tfm)
    remove_clutter()
    fiscal_tfm = session.get_tfm(dataset="fiscal", aggregation="nfps",
                                 fss=True, unit="real", seas_adj=None).dataset
    compare_real = transform.convert_real(compare)
    compare_real.columns = fiscal_tfm.columns
    assert compare_real.equals(fiscal_tfm)
    remove_clutter()
    start_date = "2010-01-31"
    end_date = "2010-12-31"
    fiscal_tfm = session.get_tfm(dataset="fiscal", aggregation="nfps",
                                 fss=True, unit="real_usd", seas_adj=None,
                                 start_date=start_date,
                                 end_date=end_date).dataset
    compare_real_usd = transform.convert_real(compare, start_date=start_date,
                                              end_date=end_date)
    xr = nxr.get(update=None, save=None)
    compare_real_usd = compare_real_usd.divide(
        xr[start_date:end_date].mean()[3])
    compare_real_usd.columns = fiscal_tfm.columns
    assert compare_real_usd.equals(fiscal_tfm)
    remove_clutter()
    fiscal_tfm = session.get_tfm(dataset="fiscal", aggregation="nfps",
                                 fss=True, unit=None, seas_adj="trend").dataset
    compare_trend, compare_sa = transform.decompose(compare, outlier=True,
                                                    trading=True)
    compare_trend.columns = fiscal_tfm.columns
    assert compare_trend.equals(fiscal_tfm)
    remove_clutter()
    fiscal_tfm = session.get_tfm(dataset="fiscal", aggregation="nfps",
                                 fss=True, unit=None, seas_adj=None,
                                 cum=12).dataset
    compare_roll = transform.rolling(compare, periods=12, operation="sum")
    compare_roll.columns = fiscal_tfm.columns
    assert compare_roll.equals(fiscal_tfm)
    remove_clutter()
    with pytest.raises(ValueError):
        session.get_tfm(dataset="fiscal", aggregation="nfps",
                        unit="wrong")
    with pytest.raises(ValueError):
        session.get_tfm(dataset="fiscal", aggregation="nfps",
                        seas_adj="wrong")
    with pytest.raises(ValueError):
        session.get_tfm(dataset="fiscal", aggregation="wrong")
    remove_clutter()


def test_labor():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    labor_tfm = session.get_tfm(dataset="labor", seas_adj="trend").dataset
    remove_clutter()
    labor_ = session.get(dataset="labor").dataset
    labor_trend, labor_sa = transform.decompose(labor_, outlier=True,
                                                trading=True)
    compare = pd.concat([labor_, labor_trend], axis=1)
    compare.columns = labor_tfm.columns
    assert compare.equals(labor_tfm)
    remove_clutter()
    labor_tfm = session.get_tfm(dataset="labor", seas_adj="seas").dataset
    compare = pd.concat([labor_, labor_sa], axis=1)
    compare.columns = labor_tfm.columns
    assert compare.equals(labor_tfm)
    remove_clutter()
    with pytest.raises(ValueError):
        session.get_tfm(dataset="labor", seas_adj="wrong")


def test_naccounts():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    na_tfm = session.get_tfm(dataset="na", supply=True, real=True, index=False,
                             off_seas_adj=False, usd=False, cum=1,
                             seas_adj=None, variation=None).dataset
    remove_clutter()
    na_ = session.get(dataset="na").dataset
    compare = na_["ind_con_nsa"]
    compare.columns = na_tfm.columns
    assert compare.equals(na_tfm)
    remove_clutter()
    na_tfm = session.get_tfm(dataset="na", supply=True, real=False,
                             index=False, off_seas_adj=False, usd=False, cum=1,
                             seas_adj=None, variation=None).dataset
    compare = na_["ind_cur_nsa"]
    compare.columns = na_tfm.columns
    assert compare.equals(na_tfm)
    remove_clutter()
    na_tfm = session.get_tfm(dataset="na", supply=False, real=True,
                             index=False, off_seas_adj=False, usd=False, cum=1,
                             seas_adj=None, variation=None).dataset
    compare = na_["gas_con_nsa"]
    compare.columns = na_tfm.columns
    assert compare.equals(na_tfm)
    remove_clutter()
    na_tfm = session.get_tfm(dataset="na", supply=False, real=True,
                             index=False, off_seas_adj=False, usd=True, cum=4,
                             seas_adj=None, variation="inter").dataset
    compare_mult = transform.convert_usd(compare)
    compare_mult = transform.rolling(compare_mult, periods=4, operation="sum")
    compare_mult = transform.chg_diff(compare_mult, operation="chg",
                                      period_op="inter")
    compare_mult.columns = na_tfm.columns
    assert compare_mult.equals(na_tfm)
    remove_clutter()
    na_tfm = session.get_tfm(dataset="na", supply=False, real=True,
                             index=False, off_seas_adj=False, usd=False, cum=1,
                             seas_adj="trend", variation=None).dataset
    compare_trend, compare_sa = transform.decompose(compare, trading=True,
                                                    outlier=True)
    compare_trend.columns = na_tfm.columns
    assert compare_trend.equals(na_tfm)
    remove_clutter()
    na_tfm = session.get_tfm(dataset="na", supply=False, real=True,
                             index=False, off_seas_adj=False, usd=False, cum=1,
                             seas_adj="seas", variation=None).dataset
    compare_sa.columns = na_tfm.columns
    assert compare_sa.equals(na_tfm)
    with pytest.raises(KeyError):
        session.get_tfm(dataset="na", supply=False, real=True, index=True,
                        off_seas_adj=True, usd=False, cum=1,
                        seas_adj=None, variation=None)
    with pytest.raises(ValueError):
        session.get_tfm(dataset="na", supply=False, real=True, index=False,
                        off_seas_adj=False, usd=False, cum=1,
                        seas_adj="wrong", variation=None)
    with pytest.raises(ValueError):
        session.get_tfm(dataset="na", supply=False, real=True, index=False,
                        off_seas_adj=False, usd=False, cum=1,
                        seas_adj=None, variation="wrong")
    remove_clutter()


def test_edge():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    session.get(dataset="cpi", update=False, save=False)
    with pytest.raises(ValueError):
        session.get(dataset="wrong")
    with pytest.raises(ValueError):
        session.get_tfm(dataset="wrong")
    remove_clutter()
    Session(loc_dir="new_dir")
    assert path.isdir("new_dir")
    Session(loc_dir=TEST_DIR).get_tfm(dataset="inflation",
                                      update=False, save=False)
    remove_clutter()


def test_save():
    remove_clutter()
    data = dummy_df(freq="M")
    session = Session(loc_dir=TEST_DIR, dataset=data)
    session.save(name="test_save")
    assert path.isfile(Path(TEST_DIR) / "test_save.csv")
    remove_clutter()
    data = dummy_df(freq="M")
    session = Session(loc_dir=TEST_DIR, dataset={"data1": data, "data2": data})
    session.save(name="test_save")
    assert path.isfile(Path(TEST_DIR) / "test_save_data1.csv")
    assert path.isfile(Path(TEST_DIR) / "test_save_data2.csv")
    remove_clutter()
    session.loc_dir = "new_dir"
    session.save(name="test_save")
    assert path.isfile(Path(session.loc_dir) / "test_save_data1.csv")
    assert path.isfile(Path(session.loc_dir) / "test_save_data2.csv")
    session.dataset = data
    session.save(name="test_save")
    assert path.isfile(Path(session.loc_dir) / "test_save.csv")
    remove_clutter()


def test_logging(caplog):
    remove_clutter()
    caplog.clear()
    Session(loc_dir=TEST_DIR, log="test")
    assert path.isfile(path.join(TEST_DIR, "test.log"))
    remove_clutter()
    caplog.clear()
    Session(loc_dir=TEST_DIR, log=2)
    assert path.isfile(path.join(TEST_DIR, "info.log"))
    remove_clutter()
    caplog.clear()
    with pytest.raises(ValueError):
        Session(loc_dir=TEST_DIR, log=5)
    remove_clutter()
    caplog.clear()
    Session(loc_dir=TEST_DIR, log=1)
    assert "Logging method: console" in caplog.text
    assert "Logging method: console and file" not in caplog.text
    assert not path.isfile(path.join(TEST_DIR, "info.log"))
    caplog.clear()
    remove_clutter()
    Session(loc_dir=TEST_DIR, log=0)
    assert caplog.text is ""
