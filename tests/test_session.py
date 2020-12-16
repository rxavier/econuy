import logging
import shutil
import datetime as dt
from os import listdir, remove, path
from pathlib import Path
from typing import Tuple

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect

from econuy import transform
from econuy.session import Session
from econuy.utils import metadata, sqlutil
from econuy.utils.lstrings import fiscal_metadata
try:
    from tests.test_transform import dummy_df
except ImportError:
    from .test_transform import dummy_df

CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(CUR_DIR, "test-data")
TEST_CON = create_engine("sqlite://").connect()
sqlutil.insert_csvs(con=TEST_CON, directory=TEST_DIR)


def remove_clutter(avoid: Tuple[str] = ("reserves_changes.csv",
                                        "commodity_weights.csv",
                                        "nxr_daily.csv")):
    [remove(path.join(TEST_DIR, x)) for x in listdir(TEST_DIR)
     if x not in avoid]

    for table in inspect(TEST_CON).get_table_names():
        if table not in [f[:-4] for f in avoid]:
            TEST_CON.engine.execute(f'DROP TABLE IF EXISTS "{table}"')

    return


def test_prices_inflation():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    measures = session.get_custom(dataset="cpi_measures").dataset
    remove_clutter()
    prices = session.get(dataset="cpi").dataset
    prices = prices.loc[prices.index >= "1997-03-31"]
    prices = transform.base_index(prices, start_date="2010-12-01",
                                  end_date="2010-12-31")
    compare = measures.iloc[:, [0]]
    compare.columns = prices.columns
    assert compare.equals(prices)


def test_fiscal():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    fiscal_tfm = session.get_custom(dataset="balance_fss").dataset
    remove_clutter()
    fiscal_ = session.get(dataset="balance").dataset
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
    metadata._set(compare, area="Sector público",
                  currency="UYU", inf_adj="No", unit="No",
                  seas_adj="NSA", ts_type="Flujo", cumperiods=1)
    compare.columns = fiscal_tfm["balance_fss_nfps_fssadj"].columns
    assert compare.equals(fiscal_tfm["balance_fss_nfps_fssadj"])
    remove_clutter()
    fiscal_ = session.get(dataset="balance").dataset
    session.only_get = True
    compare = session.get(dataset="balance").dataset
    for v, v2 in zip(fiscal_.values(), compare.values()):
        assert v.round(4).equals(v2.round(4))
    remove_clutter()
    session.only_get = False


def test_labor():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    labor_tfm = session.get_custom(dataset="rates_people").dataset
    labor_tfm = labor_tfm.iloc[:, [0, 1, 2]].loc[labor_tfm.index >= "2006-01-01"]
    remove_clutter()
    labor_ = session.get(dataset="labor").dataset.iloc[:, [0, 3, 6]]
    session.only_get = True
    compare = session.get(dataset="labor").dataset.iloc[:, [0, 3, 6]]
    compare.columns = labor_tfm.columns
    assert labor_tfm.round(4).equals(compare.round(4))
    remove_clutter()
    labor_tfm = session.get_custom(dataset="rates_people").dataset
    labor_tfm = labor_tfm.loc[labor_tfm.index >= "2006-01-01"]
    compare = labor_.iloc[:, 0].div(labor_.iloc[:, 1]).round(4)
    compare_2 = labor_tfm.iloc[:, 3].div(labor_tfm.iloc[:, 4]).round(4)
    assert compare.equals(compare_2)
    compare = labor_tfm.iloc[:, 3].mul(labor_.iloc[:, 2]).div(100).round(4)
    assert compare.equals(labor_tfm.iloc[:, 5].round(4))
    remove_clutter()
    labor_ext = session.get_custom(dataset="rates_people").dataset
    assert len(labor_ext) > len(labor_tfm)
    remove_clutter()


def test_wages():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    real_wages = session.get(dataset="real_wages").dataset
    nominal_wages = session.get(dataset="wages").dataset
    compare = transform.convert_real(nominal_wages, update_loc=TEST_CON)
    compare = transform.base_index(compare, start_date="2008-07-31")
    compare.columns = real_wages.columns
    assert compare.equals(real_wages)


def test_naccounts():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    na_ = session.get(dataset="naccounts").dataset
    assert isinstance(na_, dict)
    assert len(na_) == 6
    remove_clutter()


def test_deposits():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    deposits = session.get(dataset="deposits").dataset
    assert deposits.index[0] == dt.datetime(1998, 12, 31)
    remove_clutter()


def test_credit():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    credit = session.get(dataset="credit").dataset
    assert credit.index[0] == dt.datetime(1998, 12, 31)
    remove_clutter()


def test_rates():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    rates = session.get(dataset="interest_rates").dataset
    assert rates.index[0] == dt.datetime(1998, 1, 31)
    remove_clutter()


def test_taxes():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    taxes = session.get(dataset="taxes").dataset
    assert taxes.index[0] == dt.datetime(1982, 1, 31)
    assert len(taxes.columns) == 38
    assert taxes[-1:].count().sum() == 10
    remove_clutter()


def test_call():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    call_rate = session.get(dataset="call").dataset
    assert call_rate.index[0] == dt.datetime(2002, 1, 2)
    remove_clutter()


def test_hours():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    hours = session.get(dataset="hours").dataset
    assert hours.index[0] == dt.datetime(2006, 1, 31)
    assert len(hours.columns) == 17
    remove_clutter()


def test_bonds():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    bonds = session.get_custom(dataset="bonds").dataset
    assert bonds.index[0] == dt.datetime(2003, 6, 2)
    remove_clutter()


def test_income():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    household = session.get(dataset="household_income").dataset
    assert household.index[0] == dt.datetime(2006, 1, 31)
    assert len(household.columns) == 5
    remove_clutter()
    capita = session.get(dataset="capita_income").dataset
    assert capita.index[0] == dt.datetime(2006, 1, 31)
    assert len(capita.columns) == 5
    remove_clutter()


def test_sectors():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    cattle = session.get(dataset="cattle").dataset
    assert cattle.index[0] == dt.datetime(2005, 1, 2)
    assert len(cattle.columns) == 6
    remove_clutter()
    milk = session.get(dataset="milk").dataset
    assert milk.index[0] == dt.datetime(2002, 1, 31)
    assert len(milk.columns) == 1
    remove_clutter()
    cement = session.get(dataset="cement").dataset
    assert cement.index[0] == dt.datetime(1990, 1, 31)
    assert len(cement.columns) == 3
    remove_clutter()


def test_energy():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    diesel = session.get(dataset="diesel").dataset
    assert diesel.index[0] == dt.datetime(2004, 1, 31)
    assert len(diesel.columns) == 21
    remove_clutter()
    gasoline = session.get(dataset="gasoline").dataset
    assert gasoline.index[0] == dt.datetime(2004, 1, 31)
    assert len(gasoline.columns) == 21
    remove_clutter()
    electricity = session.get(dataset="electricity").dataset
    assert electricity.index[0] == dt.datetime(2000, 1, 31)
    assert len(electricity.columns) == 8
    remove_clutter()


def test_confidence():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    consumer = session.get(dataset="consumer_confidence").dataset
    assert consumer.index[0] == dt.datetime(2007, 8, 31)
    assert len(consumer.columns) == 4
    remove_clutter()


def test_bond_index():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    risk = session.get(dataset="sovereign_risk").dataset
    assert risk.index[0] == dt.datetime(1999, 1, 1)
    assert len(risk.columns) == 1
    remove_clutter()


def test_global_gdp():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    global_gdp = session.get_custom(dataset="global_gdp").dataset
    assert global_gdp.index[0] == dt.datetime(1947, 3, 31)
    assert len(global_gdp.columns) == 4
    remove_clutter()


def test_global_stocks():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    stocks = session.get_custom(dataset="global_stocks").dataset
    assert stocks.index[0] == dt.datetime(1927, 12, 30)
    assert len(stocks.columns) == 4
    remove_clutter()


def test_global_policy_rates():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    policy = session.get_custom(dataset="global_policy_rates").dataset
    assert policy.index[0] == dt.datetime(1946, 1, 1)
    assert len(policy.columns) == 4
    remove_clutter()


def test_global_long_rates():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    long = session.get_custom(dataset="global_long_rates").dataset
    assert long.index[0] == dt.datetime(1962, 1, 2)
    assert len(long.columns) == 8
    remove_clutter()


def test_global_nxr():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    currencies = session.get_custom(dataset="global_nxr").dataset
    assert currencies.index[0] == dt.datetime(1971, 1, 4)
    assert len(currencies.columns) == 4
    remove_clutter()


def test_regional_gdp():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    gdp = session.get_custom(dataset="regional_gdp").dataset
    assert gdp.index[0] == dt.datetime(1993, 3, 31)
    assert len(gdp.columns) == 2
    remove_clutter()


def test_regional_monthly_gdp():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    gdp = session.get_custom(dataset="regional_monthly_gdp").dataset
    assert gdp.index[0] == dt.datetime(2003, 1, 31)
    assert len(gdp.columns) == 2
    remove_clutter()


def test_regional_cpi():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    cpi = session.get_custom(dataset="regional_cpi").dataset
    assert cpi.index[0] == dt.datetime(1970, 1, 31)
    assert len(cpi.columns) == 2
    remove_clutter()


def test_regional_embi_spreads():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    spreads = session.get_custom(dataset="regional_embi_spreads").dataset
    assert spreads.index[0] == dt.datetime(1998, 12, 11)
    assert len(spreads.columns) == 3
    remove_clutter()


def test_regional_embi_yields():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    yields = session.get_custom(dataset="regional_embi_yields").dataset
    assert yields.index[0] == dt.datetime(1998, 12, 11)
    assert len(yields.columns) == 3
    remove_clutter()


def test_regional_nxr():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    nxr = session.get_custom(dataset="regional_nxr").dataset
    assert nxr.index[0] == dt.datetime(2002, 4, 9)
    assert len(nxr.columns) == 3
    remove_clutter()


def test_regional_rxr():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    rxr = session.get_custom(dataset="regional_rxr").dataset
    assert rxr.index[0] == dt.datetime(1970, 1, 31)
    assert len(rxr.columns) == 2
    remove_clutter()


def test_regional_policy_rates():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    policy = session.get_custom(dataset="regional_policy_rates").dataset
    assert policy.index[0] == dt.datetime(1986, 6, 4)
    assert len(policy.columns) == 2
    remove_clutter()


def test_regional_stocks():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    stocks = session.get_custom(dataset="regional_stocks").dataset
    assert stocks.index[0] == dt.datetime(2002, 4, 9)
    assert len(stocks.columns) == 2
    remove_clutter()


def test_trade():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    tb_ = session.get(dataset="trade").dataset
    assert isinstance(tb_, dict)
    assert len(tb_) == 12
    remove_clutter()
    net = session.get_custom(dataset="net_trade").dataset
    compare = (tb_["trade_x_dest_val"].
               rename(columns={"Total exportaciones": "Total"})
               - tb_["trade_m_orig_val"].
               rename(columns={"Total importaciones": "Total"}))
    compare.columns = net.columns
    assert net.equals(compare)
    remove_clutter()
    net = session.get_custom(dataset="terms_of_trade").dataset
    compare = (tb_["trade_x_dest_pri"].
               rename(columns={"Total exportaciones": "Total"})
               / tb_["trade_m_orig_pri"].
               rename(columns={"Total importaciones": "Total"}))
    compare = compare.loc[:, ["Total"]]
    compare = transform.base_index(compare, start_date="2005-01-01",
                                   end_date="2005-12-31")
    compare.columns = net.columns
    assert net.equals(compare)
    remove_clutter()


def test_public_debt():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    debt = session.get(dataset="public_debt").dataset
    assert isinstance(debt, dict)
    assert len(debt) == 4
    net_debt = session.get_custom(dataset="net_public_debt").dataset
    assert net_debt.index.__len__() < debt["gps"].index.__len__()
    remove_clutter()


def test_industrial_production():
    remove_clutter()
    session = Session(location=TEST_CON)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    indprod = session.get(dataset="industrial_production").dataset
    remove_clutter()
    core = session.get_custom(dataset="core_industrial").dataset
    core = core.loc[:, ["Núcleo industrial"]]
    compare = (indprod["Industrias manufactureras sin refinería"]
               - indprod.loc[:, 1549] * 0.082210446
               - indprod.loc[:, 2101] * 0.008097608)
    compare = pd.concat([compare], keys=["Núcleo industrial"],
                        names=["Indicador"], axis=1)
    compare = transform.base_index(compare, start_date="2006-01-01",
                                   end_date="2006-12-31")
    compare.columns = core.columns
    assert core.round(2).equals(compare.round(2))
    remove_clutter()


def test_edge():
    remove_clutter()
    session = Session(location=TEST_DIR)
    assert isinstance(session, Session)
    assert isinstance(session.dataset, pd.DataFrame)
    session.get(dataset="cpi", update=False, save=False)
    with pytest.raises(ValueError):
        session.get(dataset="wrong")
    with pytest.raises(ValueError):
        session.get_custom(dataset="wrong")
    remove_clutter()
    session = Session(location="new_dir")
    assert path.isdir("new_dir")
    shutil.rmtree(session.location)
    Session(location=TEST_DIR).get_custom(dataset="cpi_measures",
                                          update=False, save=False)
    remove_clutter()


def test_save():
    remove_clutter()
    data = dummy_df(freq="M")
    session = Session(location=TEST_DIR, dataset=data)
    session.save(name="test_save")
    assert path.isfile(Path(TEST_DIR) / "test_save.csv")
    remove_clutter()
    data = dummy_df(freq="M")
    session = Session(location=TEST_DIR, dataset={
        "data1": data, "data2": data})
    session.save(name="test_save")
    assert path.isfile(Path(TEST_DIR) / "test_save_data1.csv")
    assert path.isfile(Path(TEST_DIR) / "test_save_data2.csv")
    remove_clutter()
    session.location = "new_dir"
    session.save(name="test_save")
    assert path.isfile(Path(session.location) / "test_save_data1.csv")
    assert path.isfile(Path(session.location) / "test_save_data2.csv")
    session.dataset = data
    session.save(name="test_save")
    assert path.isfile(Path(session.location) / "test_save.csv")
    remove_clutter()
    shutil.rmtree(session.location)


def test_logging(caplog):
    remove_clutter()
    caplog.clear()
    s = Session(location=TEST_DIR, log="test")
    test_path = path.join(TEST_DIR, "test.log")
    assert path.isfile(test_path)
    logging.shutdown()
    caplog.clear()
    s = Session(location=TEST_DIR, log=2)
    info_path = path.join(TEST_DIR, "info.log")
    assert path.isfile(info_path)
    logging.shutdown()
    caplog.clear()
    with pytest.raises(ValueError):
        Session(location=TEST_DIR, log=5)
    caplog.clear()
    remove_clutter()
    Session(location=TEST_DIR, log=1)
    assert "Logging method: console" in caplog.text
    assert "Logging method: console and file" not in caplog.text
    assert not path.isfile(path.join(TEST_DIR, "info.log"))
    caplog.clear()
    Session(location=TEST_DIR, log=0)
    assert caplog.text == ""
    logging.shutdown()
