import datetime as dt
from os import path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from econuy.retrieval import reserves, national_accounts
from econuy.session import Session
from econuy.utils import metadata, sqlutil
try:
    from tests.test_session import remove_clutter
except ImportError:
    from .test_session import remove_clutter

CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(path.dirname(CUR_DIR), "test-data")
TEST_CON = create_engine("sqlite://").connect()
sqlutil.insert_csvs(con=TEST_CON, directory=TEST_DIR)


def test_changes():
    remove_clutter()
    previous_data = pd.read_csv(path.join(TEST_DIR, "reserves_chg.csv"),
                                index_col=0, header=list(range(9)))
    metadata._set(previous_data)
    res = reserves.get_changes(
        update_loc=TEST_DIR, save_loc=TEST_DIR)
    previous_data.index = pd.to_datetime(previous_data.index)
    compare = res.loc[previous_data.index].round(4)
    compare.columns = previous_data.columns
    assert compare.equals(previous_data.round(4))
    remove_clutter()


def test_rxr_official():
    remove_clutter()
    session = Session(location=TEST_DIR)
    tcr = session.get(dataset="rxr_official").dataset
    assert isinstance(tcr, pd.DataFrame)
    assert tcr.index[0] == dt.date(2000, 1, 31)
    remove_clutter()


def test_rxr_custom():
    remove_clutter()
    session = Session(location=TEST_DIR)
    tcr = session.get(dataset="rxr_custom").dataset
    assert isinstance(tcr, pd.DataFrame)
    assert tcr.index[0] == dt.date(1979, 12, 31)
    assert len(tcr.columns) == 5
    avs = tcr.loc[(tcr.index >= "2010-01-01") &
                  (tcr.index <= "2010-12-31")].mean().values.round(1)
    arr = np.array([100] * 5, dtype="float64")
    assert np.all(avs == arr)
    remove_clutter()


def test_comm_index():
    remove_clutter()
    session = Session(location=TEST_DIR)
    comm = session.get(dataset="comm_index").dataset
    assert isinstance(comm, pd.DataFrame)
    assert comm.index[0] == dt.date(2002, 1, 31)
    assert comm.iloc[0][0] == 100
    remove_clutter()


def test_lin():
    remove_clutter()
    lin = national_accounts._lin_gdp(
        update_loc="test-data", save_loc="test-data")
    assert isinstance(lin, pd.DataFrame)
    assert (sorted(lin.columns.get_level_values("Moneda"))
            == sorted(["UYU", "USD"]))
    remove_clutter()


def test_nxr_daily():
    remove_clutter()
    previous_data = pd.read_csv(path.join(TEST_DIR, "nxr_daily.csv"),
                                index_col=0, header=list(range(9)))
    metadata._set(previous_data)
    previous_data.index = pd.to_datetime(previous_data.index)
    session = Session(location=TEST_DIR)
    nxr = session.get(dataset="nxr_daily").dataset
    compare = nxr.loc[previous_data.index].round(4)
    compare.columns = previous_data.columns
    assert compare.equals(previous_data.round(4))
    remove_clutter()


def test_nxr_monthly():
    remove_clutter()
    session = Session(location=TEST_DIR)
    nxr = session.get(dataset="nxr_m").dataset
    assert len(nxr.columns) == 2
    assert isinstance(nxr.index[0], pd._libs.tslibs.timestamps.Timestamp)
    remove_clutter()
