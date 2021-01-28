import datetime as dt
from os import path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from econuy.retrieval import economic_activity
from econuy.session import Session
from econuy.utils import metadata, sqlutil
try:
    from tests.test_session import remove_clutter
except ImportError:
    from .test_session import remove_clutter

CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(CUR_DIR, "test-data")
TEST_CON = create_engine("sqlite://").connect()
sqlutil.insert_csvs(con=TEST_CON, directory=TEST_DIR)


def test_reserves_changes():
    remove_clutter()
    session = Session(location=TEST_CON)
    previous_data = pd.read_csv(path.join(TEST_DIR, "reserves_changes.csv"),
                                index_col=0, header=list(range(9)),
                                float_precision="high")
    metadata._set(previous_data)
    res = session.get(dataset="reserves_changes").dataset
    previous_data.index = pd.to_datetime(previous_data.index)
    compare = res.loc[previous_data.index].round(3)
    compare.columns = previous_data.columns
    assert compare.equals(previous_data.round(3))
    session.only_get = True
    compare = session.get(dataset="reserves_changes").dataset
    assert res.round(3).equals(compare.round(3))
    remove_clutter()


def test_rxr_official():
    remove_clutter()
    session = Session(location=TEST_DIR)
    tcr = session.get(dataset="rxr_official").dataset
    assert isinstance(tcr, pd.DataFrame)
    assert tcr.index[0] == dt.date(2000, 1, 31)
    session.only_get = True
    compare = session.get(dataset="rxr_official").dataset
    assert tcr.round(4).equals(compare.round(4))
    remove_clutter()


def test_rxr_custom():
    remove_clutter()
    session = Session(location=TEST_DIR)
    tcr = session.get_custom(dataset="rxr_custom").dataset
    assert isinstance(tcr, pd.DataFrame)
    assert tcr.index[0] == dt.date(1979, 12, 31)
    assert len(tcr.columns) == 4
    avs = tcr.loc[(tcr.index >= "2010-01-01") &
                  (tcr.index <= "2010-12-31")].mean().values.round(1)
    arr = np.array([100] * 4, dtype="float64")
    assert np.all(avs == arr)
    session.only_get = True
    compare = session.get_custom(dataset="rxr_custom").dataset
    assert tcr.round(4).equals(compare.round(4))
    remove_clutter()


def test_comm_index():
    remove_clutter()
    session = Session(location=TEST_DIR)
    comm = session.get_custom(dataset="commodity_index").dataset
    assert isinstance(comm, pd.DataFrame)
    assert comm.index[0] == dt.date(2002, 1, 31)
    assert comm.iloc[0][0] == 100
    session.only_get = True
    compare = session.get_custom(
        dataset="commodity_index",
        only_get_prices=True).dataset
    assert compare.round(4).equals(comm.round(4))
    remove_clutter()


def test_lin():
    remove_clutter()
    lin = economic_activity._lin_gdp(update_loc=TEST_DIR, save_loc=TEST_DIR)
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
    session.only_get = True
    compare = session.get(dataset="nxr_daily").dataset
    assert compare.round(4).equals(nxr.round(4))
    session.only_get = False
    remove_clutter()


def test_nxr_monthly():
    remove_clutter()
    session = Session(location=TEST_DIR)
    nxr = session.get(dataset="nxr_monthly").dataset
    assert len(nxr.columns) == 2
    assert isinstance(nxr.index[0], pd._libs.tslibs.timestamps.Timestamp)
    remove_clutter()


def test_reserves():
    remove_clutter()
    session = Session(location=TEST_DIR)
    reserves = session.get(dataset="reserves").dataset
    assert len(reserves.columns) == 6
    assert isinstance(reserves.index[0], pd._libs.tslibs.timestamps.Timestamp)
    remove_clutter()
