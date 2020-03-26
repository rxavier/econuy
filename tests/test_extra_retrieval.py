import datetime as dt
from os import path

import numpy as np
import pandas as pd

from econuy.session import Session
from econuy.retrieval import reserves, national_accounts
from econuy.resources import columns
from .test_session import remove_clutter

CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(path.dirname(CUR_DIR), "test-data")


def test_changes():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    previous_data = pd.read_csv(path.join(TEST_DIR, "reserves_chg.csv"),
                                index_col=0, header=list(range(9)))
    columns._setmeta(previous_data)
    res = session.get(dataset="reserves").dataset
    previous_data.index = pd.to_datetime(previous_data.index)
    compare = res.loc[previous_data.index].round(4)
    compare.columns = previous_data.columns
    assert compare.equals(previous_data.round(4))
    remove_clutter()


def test_ff():
    remove_clutter()
    previous_data = pd.read_csv(path.join(TEST_DIR, "fx_ff.csv"),
                                index_col=0, header=list(range(9)))
    columns._setmeta(previous_data)
    compare = previous_data.iloc[0:-30]
    ff = reserves.get_fut_fwd(update=TEST_DIR, name=None, save=TEST_DIR)
    assert len(ff) > len(compare)
    remove_clutter()


def test_ops():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    ops = session.get(dataset="fx_ops").dataset
    assert isinstance(ops, pd.DataFrame)
    remove_clutter()


def test_rxr_official():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    tcr = session.get(dataset="rxr_official").dataset
    assert isinstance(tcr, pd.DataFrame)
    assert tcr.index[0] == dt.date(2000, 1, 31)
    remove_clutter()


def test_rxr_custom():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    tcr = session.get(dataset="rxr_custom").dataset
    assert isinstance(tcr, pd.DataFrame)
    assert tcr.index[0] == dt.date(1979, 12, 31)
    assert len(tcr.columns) == 5
    avs = tcr.loc[(tcr.index >= "2010-01-01") &
                  (tcr.index <= "2010-12-31")].mean().values.round(1)
    arr = np.array([100]*5, dtype="float64")
    assert np.all(avs == arr)
    remove_clutter()


def test_comm_index():
    remove_clutter()
    session = Session(loc_dir=TEST_DIR)
    comm = session.get(dataset="comm_index").dataset
    assert isinstance(comm, pd.DataFrame)
    assert comm.index[0] == dt.date(2002, 1, 31)
    assert comm.iloc[0][0] == 1
    remove_clutter()


def test_lin():
    remove_clutter()
    lin = national_accounts._lin_gdp(update="test-data", save="test-data")
    assert isinstance(lin, pd.DataFrame)
    assert (sorted(lin.columns.get_level_values("Unidad/Moneda"))
            == sorted(["UYU", "USD"]))
    remove_clutter()
