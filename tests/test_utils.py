from os import path

import pytest
from sqlalchemy import create_engine

from econuy.utils.metadata import _get_sources
from econuy.retrieval import cpi
from econuy.utils import sqlutil
try:
    from tests.test_session import remove_clutter
except ImportError:
    from .test_session import remove_clutter


CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(path.dirname(CUR_DIR), "test-data")
TEST_CON = create_engine("sqlite://").connect()
sqlutil.insert_csvs(con=TEST_CON, directory=TEST_DIR)


def test_revise():
    remove_clutter()
    prices = cpi.get(save_loc=TEST_DIR)
    prices_alt = cpi.get(update_loc=TEST_DIR, revise_rows=6)
    assert prices.round(4).equals(prices_alt.round(4))
    prices_alt = cpi.get(update_loc=TEST_DIR, revise_rows="nodup")
    assert prices.round(4).equals(prices_alt.round(4))
    prices_alt = cpi.get(update_loc=TEST_DIR, revise_rows="auto")
    assert prices.round(4).equals(prices_alt.round(4))
    with pytest.raises(ValueError):
        cpi.get(update_loc=TEST_DIR, revise_rows="wrong")
    remove_clutter()


def test_sqlutil():
    remove_clutter()
    sqlutil.read(con=TEST_CON, command='SELECT * FROM nxr_daily')
    sqlutil.read(con=TEST_CON, table_name="nxr_daily",
                 start_date="2011-01-14",
                 end_date="2012-01-15")
    sqlutil.read(con=TEST_CON, table_name="nxr_daily",
                 start_date="2011-01-14")
    sqlutil.read(con=TEST_CON, table_name="reserves_chg",
                 end_date="2012-01-01",
                 cols=["1. Compras netas de moneda extranjera"])


def test_sources():
    source_1 = _get_sources("tfm_labor_test")
    source_2 = _get_sources("tfm_labor")
    assert source_1 == source_2
