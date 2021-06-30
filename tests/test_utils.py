from os import path

import pytest
from sqlalchemy import create_engine

from econuy.utils.metadata import _get_sources
from econuy.retrieval import prices
from econuy.utils import sqlutil
try:
    from tests.test_session import remove_temporary_files_folders
except ImportError:
    from .test_session import remove_temporary_files_folders


CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(CUR_DIR, "test-data")
TEST_CON = create_engine("sqlite://").connect()
sqlutil.insert_csvs(con=TEST_CON, directory=TEST_DIR)


def test_revise():
    remove_temporary_files_folders()
    price = prices.cpi(save_loc=TEST_DIR)
    price_alt = prices.cpi(update_loc=TEST_DIR, revise_rows=6)
    assert price.round(4).equals(price_alt.round(4))
    price_alt = prices.cpi(update_loc=TEST_DIR, revise_rows="nodup")
    assert price.round(4).equals(price_alt.round(4))
    price_alt = prices.cpi(update_loc=TEST_DIR, revise_rows="auto")
    assert price.round(4).equals(price_alt.round(4))
    with pytest.raises(ValueError):
        prices.cpi(update_loc=TEST_DIR, revise_rows="wrong")
    remove_temporary_files_folders()


def test_sqlutil():
    remove_temporary_files_folders()
    sqlutil.read(con=TEST_CON, command='SELECT * FROM nxr_daily')
    sqlutil.read(con=TEST_CON, table_name="nxr_daily",
                 start_date="2011-01-14",
                 end_date="2012-01-15")
    sqlutil.read(con=TEST_CON, table_name="nxr_daily",
                 start_date="2011-01-14")
    sqlutil.read(con=TEST_CON, table_name="reserves_changes",
                 end_date="2012-01-01",
                 cols=["1. Compras netas de moneda extranjera"])


def test_sources():
    source_1 = _get_sources("balance_gps")
    source_2 = _get_sources("balance_nfps")
    assert source_1 == source_2
