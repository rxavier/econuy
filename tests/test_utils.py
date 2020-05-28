from os import path

import pytest
from sqlalchemy import create_engine

from econuy.retrieval import cpi
try:
    from tests.test_session import remove_clutter
except ImportError:
    from .test_session import remove_clutter


CUR_DIR = path.abspath(path.dirname(__file__))
TEST_DIR = path.join(path.dirname(CUR_DIR), "test-data")
TEST_CON = create_engine("sqlite://").connect()


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
