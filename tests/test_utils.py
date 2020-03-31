import pytest

from econuy.retrieval import cpi
from .test_session import remove_clutter


def test_revise():
    remove_clutter()
    prices = cpi.get(save_path="test-data")
    prices_alt = cpi.get(update_path="test-data", force_update=True,
                         revise_rows=6)
    assert prices.round(4).equals(prices_alt.round(4))
    prices_alt = cpi.get(update_path="test-data", force_update=True,
                         revise_rows="nodup")
    assert prices.round(4).equals(prices_alt.round(4))
    prices_alt = cpi.get(update_path="test-data", force_update=True,
                         revise_rows="auto")
    assert prices.round(4).equals(prices_alt.round(4))
    with pytest.raises(ValueError):
        cpi.get(update_path="test-data", force_update=True,
                revise_rows="wrong")
    remove_clutter()
