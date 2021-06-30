import os
from pathlib import Path

import pytest

from econuy.session import Session
from econuy.utils import get_project_root, metadata, ops


def trim_rows(input: str, output: str, rows: int = 10):
    for f in os.listdir(input):
        if f.endswith(".csv") and "commodity_weights" not in f:
            df = ops._read(Path(input, f), file_fmt="csv",
                           multiindex="included")
            df = df.reindex(df.index[:-rows])
            ops._save(df, Path(output, f))


@pytest.mark.parametrize("area",
                         ["economic_activity", "prices", "fiscal_accounts",
                          "labor", "external_sector", "financial_sector",
                          "income", "international", "regional"])
def test_retrieval(area):
    """Download every available dataset and compare with a previous version.

    Up to 5% deviations are accepted.
    """
    location = Path(get_project_root().parent, "tests/test-data")
    s = Session(location=location, always_save=False)
    s.get_bulk(names=area)
    for k, v in s.datasets.items():
        print(k)
        test_path = Path(location, f"{k}.csv")
        test = ops._read(test_path)
        metadata._set(test)
        compare = v.reindex(test.index)
        div = test / compare
        final = div[((div > 1.05) | (div < 0.95)).any(1)]
        assert len(final) == 0
