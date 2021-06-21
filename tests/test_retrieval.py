from pathlib import Path

import pandas as pd

from econuy.session import Session
from econuy.utils import get_project_root, metadata, ops

def test_retrieval():
    """Download every available dataset and compare with a previous version.

    Up to 5% deviations are accepted.
    """
    location = Path(get_project_root().parent, "tests/test-data")
    s = Session(location=location)
    s.get_bulk(names="all", save=False)
    for k, v in s.datasets.items():
        print(k)
        test_path = Path(location, f"{k}.csv")
        test = ops._read(test_path)
        metadata._set(test)
        compare = v.reindex(test.index)
        div = test / compare
        final = div[((div > 1.05) | (div < 0.95)).any(1)]
        assert len(final) == 0
