import os
from pathlib import Path

import pytest

from econuy.core import Pipeline
from econuy.utils import get_project_root, metadata, ops


def trim_rows(input: str, output: str, rows: int = 10):
    for f in os.listdir(input):
        if f.endswith(".csv") and "commodity_weights" not in f:
            df = ops._read(Path(input, f), file_fmt="csv", multiindex="included")
            df = df.reindex(df.index[:-rows])
            ops._save(df, Path(output, f))


p = Pipeline()
all_datasets = list(p.available_datasets())


@pytest.mark.parametrize(
    "dataset",
    all_datasets,
)
def test_retrieval(dataset):
    """Download every available dataset and compare with a previous version.

    Up to 5% deviations are accepted.
    """
    location = Path(get_project_root().parent, "tests/test-data")
    p = Pipeline(location=location, always_save=False)
    p.get(dataset)
    test_path = Path(location, f"{dataset}.csv")
    test = ops._read(test_path)
    metadata._set(test)
    compare = p.dataset.reindex(test.index)
    div = test / compare
    final = div[((div > 1.05) | (div < 0.95)).any(1)]
    assert len(final) == 0
