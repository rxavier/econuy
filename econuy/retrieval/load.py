import importlib
import datetime as dt
from typing import Union
from pathlib import Path

import pandas as pd

from econuy.utils.operations import DATASETS, read_dataset, get_data_dir

from econuy.base import Dataset


OUTDATED_DELTA_THRESHOLD = dt.timedelta(days=1) # TODO: Use an env var or config file


def load_dataset(
    name: str,
    data_dir: Union[str, Path, None] = None,
    skip_cache: bool = False,
    safe_overwrite: bool = True,
) -> Dataset:
    data_dir = data_dir or get_data_dir()
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True, mode=0o755)

    if not skip_cache:
        existing_dataset = read_dataset(name, data_dir)
        if existing_dataset is not None:
            created_at = existing_dataset.metadata.created_at
            if (dt.datetime.now() - created_at) < OUTDATED_DELTA_THRESHOLD:
                return existing_dataset
            else:
                print(f"Dataset {name} exists in cache but is outdated (created at {created_at.strftime('%Y-%m-%d %H:%M:%S')}). Retrieving new data.")

    dataset_metadata = DATASETS[name]
    function_string = dataset_metadata["function"]
    module, function = function_string.split(".")
    path_prefix = "econuy.retrieval."
    module = importlib.import_module(path_prefix + module)
    dataset_retriever = getattr(module, function)
    dataset = dataset_retriever()

    if safe_overwrite:
        existing_dataset = read_dataset(name, data_dir)
        if existing_dataset is not None:
            try:
                check_updated_dataset(existing_dataset, dataset)
                dataset.save(data_dir)
            except AssertionError as exc:
                print(f"Dataset {name} has changed. Will not overwrite. Error: {exc}")
        else:
            dataset.save(data_dir)
    else:
        dataset.save(data_dir)

    return dataset


def check_updated_dataset(original: Dataset, new: Dataset) -> None:  # noqa: F821
    assert original.metadata.name == new.metadata.name, "Datasets have different names"
    assert (
        original.metadata.indicator_metadata == new.metadata.indicator_metadata
    ), "Datasets have different indicator metadata"
    assert (
        original.data.shape[1] == new.data.shape[1]
    ), "Datasets have different number of columns"
    assert (
        original.data.index[0] == new.data.index[0]
    ), "Datasets have different start date"

    shortened_n = int(original.data.shape[0] * 0.9)
    shortened_original = original.data.head(shortened_n)
    shortened_new = new.data.head(shortened_n)
    assert (
        shortened_original.notna().sum().sum() == shortened_new.notna().sum().sum()
    ), "Datasets have different number of missing values"
    try:
        pd.testing.assert_series_equal(
            shortened_original.mean(), shortened_new.mean(), atol=0, rtol=0.05
        )
    except AssertionError as exc:
        raise AssertionError("Datasets have different means") from exc
    try:
        pd.testing.assert_series_equal(
            shortened_original.std(), shortened_new.std(), atol=0, rtol=0.05
        )
    except AssertionError as exc:
        raise AssertionError("Datasets have different standard deviations") from exc
