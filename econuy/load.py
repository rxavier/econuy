import importlib
import datetime as dt
import inspect
import os
from typing import Union, List, Optional, Dict, Literal
from pathlib import Path
from urllib.error import URLError
from json.decoder import JSONDecodeError
from concurrent import futures

import pandas as pd
from httpx import ReadTimeout
from opnieuw import retry
from tqdm.auto import tqdm

from econuy.utils.operations import REGISTRY, read_dataset, get_data_dir
from econuy.base import Dataset


OUTDATED_DELTA_THRESHOLD = dt.timedelta(days=1)  # TODO: Use an env var or config file


@retry(
    retry_on_exceptions=(ConnectionError, URLError, JSONDecodeError, ReadTimeout),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=30,
)
def load_dataset(
    name: str,
    data_dir: Union[str, Path, None] = None,
    skip_cache: bool = False,
    force_overwrite: bool = False,
    skip_update: bool = False,
) -> Dataset:
    """
    Load a dataset by name, optionally skipping cache and forcing overwrite.

    Parameters
    ----------
    name : str
        The name of the dataset to load.
    data_dir : Union[str, Path, None], optional
        The directory where the dataset is stored or will be stored. If None,
        the default data directory is used. Default is None.
    skip_cache : bool, optional
        If True, the cache will be skipped and a new dataset will be retrieved.
        Default is False.
    force_overwrite : bool, optional
        If True, the existing dataset will be overwritten. Default is False.
    skip_update : bool, optional
        If True, the dataset will not be updated if it already exists. Default is False.

    Returns
    -------
    Dataset
        The loaded dataset.

    Raises
    ------
    ValueError
        If the dataset name is not available in the registry.
    AssertionError
        If the existing dataset has changed and force_overwrite is False.
    """
    data_dir = data_dir or get_data_dir()
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True, mode=0o755)

    if not skip_cache:
        existing_dataset = read_dataset(name, data_dir)
        if existing_dataset is not None:
            created_at = existing_dataset.metadata.created_at
            if (
                dt.datetime.now() - created_at
            ) < OUTDATED_DELTA_THRESHOLD or skip_update:
                return existing_dataset
            else:
                print(
                    f"Dataset {name} exists in cache but is outdated "
                    f"(created at {created_at.strftime('%Y-%m-%d %H:%M:%S')}). "
                    "Retrieving new data."
                )

    try:
        dataset_metadata = REGISTRY[name]
    except KeyError:
        raise ValueError(f"Dataset {name} not available.")

    function_string = dataset_metadata["function"]
    module, function = function_string.split(".")
    path_prefix = "econuy.retrieval."
    module = importlib.import_module(path_prefix + module)
    dataset_retriever = getattr(module, function)

    signature = inspect.signature(dataset_retriever)
    parameters = signature.parameters
    if parameters:
        dataset = dataset_retriever(data_dir, skip_cache, force_overwrite, skip_update)
    else:
        dataset = dataset_retriever()

    if not force_overwrite:
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


def load_datasets_parallel(
    names: List[str],
    data_dir: Union[str, Path, None] = None,
    skip_cache: bool = False,
    force_overwrite: bool = False,
    skip_update: bool = False,
    max_workers: Optional[int] = None,
    executor_type: Literal["thread", "process"] = "thread",
) -> Dict[str, Dataset]:
    """
    Load multiple datasets in parallel using either threading or multiprocessing.

    Parameters
    ----------
    names : List[str]
        List of dataset names to load.
    data_dir : Union[str, Path, None], optional
        Directory where datasets are stored. If None, a default directory is used.
    skip_cache : bool, optional
        If True, skip loading from cache. Default is False.
    force_overwrite : bool, optional
        If True, force overwrite existing datasets. Default is False.
    skip_update : bool, optional
        If True, skip updating datasets that already exist. Default is False.
    max_workers : Optional[int], optional
        Maximum number of workers to use for parallel loading. If None, it will use the default number of workers.
    executor_type : Literal["thread", "process"], optional
        Type of executor to use for parallel loading. Can be "thread" for ThreadPoolExecutor or "process" for ProcessPoolExecutor. Default is "thread".

    Returns
    -------
    Dict[str, Dataset]
        A dictionary where keys are dataset names and values are the loaded datasets.

    Raises
    ------
    Exception
        If there is an error loading any of the datasets, it will be printed and the dataset will be skipped.
    """
    datasets = {}

    # We first pick an executor, then get the default workers used in the stdlib code.
    # If max_workers is not set, we use the default number of workers.
    # In both cases we limit the number of workers to the number of datasets.
    if executor_type == "thread":
        executor_class = futures.ThreadPoolExecutor
        default_workers = min(32, (os.cpu_count() or 1) + 4)
    elif executor_type == "process":
        executor_class = futures.ProcessPoolExecutor
        default_workers = os.cpu_count() or 1

    workers = max_workers or default_workers
    workers = min(workers, len(names))

    with executor_class(workers) as executor:
        future_to_name = {
            executor.submit(
                load_dataset, name, data_dir, skip_cache, force_overwrite, skip_update
            ): name
            for name in names
        }
        with tqdm(total=len(names), desc="Loading datasets") as pbar:
            for future in futures.as_completed(future_to_name):
                name = future_to_name[future]
                pbar.set_postfix_str(name)
                try:
                    dataset = future.result()
                    datasets[name] = dataset
                except Exception as exc:
                    print(f"Error loading dataset {name} | {exc}")
                pbar.update(1)
    return datasets


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
