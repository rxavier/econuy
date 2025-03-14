import importlib
import datetime as dt
import inspect
import os
from typing import Union, List, Optional, Dict, Literal, Tuple
from pathlib import Path
from urllib.error import URLError
from json.decoder import JSONDecodeError
from concurrent import futures

from httpx import ReadTimeout
from opnieuw import retry
from tqdm.auto import tqdm

from econuy.utils.operations import REGISTRY, read_dataset, get_data_dir
from econuy.base import Dataset
from econuy.utils.logging import logger


OUTDATED_DELTA_THRESHOLD = dt.timedelta(days=1)  # TODO: Use an env var or config file


@retry(
    retry_on_exceptions=(ConnectionError, URLError, JSONDecodeError, ReadTimeout),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=30,
)
def load_dataset(
    id: str,
    data_dir: Union[str, Path, None] = None,
    skip_cache: bool = False,
    force_overwrite: bool = False,
    skip_update: bool = False,
) -> Dataset:
    """
    Load a dataset by id, optionally skipping cache and forcing overwrite.

    Parameters
    ----------
    id : str
        The id of the dataset to load.
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
        If the dataset id is not available in the registry.
    AssertionError
        If the existing dataset has changed and force_overwrite is False.
    """
    data_dir = data_dir or get_data_dir()
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
    now = dt.datetime.now(dt.timezone.utc)

    existing_dataset = read_dataset(id, data_dir)

    if not skip_cache:
        if existing_dataset is not None:
            checked_at = existing_dataset.metadata.checked_at
            if (now - checked_at) < OUTDATED_DELTA_THRESHOLD or skip_update:
                logger.info(
                    f"Using cached dataset {id} "
                    f"(last checked: {existing_dataset.metadata.checked_at.strftime('%Y-%m-%d %H:%M:%S')})"
                )
                return existing_dataset
            else:
                logger.info(
                    f"Dataset {id} exists in cache but may be outdated "
                    f"(last checked: {checked_at.strftime('%Y-%m-%d %H:%M:%S')}). "
                    "Retrieving new data."
                )

    try:
        dataset_metadata = REGISTRY[id]
    except KeyError:
        raise ValueError(f"Dataset {id} not available.")

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
        if existing_dataset is not None:
            compatible, updated_timestamps, new_timestamps = compare_datasets(existing_dataset, dataset)
            if compatible:
                dataset.metadata.created_at = existing_dataset.metadata.created_at
                dataset.metadata.updated_at = existing_dataset.metadata.updated_at
                dataset.metadata.last_update = existing_dataset.metadata.last_update
                dataset.metadata.checked_at = now
                if updated_timestamps or new_timestamps:
                    logger.info(
                        f"Dataset {id} has changes: "
                        f"{len(updated_timestamps)} updated timestamps, "
                        f"{len(new_timestamps)} new timestamps"
                    )
                    dataset.metadata.last_update = {
                        "updated": updated_timestamps,
                        "new": new_timestamps
                    }
                    dataset.metadata.updated_at = now
                dataset.save(data_dir)
            else:
                logger.warning(f"Dataset {id} has incompatible changes, will not overwrite")
                return existing_dataset
        else:
            dataset.metadata.checked_at = dataset.metadata.created_at
            dataset.metadata.updated_at = None
            dataset.metadata.last_update = {"updated": [], "new": []}
            dataset.save(data_dir)
    else:
        dataset.metadata.checked_at = dataset.metadata.created_at
        dataset.metadata.updated_at = None
        dataset.metadata.last_update = {"updated": [], "new": []}
        dataset.save(data_dir)

    return dataset


def load_datasets_parallel(
    ids: List[str],
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
    ids : List[str]
        List of dataset ids to load.
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
        A dictionary where keys are dataset ids and values are the loaded datasets.

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
    workers = min(workers, len(ids))

    with executor_class(workers) as executor:
        future_to_id = {
            executor.submit(
                load_dataset, id, data_dir, skip_cache, force_overwrite, skip_update
            ): id
            for id in ids
        }
        with tqdm(total=len(ids), desc="Loading datasets") as pbar:
            for future in futures.as_completed(future_to_id):
                id = future_to_id[future]
                pbar.set_postfix_str(id)
                try:
                    dataset = future.result()
                    datasets[id] = dataset
                except Exception as exc:
                    logger.error(f"Error loading dataset {id} | {exc}")
                pbar.update(1)
    return datasets


def compare_datasets(
    original: Dataset,
    new: Dataset,
    value_change_threshold: float = 0.05,
    max_changes_pct: float = 0.1,
) -> Tuple[bool, List[dt.datetime], List[dt.datetime]]:
    """Compare two datasets and identify changes.

    Parameters
    ----------
    original : Dataset
        The original dataset to compare against
    new : Dataset
        The new dataset to compare
    value_change_threshold : float, default 0.05
        The relative threshold for considering a value as changed for compatibility checks.
    max_changes_pct : float, default 0.1
        The maximum percentage of changes allowed for a column to be considered compatible.

    Returns
    -------
    Tuple[bool, List[datetime], List[datetime]]
        A tuple containing:
        - bool: Whether the datasets are compatible (same structure, etc)
        - List[datetime]: Timestamps where values changed (beyond floating point differences)
        - List[datetime]: Timestamps that are new in the new dataset
    """
    if original.metadata.id != new.metadata.id:
        logger.error(f"Datasets have different ids: {original.metadata.id} vs {new.metadata.id}")
        return False, [], []
    if original.metadata.indicator_metadata != new.metadata.indicator_metadata:
        logger.error("Datasets have different indicator metadata")
        return False, [], []
    if original.data.shape[1] != new.data.shape[1]:
        logger.error(f"Datasets have different number of columns: {original.data.shape[1]} vs {new.data.shape[1]}")
        return False, [], []
    if original.data.index[0] != new.data.index[0]:
        logger.error(f"Datasets have different start dates: {original.data.index[0]} vs {new.data.index[0]}")
        return False, [], []

    new_timestamps = new.data.index.difference(original.data.index).to_list()

    common_timestamps = original.data.index.intersection(new.data.index)
    original_subset = original.data.loc[common_timestamps]
    new_subset = new.data.loc[common_timestamps]

    abs_mean = (original_subset.abs() + new_subset.abs()) / 2
    abs_mean = abs_mean.replace(0, 1e-10)
    relative_changes = (new_subset - original_subset).abs() / abs_mean
    pct_significant_changes = (relative_changes > value_change_threshold).mean()

    if (pct_significant_changes > max_changes_pct).any():
        problematic_cols = pct_significant_changes[pct_significant_changes > max_changes_pct]
        logger.error(
            "Datasets have incompatible changes. Columns with too many significant changes: "
            f"{', '.join(f'{col}: {pct:.1%}' for col, pct in problematic_cols.items())}"
        )
        return False, [], []

    differences = (new_subset - original_subset).abs()
    updated_timestamps = common_timestamps[differences.gt(1e-10).any(axis=1)].to_list()

    return True, updated_timestamps, new_timestamps
