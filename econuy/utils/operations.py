import inspect
import json
import os
from pathlib import Path
from typing import  Optional, Dict

import pandas as pd

from econuy.utils import get_project_root
from econuy.base import Dataset, DatasetMetadata


def load_datasets_info() -> Dict:
    with open(get_project_root() / "retrieval" / "datasets.json", "r") as f:
        return json.load(f)


DATASETS = load_datasets_info()


def get_name_from_function() -> str:
    return inspect.currentframe().f_back.f_code.co_name


def get_download_sources(name: str) -> Dict:
    return DATASETS[name]["sources"]["downloads"]


def get_data_dir() -> Path:
    data_dir = os.getenv("ECONUY_DATA_DIR", "") or Path.home() / ".cache" / "econuy"
    data_dir = Path(data_dir)
    os.environ["ECONUY_DATA_DIR"] = data_dir.as_posix()
    data_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
    return data_dir


def read_dataset(name: str, data_dir: Path) -> Optional[Dataset]:  # noqa: F821
    dataset_path = (data_dir / name).with_suffix(".csv")
    metadata_path = (data_dir / f"{name}_metadata").with_suffix(".json")
    if not dataset_path.exists() or not metadata_path.exists():
        return None

    dataset = pd.read_csv(dataset_path, index_col=0, parse_dates=True)
    metadata = DatasetMetadata.from_json(metadata_path)
    dataset = Dataset(name, dataset, metadata)
    return dataset
