import inspect
import json
import os
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd

from econuy.utils import get_project_root
from econuy.base import Dataset, DatasetMetadata


class DatasetRegistry:
    def __init__(self):
        with open(get_project_root() / "retrieval" / "datasets.json", "r") as f:
            self.registry = json.load(f)

    def __getitem__(self, name: str) -> Dict:
        return self.registry[name]

    def get_multiple(self, names: List[str]) -> Dict:
        return {k: v for k, v in self.registry.items() if k in names}

    def get_available(self):
        return {k: v for k, v in self.registry.items() if not v["disabled"]}

    def get_custom(self):
        return {k: v for k, v in self.registry.items() if v["custom"]}

    def get_by_area(self, area: str, keep_disabled: bool = False):
        return {
            k: v
            for k, v in self.registry.items()
            if v["area"] == area and (keep_disabled or not v["disabled"])
        }

    def list_available(self):
        return list(self.get_available().keys())

    def list_custom(self):
        return list(self.get_custom().keys())

    def list_by_area(self, area: str, keep_disabled: bool = False):
        return list(self.get_by_area(area, keep_disabled).keys())


REGISTRY = DatasetRegistry()


def get_name_from_function() -> str:
    return inspect.currentframe().f_back.f_code.co_name


def get_download_sources(name: str) -> Dict:
    return REGISTRY[name]["sources"]["downloads"]


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
