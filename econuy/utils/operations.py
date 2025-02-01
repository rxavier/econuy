import inspect
import json
import os
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import pandas as pd

from econuy.utils import get_project_root
from econuy.base import Dataset, DatasetMetadata


class DatasetRegistry:
    def __init__(self):
        """
        Initialize the DatasetRegistry by loading the dataset information from a JSON file.
        """
        with open(
            get_project_root() / "retrieval" / "datasets.json", "r", encoding="utf-8"
        ) as f:
            self.registry = json.load(f)

    def __getitem__(self, name: str) -> Dict:
        """
        Retrieve a dataset by its name.

        Parameters
        ----------
        name : str
            The name of the dataset to retrieve.

        Returns
        -------
        dict
            The dataset information.
        """
        return self.registry[name]

    def get_multiple(self, names: List[str]) -> Dict:
        """
        Retrieve multiple datasets by their names.

        Parameters
        ----------
        names : List[str]
            A list of dataset names to retrieve.

        Returns
        -------
        dict
            A dictionary containing the requested datasets.
        """
        return {k: v for k, v in self.registry.items() if k in names}

    def get_available(self) -> Dict:
        """
        Retrieve all available datasets that are not disabled.

        Returns
        -------
        dict
            A dictionary containing all available datasets.
        """
        return {
            k: v
            for k, v in self.registry.items()
            if not v["disabled"] and not v["auxiliary"]
        }

    def get_custom(self) -> Dict:
        """
        Retrieve all custom datasets.

        Returns
        -------
        dict
            A dictionary containing all custom datasets.
        """
        return {k: v for k, v in self.registry.items() if v["custom"]}

    def get_by_area(
        self, area: str, keep_disabled: bool = False, keep_auxiliary: bool = False
    ) -> Dict:
        """
        Retrieve datasets by a specific area, with options to include disabled and auxiliary datasets.

        Parameters
        ----------
        area : str
            The area to filter datasets by.
        keep_disabled : bool, optional
            Whether to include disabled datasets (default is False).
        keep_auxiliary : bool, optional
            Whether to include auxiliary datasets (default is False).

        Returns
        -------
        dict
            A dictionary containing the datasets that match the specified area and options.
        """
        return {
            k: v
            for k, v in self.registry.items()
            if v["area"] == area
            and (keep_disabled or not v["disabled"])
            and (keep_auxiliary or not v["auxiliary"])
        }

    def list_available(self) -> List[str]:
        """
        List the names of all available datasets.

        Returns
        -------
        List[str]
            A list of names of all available datasets.
        """
        return list(self.get_available().keys())

    def list_custom(self) -> List[str]:
        """
        List the names of all custom datasets.

        Returns
        -------
        List[str]
            A list of names of all custom datasets.
        """
        return list(self.get_custom().keys())

    def list_by_area(
        self, area: str, keep_disabled: bool = False, keep_auxiliary: bool = False
    ) -> List[str]:
        """
        List the names of datasets by a specific area, with options to include disabled and auxiliary datasets.

        Parameters
        ----------
        area : str
            The area to filter datasets by.
        keep_disabled : bool, optional
            Whether to include disabled datasets (default is False).
        keep_auxiliary : bool, optional
            Whether to include auxiliary datasets (default is False).

        Returns
        -------
        List[str]
            A list of names of datasets that match the specified area and options.
        """
        return list(self.get_by_area(area, keep_disabled, keep_auxiliary).keys())


REGISTRY = DatasetRegistry()


def get_name_from_function() -> str:
    return inspect.currentframe().f_back.f_code.co_name


def get_download_sources(name: str) -> Dict:
    return REGISTRY[name]["sources"]["downloads"]


def get_base_metadata(name: str) -> Dict:
    return REGISTRY[name]["base_metadata"]


def get_names_and_ids(name: str, language: str = "es") -> Tuple[List[str], List[Dict]]:
    ids_names = REGISTRY[name]["indicator_ids"]
    ids_names = {k: v[language] for k, v in ids_names.items()}
    language_names = [{"es": x} for x in ids_names.values()]
    ids = [f"{name}_{i}" for i in ids_names.keys()]
    return ids, language_names


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
