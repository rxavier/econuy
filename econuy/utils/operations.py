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

    def __getitem__(self, id: str) -> Dict:
        """
        Retrieve a dataset by its id.

        Parameters
        ----------
        id : str
            The id of the dataset to retrieve.

        Returns
        -------
        dict
            The dataset information.
        """
        return self.registry[id]

    def get_multiple(self, ids: List[str]) -> Dict:
        """
        Retrieve multiple datasets by their ids.

        Parameters
        ----------
        ids : List[str]
            A list of dataset ids to retrieve.

        Returns
        -------
        dict
            A dictionary containing the requested datasets.
        """
        return {k: v for k, v in self.registry.items() if k in ids}

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
        List the ids of all available datasets.

        Returns
        -------
        List[str]
            A list of ids of all available datasets.
        """
        return list(self.get_available().keys())

    def list_custom(self) -> List[str]:
        """
        List the ids of all custom datasets.

        Returns
        -------
        List[str]
            A list of ids of all custom datasets.
        """
        return list(self.get_custom().keys())

    def list_by_area(
        self, area: str, keep_disabled: bool = False, keep_auxiliary: bool = False
    ) -> List[str]:
        """
        List the ids of datasets by a specific area, with options to include disabled and auxiliary datasets.

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
            A list of ids of datasets that match the specified area and options.
        """
        return list(self.get_by_area(area, keep_disabled, keep_auxiliary).keys())


REGISTRY = DatasetRegistry()


def get_id_from_function() -> str:
    return inspect.currentframe().f_back.f_code.co_name


def get_download_sources(id: str) -> Dict:
    return REGISTRY[id]["sources"]["downloads"]


def get_base_metadata(id: str) -> Dict:
    return REGISTRY[id]["base_metadata"]


def get_names_and_ids(id: str, language: str = "es") -> Tuple[List[str], List[Dict]]:
    ids_names = REGISTRY[id]["indicator_ids"]
    ids_names = {k: v[language] for k, v in ids_names.items()}
    language_names = [{"es": x} for x in ids_names.values()]
    ids = [f"{id}_{i}" for i in ids_names.keys()]
    return ids, language_names


def get_data_dir() -> Path:
    data_dir = os.getenv("ECONUY_DATA_DIR", "") or Path.home() / ".cache" / "econuy"
    data_dir = Path(data_dir)
    os.environ["ECONUY_DATA_DIR"] = data_dir.as_posix()
    data_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
    return data_dir


def read_dataset(id: str, data_dir: Path) -> Optional[Dataset]:  # noqa: F821
    dataset_path = (data_dir / id).with_suffix(".csv")
    metadata_path = (data_dir / f"{id}_metadata").with_suffix(".json")
    if not dataset_path.exists() or not metadata_path.exists():
        return None

    dataset = pd.read_csv(dataset_path, index_col=0, parse_dates=True)
    metadata = DatasetMetadata.from_json(metadata_path)
    dataset = Dataset(id, dataset, metadata)
    return dataset
