import warnings
import copy
import pprint
from typing import Union, Any, List

import pandas as pd

from econuy import transform


def cast_metadata(indicator_metadata: dict, names: list, full_names: list) -> dict:
    # TODO: Improve this hack
    return {
        name: full_name | indicator_metadata
        for name, full_name in zip(names, full_names)
    }


class Metadata:
    def __init__(self, metadata: dict):
        self.metadata = copy.deepcopy(metadata)

    @property
    def indicators(self):
        return list(self.metadata.keys())

    @property
    def has_common_metadata(self):
        metadata_wo_full_names = self._drop_full_names(self.metadata)
        indicator_metadatas = list(metadata_wo_full_names.values())
        if len(indicator_metadatas) < 2:
            return True
        else:
            reference = indicator_metadatas[0]
            return all(reference == metadata for metadata in indicator_metadatas[1:])

    @property
    def common_metadata_dict(self):
        if self.has_common_metadata:
            metadata_wo_full_names = self._drop_full_names(self.metadata)
            return metadata_wo_full_names[self.indicators[0]]
        else:
            return {}

    @staticmethod
    def _drop_full_names(metadata: dict):
        return {
            indicator: {
                meta_name: meta
                for meta_name, meta in indicator_metadata.items()
                if "full_name" not in meta_name
            }
            for indicator, indicator_metadata in metadata.items()
        }

    def update_indicator_metadata(
        self, indicator: str, new_metadata: dict
    ) -> "Metadata":
        self.metadata[indicator].update(new_metadata)
        return self

    def update_dataset_metadata(self, new_metadata: dict) -> "Metadata":
        for indicator in self.indicators:
            self.metadata[indicator].update(new_metadata)
        return self

    def copy(self):
        return copy.deepcopy(self)

    def __getitem__(self, name: str):
        return self.metadata[name]

    def __setitem__(self, name: str, value: Any):
        self.metadata[name] = value

    def __repr__(self):
        return pprint.pformat(self.metadata)

    @classmethod
    def from_cast(cls, base_metadata: dict, names: list, full_names: list):
        return cls(cast_metadata(base_metadata, names, full_names))

    @classmethod
    def from_metadatas(cls, metadatas: List[dict]):
        metadatas_dict = {k: v for d in metadatas for k, v in d.metadata.items()}
        return cls(metadatas_dict)


class Dataset:
    def __init__(self, data: pd.DataFrame, metadata: Metadata, name: str):
        self.data = data
        self.metadata = metadata
        self.name = name
        self.indicators = self.metadata.indicators
        self.validate()

    def validate(self):
        assert len(self.indicators) == len(self.data.columns)
        assert all(indicator in self.data.columns for indicator in self.indicators)
        assert isinstance(self.data.index, pd.DatetimeIndex)
        assert self.data.dtypes.apply(pd.api.types.is_numeric_dtype).all()

    def infer_frequency(self):
        try:
            inferred_freq = pd.infer_freq(self.data.index)
        except ValueError:
            warnings.warn(
                "ValueError: Need at least 3 dates to infer frequency. "
                "Setting to 'None'.",
                UserWarning,
                stacklevel=2,
            )
            inferred_freq = None
        if inferred_freq is None:
            warnings.warn(
                "Metadata: frequency could not be inferred "
                "from the index. Setting to 'None'.",
                UserWarning,
                stacklevel=2,
            )
            inferred_freq = None
        return inferred_freq

    def __getitem__(self, indicator):
        metadata_dict = {indicator: self.metadata[indicator]}
        return self.__class__(
            data=self.data[[indicator]],
            metadata=Metadata(metadata_dict),
            name=self.name,
        )

    def __repr__(self):
        return "\n".join(
            [
                f"Dataset: {self.name}",
                f"Indicators: {self.indicators}",
                f"Metadata: {self.metadata}",
            ]
        )

    def resample(
        self,
        rule: Union[pd.DateOffset, pd.Timedelta, str],
        operation: str = "sum",
        interpolation: str = "linear",
        warn: bool = False,
    ) -> pd.DataFrame:
        """
        Resample to target frequencies.

        See Also
        --------
        :mod:`~econuy.core.Pipeline.resample`

        """
        if operation not in ["sum", "mean", "upsample", "last"]:
            raise ValueError("Invalid 'operation' option.")

        if self.metadata.has_common_metadata:
            transformed, new_metadata = transform.resample._resample(
                dataset=self,
                rule=rule,
                operation=operation,
                interpolation=interpolation,
                warn=warn,
            )
        else:
            transformed = []
            new_metadatas = []
            for column_name in self.data.columns:
                transformed_col, new_metadata = transform.resample._resample(
                    dataset=self[column_name],
                    rule=rule,
                    operation=operation,
                    interpolation=interpolation,
                    warn=warn,
                )
                transformed.append(transformed_col)
                new_metadatas.append(new_metadata)
            transformed = pd.concat(transformed, axis=1)
            new_metadata = Metadata.from_metadatas(new_metadatas)

        inferred_frequency = pd.infer_freq(transformed.index)
        new_metadata.update_dataset_metadata({"frequency": inferred_frequency})
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name)
        return output
