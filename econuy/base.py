import copy
import warnings
import json
from pathlib import Path
from datetime import datetime
from typing import List, Union, Optional, Literal, Dict

import pandas as pd
import numpy as np

from econuy.transform.change import _chg_diff
from econuy.transform.resample import _resample
from econuy.transform.rolling import _rolling
from econuy.transform.rebase import _rebase
from econuy.transform.convert import _convert_usd, _convert_gdp, _convert_real
from econuy.transform.decompose import _decompose


class DatasetConfig:
    def __init__(self, name: str) -> None:
        self.name = name
        self.load()

    def load(self) -> None:
        from econuy.utils.operations import REGISTRY

        dataset_config = REGISTRY[self.name]
        for key, value in dataset_config.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        return json.dumps(self.__dict__, indent=4)


class DatasetMetadata:
    def __init__(
        self,
        name: str,
        indicator_metadata: dict,
        created_at: Optional[datetime] = None,
        config: Optional[DatasetConfig] = None,
    ) -> None:
        self.name = name
        self.indicator_metadata = indicator_metadata
        self.created_at = created_at or datetime.now()
        self.config = config or DatasetConfig(name)

    def __getitem__(self, indicator) -> "DatasetMetadata":
        return self.__class__(
            name=self.name,
            indicator_metadata={indicator: self.indicator_metadata[indicator]},
        )

    def __setitem__(self, indicator, metadata) -> None:
        self.indicators[indicator] = metadata

    @property
    def indicator_ids(self) -> list:
        """
        Get the list of indicators in the metadata.

        Returns
        -------
        list
            The list of indicators.
        """
        return list(self.indicator_metadata.keys())

    @property
    def has_common_metadata(self) -> bool:
        """
        Check if all indicators have the same metadata.

        Returns
        -------
        bool
            True if all indicators have the same metadata, False otherwise.
        """
        metadata_wo_full_names = self._drop_full_names(self.indicator_metadata)
        indicator_metadatas = list(metadata_wo_full_names.values())
        if len(indicator_metadatas) < 2:
            return True
        else:
            reference = indicator_metadatas[0]
            return all(reference == metadata for metadata in indicator_metadatas[1:])

    @property
    def common_metadata_dict(self) -> dict:
        """
        Get the common metadata dictionary.

        Returns
        -------
        dict
            The metadata dictionary excluding keys that are not common to all indicators.
        """
        metadata_wo_full_names = self._drop_full_names(self.indicator_metadata)
        if len(metadata_wo_full_names) == 0:
            return {}

        first_metadata = metadata_wo_full_names[self.indicator_ids[0]]
        common_metadata = {}

        for key, value in first_metadata.items():
            if all(
                metadata_wo_full_names[indicator].get(key) == value
                for indicator in self.indicator_ids
            ):
                common_metadata[key] = value

        return common_metadata

    @staticmethod
    def _drop_full_names(indicator_metadata: dict) -> dict:
        """
        Drop full names from the metadata.

        Parameters
        ----------
        metadata : dict
            The metadata to process.

        Returns
        -------
        dict
            The metadata with full names dropped.
        """
        return {
            indicator: {
                meta_name: meta
                for meta_name, meta in single_indicator_metadata.items()
                if "name" not in meta_name
            }
            for indicator, single_indicator_metadata in indicator_metadata.items()
        }

    def update_indicator_metadata(
        self, indicator: str, single_indicator_metadata: dict
    ) -> "DatasetMetadata":
        """
        Update the metadata for a specific indicator.

        Parameters
        ----------
        indicator : str
            The indicator to update.
        single_indicator_metadata : dict
            The metadata to update with.

        Returns
        -------
        Metadata
            The updated metadata.
        """
        self.indicator_metadata[indicator].update(single_indicator_metadata)
        return self

    def update_indicator_metadata_value(
        self,
        indicator: str,
        key: str,
        value: str,
    ) -> "DatasetMetadata":
        """
        Update the metadata for a specific indicator.

        Parameters
        ----------
        indicator : str
            The indicator to update.
        key : str
            The key to update.
        value : str
            The value to update with.

        Returns
        -------
        Metadata
            The updated metadata.
        """
        self.indicator_metadata[indicator][key] = value
        return self

    def update_dataset_metadata(self, indicator_metadata: dict) -> "DatasetMetadata":
        """
        Update the metadata for all indicators.

        Parameters
        ----------
        indicator_metadata : dict
            The new metadata to update with.

        Returns
        -------
        Metadata
            The updated metadata.
        """
        for indicator in self.indicator_metadata:
            self.indicator_metadata[indicator].update(indicator_metadata)
        return self

    def add_transformation_step(self, transformation: dict) -> "DatasetMetadata":
        """
        Add a transformation step to the metadata.

        Parameters
        ----------
        transformation : dict
            The transformation step to add.

        Returns
        -------
        Metadata
            The updated metadata.
        """
        for indicator in self.indicator_metadata:
            self.indicator_metadata[indicator]["transformations"].append(transformation)
        return self

    def copy(self) -> "DatasetMetadata":
        """
        Create a copy of the metadata.

        Returns
        -------
        Metadata
            The copied metadata.
        """
        return copy.deepcopy(self)

    def to_dict(self) -> Dict:
        d = self.__dict__.copy()
        d["created_at"] = d["created_at"].isoformat()
        d["config"] = self.config.__dict__
        return d

    def save(self, name: str, data_dir: Union[str, Path, None] = None) -> None:
        from econuy.utils.operations import get_data_dir

        data_dir = data_dir or get_data_dir()
        data_dir = Path(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        for_json = self.to_dict()
        with open(data_dir / f"{name}_metadata.json", "w") as f:
            json.dump(for_json, f, indent=4)
        return

    @staticmethod
    def cast_metadata(
        indicator_metadata: dict, indicator_ids: list, full_names: list
    ) -> dict:
        # TODO: Improve this hack
        return {
            name: {"names": full_name} | indicator_metadata
            for name, full_name in zip(indicator_ids, full_names)
        }

    @classmethod
    def from_cast(
        cls, name: str, base_metadata: dict, indicator_ids: list, indicator_names: list
    ) -> "DatasetMetadata":
        """
        Create a metadata instance from a casted metadata.

        Parameters
        ----------
        base_metadata : dict
            The base metadata.
        names : list
            The names of the indicators.
        full_names : list
            The full names of the indicators.

        Returns
        -------
        Metadata
            The created metadata instance.
        """
        indicator_metadata = cls.cast_metadata(
            base_metadata, indicator_ids, indicator_names
        )
        return cls(name, indicator_metadata)

    @classmethod
    def from_metadatas(
        cls, name: str, metadatas: List["DatasetMetadata"]
    ) -> "DatasetMetadata":
        """
        Create a metadata instance from a list of metadatas.

        Parameters
        ----------
        metadatas : list
            The list of metadatas.

        Returns
        -------
        Metadata
            The created metadata instance.
        """
        metadatas_dict = {
            k: v for d in metadatas for k, v in d.indicator_metadata.items()
        }
        return cls(name, metadatas_dict)

    @classmethod
    def from_json(cls, path: Union[str, Path]) -> "DatasetMetadata":
        """
        Create a metadata instance from a JSON file.

        Parameters
        ----------
        path : str or Path
            The path to the JSON file.

        Returns
        -------
        Metadata
            The created metadata instance.
        """
        with open(path, "r") as f:
            metadata_dict = json.load(f)
        metadata_dict["created_at"] = datetime.fromisoformat(
            metadata_dict["created_at"]
        )
        metadata_dict["config"] = DatasetConfig(metadata_dict["name"])
        return cls(**metadata_dict)

    def __repr__(self) -> str:
        return "\n".join(
            [
                f"Name: {self.name}",
                f"Created at: {self.created_at}",
                f"Indicator metadata: {self.indicator_metadata}",
            ]
        )


class Dataset:
    """
    A class to represent a collection of economic data.

    Parameters
    ----------
    data : pd.DataFrame
        The economic data.
    metadata : Metadata
        The metadata of the data.
    name : str
        The name of the dataset.

    Returns
    -------
    None

    See Also
    --------
    :class:`pd.DataFrame`
    :class:`Metadata`
    """

    def __init__(
        self,
        name: str,
        data: pd.DataFrame,
        metadata: DatasetMetadata,
        transformed: bool = False,
    ) -> None:
        """
        Initialize the dataset.

        Parameters
        ----------
        name : str
            The name of the dataset.
        data : pd.DataFrame
            The economic data.
        metadata : Metadata
            The metadata of the data.
        transformed : bool
            Whether the data has been transformed.

        Returns
        -------
        None
        """
        self.data = data
        self.metadata = metadata
        self.name = name
        self.transformed = transformed
        self.indicators = self.metadata.indicator_ids

    def validate(self) -> None:
        """
        Validate the dataset.

        Raises
        ------
        AssertionError
            If the number of indicators does not match the number of columns in the data.
            If any of the indicators are not in the data.
            If the index of the data is not a DatetimeIndex.
            If the data contains non-numeric values.

        """
        assert len(self.indicators) == len(self.data.columns)
        assert all(indicator in self.data.columns for indicator in self.indicators)
        assert isinstance(self.data.index, pd.DatetimeIndex)
        assert self.data.dtypes.apply(pd.api.types.is_numeric_dtype).all()

    def to_detailed(self, language: str = "es") -> pd.DataFrame:
        """
        Rename the data using the metadata.

        Parameters
        ----------
        language : str, default "es"
            The language to use for the metadata.

        Returns
        -------
        pd.DataFrame
            The data with the indicators renamed.

        """
        column_metadatas = {
            indicator: {
                "name": self.metadata.indicator_metadata[indicator]["names"][language]
            }
            for indicator in self.indicators
        }
        for ind, col_meta in column_metadatas.items():
            col_meta.update(self.metadata.indicator_metadata[ind])
            col_meta.update({"id": ind})
            col_meta.pop("names")
            col_meta.pop(
                "transformations", None
            )  # TODO: Add transformations to metadata
        col_names = [
            x.replace("_", " ").capitalize()
            for x in column_metadatas[self.indicators[0]].keys()
        ]
        columns = pd.MultiIndex.from_tuples(
            [tuple(column_metadatas[k].values()) for k in column_metadatas.keys()],
            names=col_names,
        )
        detailed_data = self.data.copy()
        detailed_data.columns = columns
        return detailed_data

    def to_named(self, language: str = "es") -> pd.DataFrame:
        """
        Rename the data using the metadata.

        Parameters
        ----------
        language : str, default "es"
            The language to use for the metadata.

        Returns
        -------
        pd.DataFrame
            The data with the indicators renamed.

        """
        column_metadatas = {
            indicator: self.metadata.indicator_metadata[indicator]["names"][language]
            for indicator in self.indicators
        }
        named_data = self.data.copy()
        named_data.columns = [column_metadatas[ind] for ind in self.indicators]
        return named_data

    def to_json(self) -> dict:
        """
        Convert the dataset to a valid JSON dictionary.

        Returns
        -------
        dict
            A JSON representation of the dataset.

        """
        data = self.data.copy()
        data.index = data.index.astype(str)
        data = data.replace([np.inf, -np.inf], np.nan)
        data = data.astype(object).where(pd.notnull(data), None)

        metadata = self.metadata.to_dict()
        metadata.pop("config")
        return {
            "name": self.name,
            "data": data.to_dict(),
            "metadata": metadata,
            "transformed": self.transformed,
        }

    def save(
        self, data_dir: Union[str, Path, None] = None, name: Optional[str] = None
    ) -> None:
        """
        Save the dataset to a directory.

        Parameters
        ----------
        data_dir : str or Path
            The directory to save the dataset to.
        name : str, default None
            The name to save the dataset as without suffixes.

        Returns
        -------
        None

        """
        from econuy.utils.operations import get_data_dir

        data_dir = data_dir or get_data_dir()
        data_dir = Path(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        name = name or (f"{self.name}_transformed" if self.transformed else self.name)
        self.data.to_csv(data_dir / f"{name}.csv")
        self.metadata.save(name, data_dir)
        return

    def infer_frequency(self) -> Optional[pd.Timedelta]:
        """
        Infer the frequency of the data.

        Returns
        -------
        Optional[pd.Timedelta]
            The inferred frequency of the data.

        """
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

    def call_pandas_method(self, method: str, *args, **kwargs) -> "Dataset":
        output = self.__class__(
            data=self.data.__getattribute__(method)(*args, **kwargs),
            metadata=self.metadata,
            name=self.name,
            transformed=self.transformed,
        )
        return output

    def __getitem__(self, indicators: Union[str, List[str]]) -> "Dataset":
        indicators = [indicators] if isinstance(indicators, str) else indicators
        metadata_dict = {i: self.metadata.indicator_metadata[i] for i in indicators}
        return self.__class__(
            data=self.data[indicators],
            metadata=DatasetMetadata(self.name, metadata_dict),
            name=self.name,
            transformed=self.transformed,
        )

    def select(
        self,
        ids: Union[str, List[str], None] = None,
        names: Union[str, List[str], None] = None,
        language: str = "es",
    ) -> "Dataset":
        assert (
            ids is not None or names is not None
        ), "Either 'ids' or 'names' must be provided."
        assert (
            ids is None or names is None
        ), "Only one of 'ids' or 'names' can be provided."

        if ids is None:
            names = [names] if isinstance(names, str) else names
            ids = [
                k
                for k, v in self.metadata.indicator_metadata.items()
                if v["names"][language] in names
            ]
        return self.__getitem__(ids)

    def filter(
        self,
        start_date: Union[str, datetime, None] = None,
        end_date: Union[str, datetime, None] = None,
    ) -> "Dataset":
        return self.__class__(
            data=self.data.loc[start_date:end_date],
            metadata=self.metadata,
            name=self.name,
            transformed=self.transformed,
        )

    def __repr__(self) -> str:
        return "\n".join(
            [
                f"Name: {self.name}",
                f"Indicators: {self.indicators}",
            ]
        )

    def resample(
        self,
        rule: Union[pd.DateOffset, pd.Timedelta, str],
        operation: Literal["sum", "mean", "last", "upsample"] = "sum",
        interpolation: str = "linear",
    ) -> "Dataset":
        """
        Wrapper for the `resample method <https://pandas.pydata.org/pandas-docs
        stable/reference/api/pandas.DataFrame.resample.html>`_ in Pandas that
        integrates with econuy dataframes' metadata.

        Trim partial bins, i.e. do not calculate the resampled
        period if it is not complete, unless the input dataframe has no defined
        frequency, in which case no trimming is done.

        Parameters
        ----------
        rule : pd.DateOffset, pd.Timedelta or str
            Target frequency to resample to. See
            `Pandas offset aliases <https://pandas.pydata.org/pandas-docs/stable/
            user_guide/timeseries.html#offset-aliases>`_
        operation : {'sum', 'mean', 'last', 'upsample'}
            Operation to use for resampling.
        interpolation : str, default 'linear'
            Method to use when missing data are produced as a result of
            resampling, for example when upsampling to a higher frequency. See
            `Pandas interpolation methods <https://pandas.pydata.org/pandas-docs
            /stable/reference/api/pandas.Series.interpolate.html>`_

        Returns
        -------
        ``Dataset``

        Raises
        ------
        ValueError
            If ``operation`` is not one of available options.
        ValueError
            If the input dataframe's columns do not have the appropiate levels.

        Warns
        -----
        UserWarning
            If input frequencies cannot be assigned a numeric value, preventing
            incomplete bin trimming.

        """
        if operation not in ["sum", "mean", "upsample", "last"]:
            raise ValueError("Invalid 'operation' option.")

        if self.metadata.has_common_metadata:
            transformed, new_metadata = _resample(
                data=self.data,
                metadata=self.metadata,
                rule=rule,
                operation=operation,
                interpolation=interpolation,
            )
        else:
            transformed = []
            new_metadatas = []
            for column_name in self.data.columns:
                n_dataset = self[column_name]
                transformed_col, new_metadata = _resample(
                    data=n_dataset.data,
                    metadata=n_dataset.metadata,
                    rule=rule,
                    operation=operation,
                    interpolation=interpolation,
                )
                transformed.append(transformed_col)
                new_metadatas.append(new_metadata)
            transformed = pd.concat(transformed, axis=1)
            new_metadata = DatasetMetadata.from_metadatas(self.name, new_metadatas)

        inferred_frequency = pd.infer_freq(transformed.index)
        new_metadata.update_dataset_metadata({"frequency": inferred_frequency})
        output = self.__class__(
            data=transformed, metadata=new_metadata, name=self.name, transformed=True
        )
        return output

    def rolling(
        self, window: int, operation: Literal["sum", "mean"] = "sum"
    ) -> "Dataset":
        """
        Wrapper for the `rolling method <https://pandas.pydata.org/pandas-docs/
        stable/reference/api/pandas.DataFrame.rolling.html>`_ in Pandas that
        integrates with econuy dataframes' metadata.

        If ``periods`` is ``None``, try to infer the frequency and set ``periods``
        according to the following logic: ``{'YE-DEC': 1, 'QE-DEC': 4, 'ME': 12}``, that
        is, each period will be calculated as the sum or mean of the last year.

        Parameters
        ----------
        window : int, default None
            How many periods the window should cover.
        operation : {'sum', 'mean'}
            Operation used to calculate rolling windows.

        Returns
        -------
        ``Dataset``

        Raises
        ------
        ValueError
            If ``operation`` is not one of available options.
        ValueError
            If the input dataframe's columns do not have the appropiate levels.

        Warns
        -----
        UserWarning
            If the input dataframe is a stock time series, for which rolling
            operations are not recommended.

        """
        if operation not in ["sum", "mean"]:
            raise ValueError("Invalid 'operation' option.")

        if self.metadata.has_common_metadata:
            transformed, new_metadata = _rolling(
                data=self.data,
                metadata=self.metadata,
                window=window,
                operation=operation,
            )

        else:
            transformed = []
            new_metadatas = []
            for column_name in self.data.columns:
                n_dataset = self[column_name]
                transformed_col, new_metadata = _rolling(
                    data=n_dataset.data,
                    metadata=n_dataset.metadata,
                    window=window,
                    operation=operation,
                )
                transformed.append(transformed_col)
                new_metadatas.append(new_metadata)
            transformed = pd.concat(transformed, axis=1)
            new_metadata = DatasetMetadata.from_metadatas(self.name, new_metadatas)
        output = self.__class__(
            data=transformed,
            metadata=new_metadata,
            name=self.name,
            transformed=True,
        )
        return output

    def chg_diff(
        self,
        operation: Literal["chg", "diff"] = "chg",
        period: Literal["last", "inter", "annual"] = "last",
    ) -> "Dataset":
        """Wrapper for the `pct_change <https://pandas.pydata.org/pandas-docs/stable/
        reference/api/pandas.DataFrame.pct_change.html>`_ and `diff <https://pandas
        .pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.diff.html>`_
        Pandas methods.

        Calculate percentage change or difference for dataframes. The ``period``
        argument takes into account the frequency of the dataframe, i.e.,
        ``inter`` (for interannual) will calculate pct change/differences with
        ``periods=4`` for quarterly frequency, but ``periods=12`` for monthly
        frequency.

        Parameters
        ----------
        operation : {'chg', 'diff'}
            ``chg`` for percent change or ``diff`` for differences.
        period : {'last', 'inter', 'annual'}
            Period with which to calculate change or difference. ``last`` for
            previous period (last month for monthly data), ``inter`` for same
            period last year, ``annual`` for same period last year but taking
            annual sums.

        Returns
        -------
        ``Dataset``

        Raises
        ------
        ValueError
            If the dataframe is not of frequency ``ME`` (month), ``QE`` or
            ``QE-DEC`` (quarter), or ``YE`` or ``YE-DEC`` (year).
        ValueError
            If the ``operation`` parameter does not have a valid argument.
        ValueError
            If the ``period`` parameter does not have a valid argument.
        ValueError
            If the input dataframe's columns do not have the appropiate levels.

        """
        if operation not in ["chg", "diff"]:
            raise ValueError("Invalid 'operation' option.")
        if period not in ["last", "inter", "annual"]:
            raise ValueError("Invalid 'period' option.")

        if self.metadata.has_common_metadata:
            transformed, new_metadata = _chg_diff(
                data=self.data,
                metadata=self.metadata,
                operation=operation,
                period=period,
            )

        else:
            transformed = []
            new_metadatas = []
            for column_name in self.data.columns:
                n_dataset = self[column_name]
                transformed_col, new_metadata = _chg_diff(
                    data=n_dataset.data,
                    metadata=n_dataset.metadata,
                    operation=operation,
                    period=period,
                )
                transformed.append(transformed_col)
                new_metadatas.append(new_metadata)
            transformed = pd.concat(transformed, axis=1)
            new_metadata = DatasetMetadata.from_metadatas(self.name, new_metadatas)
        output = self.__class__(
            data=transformed,
            metadata=new_metadata,
            name=self.name,
            transformed=True,
        )
        return output

    def rebase(
        self,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime, None] = None,
        base: float = 100.0,
    ) -> "Dataset":
        """Rebase dataset to a date or range of dates.

        Parameters
        ----------
        start_date : string or datetime.datetime
            Date to which series will be rebased.
        end_date : string or datetime.datetime, default None
            If specified, series will be rebased to the average between
            ``start_date`` and ``end_date``.
        base : float, default 100
            Float for which ``start_date`` == ``base`` or average between
            ``start_date`` and ``end_date`` == ``base``.

        Returns
        -------
        ``Dataset``

        """

        if self.metadata.has_common_metadata:
            transformed, new_metadata = _rebase(
                data=self.data,
                metadata=self.metadata,
                start_date=start_date,
                end_date=end_date,
                base=base,
            )

        else:
            transformed = []
            new_metadatas = []
            for column_name in self.data.columns:
                n_dataset = self[column_name]
                transformed_col, new_metadata = _rebase(
                    data=n_dataset.data,
                    metadata=n_dataset.metadata,
                    start_date=start_date,
                    end_date=end_date,
                    base=base,
                )
                transformed.append(transformed_col)
                new_metadatas.append(new_metadata)
            transformed = pd.concat(transformed, axis=1)
            new_metadata = DatasetMetadata.from_metadatas(self.name, new_metadatas)
        output = self.__class__(
            data=transformed,
            metadata=new_metadata,
            name=self.name,
            transformed=True,
        )
        return output

    def convert(
        self,
        flavor: Literal["usd", "real", "gdp"],
        start_date: Union[str, datetime, None] = None,
        end_date: Union[str, datetime, None] = None,
        error_handling: Literal["raise", "coerce", "ignore"] = "raise",
    ) -> "Dataset":
        """Convert dataset from UYU to USD, from UYU to real UYU or
        from UYU/USD to % GDP.

        ``flavor=usd``: Convert a dataset from Uruguayan pesos to US dollars. Takes into
        account whether the input datasets is flow or stock, in order to
        choose end of period or monthly average NXR. Also take into account the
        input dataframe's frequency and whether columns represent rolling averages
        or sums.

        ``flavor=real``: Convert a dataset columns to real prices. Takes into account the
        input datasets's frequency and whether
        columns represent rolling averages or sums. Allow choosing a single period,
        a range of dates or no period as a base (i.e., period for which the
        average/sum of input dataframe and output dataframe is the same).

        ``flavor=gdp``: Convert a dataset to percentage of GDP. Takes into account the
        input dataset's currency for chossing UYU or USD GDP. If frequency of input dataset is
        higher than quarterly, GDP will be upsampled and linear interpolation will
        be performed to complete missing data.
        If input dataframe's "cumulative_periods" level is not 12 for monthly frequency or 4
        for quarterly frequency, calculate rolling input dataframe.

        In all cases, if input dataframe's frequency is higher than monthly
        (daily, business, etc.), resample to monthly frequency.

        Parameters
        ----------
        flavor : str
            ``usd`` for USD, ``real`` for real UYU, ``gdp`` for % GDP.
        start_date : str, datetime.date or None, default None
            Only used if ``flavor=real``. If set to a date-like string or a
            date, and ``end_date`` is None, the base period will be
            ``start_date``.
        end_date : str, datetime.date or None, default None
            Only used if ``flavor=real``. If ``start_date`` is set, calculate
            so that the data is in constant prices of ``start_date-end_date``.
        error_handling : {"raise", "coerce", "ignore"}, default "raise"
            What to do when the input dataset can't be converted. Coercion will set to np.nan,
            while "ignore" is a no-op. If "raise", will raise an error.

        Returns
        -------
        ``Dataset``
        """
        assert flavor in ["usd", "real", "gdp"], "Invalid 'flavor' option."
        funcs = {"usd": _convert_usd, "real": _convert_real, "gdp": _convert_gdp}
        func = funcs[flavor]
        kwargs = (
            {"start_date": start_date, "end_date": end_date} if flavor == "real" else {}
        )

        if self.metadata.has_common_metadata:
            transformed, new_metadata = func(
                data=self.data,
                metadata=self.metadata,
                error_handling=error_handling,
                **kwargs,
            )

        else:
            transformed = []
            new_metadatas = []
            for column_name in self.data.columns:
                n_dataset = self[column_name]
                transformed_col, new_metadata = func(
                    data=n_dataset.data,
                    metadata=n_dataset.metadata,
                    error_handling=error_handling,
                    **kwargs,
                )
                transformed.append(transformed_col)
                new_metadatas.append(new_metadata)
            transformed = pd.concat(transformed, axis=1)
            new_metadata = DatasetMetadata.from_metadatas(self.name, new_metadatas)
        output = self.__class__(
            data=transformed,
            metadata=new_metadata,
            name=self.name,
            transformed=True,
        )
        return output

    def decompose(
        self,
        method: Literal["x13", "loess", "mloess", "moving_averages"] = "x13",
        fallback: Literal["loess", "mloess", "moving_averages"] = "loess",
        component: Literal["t-c", "sa"] = "sa",
        fn_kwargs: Optional[dict] = None,
        ignore_warnings: bool = True,
        error_handling: Literal["raise", "coerce", "ignore"] = "raise",
    ) -> "Dataset":
        """Rebase dataset to a date or range of dates.

        Parameters
        ----------
        method : {'x13', 'loess', 'mloess', 'moving_averages'}, default 'x13'
            Method to use for decomposition.
        fallback : {'loess', 'mloess', 'moving_averages'}, default 'loess'
            Fallback method to use if the main method fails.
        component : {'t-c', 'sa'}, default 'sa'
            Component to return. 't-c' for trend-cycle, 'sa' for seasonally adjusted.
        fn_kwargs : dict, default None
            Additional keyword arguments to pass to the decomposition function.
        ignore_warnings : bool, default True
            Whether to ignore warnings.
        error_handling : {"raise", "coerce", "ignore"}, default "raise"
            What to do when the input dataset can't be converted. Coercion will set to np.nan,

        Returns
        -------
        ``Dataset``

        """
        assert method in [
            "x13",
            "loess",
            "mloess",
            "moving_averages",
        ], "Invalid 'method' option."
        assert component in ["t-c", "sa"], "Invalid 'component' option."

        fn_kwargs = fn_kwargs or {}

        if self.metadata.has_common_metadata:
            transformed, new_metadata = _decompose(
                data=self.data,
                metadata=self.metadata,
                method=method,
                fallback=fallback,
                component=component,
                fn_kwargs=fn_kwargs,
                ignore_warnings=ignore_warnings,
                error_handling=error_handling,
            )

        else:
            transformed = []
            new_metadatas = []
            for column_name in self.data.columns:
                n_dataset = self[column_name]
                transformed_col, new_metadata = _decompose(
                    data=n_dataset.data,
                    metadata=n_dataset.metadata,
                    method=method,
                    fallback=fallback,
                    component=component,
                    fn_kwargs=fn_kwargs,
                    ignore_warnings=ignore_warnings,
                    error_handling=error_handling,
                )
                transformed.append(transformed_col)
                new_metadatas.append(new_metadata)
            transformed = pd.concat(transformed, axis=1)
            new_metadata = DatasetMetadata.from_metadatas(self.name, new_metadatas)
        output = self.__class__(
            data=transformed,
            metadata=new_metadata,
            name=self.name,
            transformed=True,
        )
        return output
