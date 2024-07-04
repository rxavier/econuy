import copy
import warnings
import json
from pathlib import Path
from datetime import datetime
from typing import List, Union, Optional

import pandas as pd

from econuy.transform.change import _chg_diff
from econuy.transform.resample import _resample
from econuy.transform.rolling import _rolling
from econuy.transform.rebase import _rebase
from econuy.utils.operations import get_data_dir
#from econuy.transform.convert import _convert_gdp, _convert_real, _convert_usd


def cast_metadata(indicator_metadata: dict, names: list, full_names: list) -> dict:
    # TODO: Improve this hack
    return {
        name: full_name | indicator_metadata
        for name, full_name in zip(names, full_names)
    }


class Metadata(dict):
    """
    A class to represent a collection of metadata for a set of indicators.

    Parameters
    ----------
    None

    Returns
    -------
    None

    See Also
    --------
    :class:`dict`
    """

    @property
    def indicators(self) -> list:
        """
        Get the list of indicators in the metadata.

        Returns
        -------
        list
            The list of indicators.
        """
        return list(self.keys())

    @property
    def has_common_metadata(self) -> bool:
        """
        Check if all indicators have the same metadata.

        Returns
        -------
        bool
            True if all indicators have the same metadata, False otherwise.
        """
        metadata_wo_full_names = self._drop_full_names(self)
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
            The common metadata dictionary if all indicators have the same metadata, otherwise an empty dictionary.
        """
        if self.has_common_metadata:
            metadata_wo_full_names = self._drop_full_names(self)
            return metadata_wo_full_names[self.indicators[0]]
        else:
            return {}

    @staticmethod
    def _drop_full_names(metadata: dict) -> dict:
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
                for meta_name, meta in indicator_metadata.items()
                if "full_name" not in meta_name
            }
            for indicator, indicator_metadata in metadata.items()
        }

    def update_indicator_metadata(
        self, indicator: str, new_metadata: dict
    ) -> "Metadata":
        """
        Update the metadata for a specific indicator.

        Parameters
        ----------
        indicator : str
            The indicator to update.
        new_metadata : dict
            The new metadata to update with.

        Returns
        -------
        Metadata
            The updated metadata.
        """
        self[indicator].update(new_metadata)
        return self

    def update_indicator_metadata_value(
        self, indicator: str, key: str, value: str,
    ) -> "Metadata":
        """
        Update the metadata for a specific indicator.

        Parameters
        ----------
        indicator : str
            The indicator to update.
        new_metadata : dict
            The new metadata to update with.

        Returns
        -------
        Metadata
            The updated metadata.
        """
        self[indicator][key] = value
        return self

    def update_dataset_metadata(self, new_metadata: dict) -> "Metadata":
        """
        Update the metadata for all indicators.

        Parameters
        ----------
        new_metadata : dict
            The new metadata to update with.

        Returns
        -------
        Metadata
            The updated metadata.
        """
        for indicator in self.indicators:
            self[indicator].update(new_metadata)
        return self

    def copy(self) -> "Metadata":
        """
        Create a copy of the metadata.

        Returns
        -------
        Metadata
            The copied metadata.
        """
        return copy.deepcopy(self)

    def save(self, name: str, data_dir: Union[str, Path, None] = None) -> None:
        data_dir = data_dir or get_data_dir()
        data_dir = Path(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        with open(data_dir / f"{name}_metadata.json", "w") as f:
            json.dump(self, f)
        return

    @classmethod
    def from_cast(
        cls, base_metadata: dict, names: list, full_names: list
    ) -> "Metadata":
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
        return cls(cast_metadata(base_metadata, names, full_names))

    @classmethod
    def from_metadatas(cls, metadatas: List[dict]) -> "Metadata":
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
        metadatas_dict = {k: v for d in metadatas for k, v in d.items()}
        return cls(metadatas_dict)


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

    def __init__(self, data: pd.DataFrame, metadata: Metadata, name: str, transformed: bool = False) -> None:
        """
        Initialize the dataset.

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
        """
        self.data = data
        self.metadata = metadata
        self.name = name
        self.transformed = transformed
        self.indicators = self.metadata.indicators

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

    def named_data(self, language: str = "es") -> pd.DataFrame:
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
        name_key = f"full_name_{language}"
        return self.data.rename(
            columns={
                indicator: self.metadata[indicator][name_key]
                for indicator in self.indicators
            }
        )

    def save(self, data_dir: Union[str, Path, None], name: Optional[str] = None) -> None:
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

    def __getitem__(self, indicator) -> "Dataset":
        metadata_dict = {indicator: self.metadata[indicator]}
        return self.__class__(
            data=self.data[[indicator]],
            metadata=Metadata(metadata_dict),
            name=self.name,
        )

    def __repr__(self) -> str:
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
            new_metadata = Metadata.from_metadatas(new_metadatas)

        inferred_frequency = pd.infer_freq(transformed.index)
        new_metadata.update_dataset_metadata({"frequency": inferred_frequency})
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name, transformed=True)
        return output

    def rolling(self, window: int, operation: str = "sum") -> "Dataset":
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
            new_metadata = Metadata.from_metadatas(new_metadatas)
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name, transformed=True)
        return output

    def chg_diff(self, operation: str = "chg", period: str = "last") -> "Dataset":
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
            new_metadata = Metadata.from_metadatas(new_metadatas)
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name, transformed=True)
        return output

    def rebase(self,     start_date: Union[str, datetime],
    end_date: Union[str, datetime, None] = None,
    base: float = 100.0,) -> "Dataset":
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
            new_metadata = Metadata.from_metadatas(new_metadatas)
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name, transformed=True)
        return output

    def convert(self, flavor: str, start_date: Union[str, datetime],
    end_date: Union[str, datetime, None] = None,) -> "Dataset":
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
        assert flavor in ["usd", "real", "gdp"], "Invalid 'flavor' option."

        if self.metadata.has_common_metadata:
            transformed, new_metadata = _rebase(
                data=self.data,
                metadata=self.metadata,
                start_date=start_date,
                end_date=end_date,
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
                )
                transformed.append(transformed_col)
                new_metadatas.append(new_metadata)
            transformed = pd.concat(transformed, axis=1)
            new_metadata = Metadata.from_metadatas(new_metadatas)
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name, transformed=True)
        return output