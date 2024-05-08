import warnings
import copy
from typing import Union, List, Optional

import pandas as pd

from econuy.transform.change import _chg_diff
from econuy.transform.resample import _resample

from econuy.transform.rolling import _rolling


def cast_metadata(indicator_metadata: dict, names: list, full_names: list) -> dict:
    # TODO: Improve this hack
    return {
        name: full_name | indicator_metadata
        for name, full_name in zip(names, full_names)
    }


class Metadata(dict):
    @property
    def indicators(self):
        return list(self.keys())

    @property
    def has_common_metadata(self):
        metadata_wo_full_names = self._drop_full_names(self)
        indicator_metadatas = list(metadata_wo_full_names.values())
        if len(indicator_metadatas) < 2:
            return True
        else:
            reference = indicator_metadatas[0]
            return all(reference == metadata for metadata in indicator_metadatas[1:])

    @property
    def common_metadata_dict(self):
        if self.has_common_metadata:
            metadata_wo_full_names = self._drop_full_names(self)
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
        self[indicator].update(new_metadata)
        return self

    def update_dataset_metadata(self, new_metadata: dict) -> "Metadata":
        for indicator in self.indicators:
            self[indicator].update(new_metadata)
        return self

    def copy(self):
        return copy.deepcopy(self)

    @classmethod
    def from_cast(cls, base_metadata: dict, names: list, full_names: list):
        return cls(cast_metadata(base_metadata, names, full_names))

    @classmethod
    def from_metadatas(cls, metadatas: List[dict]):
        metadatas_dict = {k: v for d in metadatas for k, v in d.items()}
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

    def named_data(self, language: str = "es"):
        name_key = f"full_name_{language}"
        return self.data.rename(
            columns={
                indicator: self.metadata[indicator][name_key]
                for indicator in self.indicators
            }
        )

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
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name)
        return output

    def rolling(
        self, window: Optional[int] = None, operation: str = "sum"
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
            new_metadata = Metadata.from_metadatas(new_metadatas)
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name)
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
        df : pd.DataFrame
            Input dataframe.
        operation : {'chg', 'diff'}
            ``chg`` for percent change or ``diff`` for differences.
        period : {'last', 'inter', 'annual'}
            Period with which to calculate change or difference. ``last`` for
            previous period (last month for monthly data), ``inter`` for same
            period last year, ``annual`` for same period last year but taking
            annual sums.

        Returns
        -------
        ``None``

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
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name)
        return output
