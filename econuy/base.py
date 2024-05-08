import copy
from typing import List

import pandas as pd

from econuy.transform.change import _chg_diff

from econuy.transform.rolling import _rolling


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

    def __init__(self, data: pd.DataFrame, metadata: Metadata, name: str) -> None:
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
        output = self.__class__(data=transformed, metadata=new_metadata, name=self.name)
        return output
