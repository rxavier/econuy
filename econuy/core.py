from __future__ import annotations
import copy
from typing import Union, Optional, Dict
from os import PathLike
from datetime import datetime

import pandas as pd
from sqlalchemy.engine.base import Engine, Connection

from econuy.utils import ops, datasets
from econuy import transform


class Pipeline(object):
    """
    Main class to access download and transformation methods.

    Attributes
    ----------
    location : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
               default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating and saving, SQLAlchemy connection or engine object,
        or ``None``, don't save or update.
    download : bool, default True
        If False the ``get`` method will only try to retrieve data on disk.
    always_Save : bool, default True
        If True, save every retrieved dataset to the specified ``location``.
    read_fmt : {'csv', 'xls', 'xlsx'}
        File format of previously downloaded data. Ignored if ``location``
        points to a SQL object.
    save_fmt : {'csv', 'xls', 'xlsx'}
        File format for saving. Ignored if ``location`` points to a SQL object.
    read_header : {'included', 'separate', None}
        Location of dataset metadata headers. 'included' means they are in the
        first 9 rows of the dataset. 'separate' means they are in a separate
        Excel sheet (if ``read_fmt='csv'``, headers are discarded).
        None means there are no metadata headers.
    save_header : {'included', 'separate', None}
        Location of dataset metadata headers. 'included' means they will be set
        as the first 9 rows of the dataset. 'separate' means they will be saved
        in a separate Excel sheet (if ``save_fmt='csv'``, headers are
        discarded). None discards any headers.
    errors : {'raise', 'coerce', 'ignore'}
        How to handle errors that arise from transformations. ``raise`` will
        raise a ValueError, ``coerce`` will force the data into ``np.nan`` and
        ``ignore`` will leave the input data as is.

    """

    def __init__(
        self,
        location: Union[str, PathLike, Engine, Connection, None] = None,
        download: bool = True,
        always_save: bool = True,
        read_fmt: str = "csv",
        read_header: Optional[str] = "included",
        save_fmt: str = "csv",
        save_header: Optional[str] = "included",
        errors: str = "raise",
    ):
        self.location = location
        self.download = download
        self.always_save = always_save
        self.read_fmt = read_fmt
        self.read_header = read_header
        self.save_fmt = save_fmt
        self.save_header = save_header
        self.errors = errors
        self._name = None
        self._dataset = pd.DataFrame()
        self._download_commodity_weights = False

    @property
    def dataset(self) -> pd.DataFrame:
        """Get dataset."""
        return self._dataset

    @property
    def dataset_flat(self) -> pd.DataFrame:
        """Get dataset with no metadata in its column names."""
        nometa = self._dataset.copy(deep=True)
        nometa.columns = nometa.columns.get_level_values(0)
        return nometa

    @property
    def name(self) -> str:
        """Get dataset name."""
        return self._name

    @property
    def description(self) -> str:
        """Get dataset description."""
        try:
            return self.available_datasets()[self.name]["description"]
        except KeyError:
            return None

    @staticmethod
    def available_datasets() -> Dict:
        """Get a dictionary with all available datasets.

        The dictionary is separated by original and custom keys, which denote
        whether the dataset has been modified in some way or if its as provided
        by the source
        """
        all = datasets.original()
        all.update(datasets.custom())
        return all

    def __repr__(self):
        return f"Pipeline(location={self.location})\n" f"Current dataset: {self.name}"

    def copy(self, deep: bool = False) -> Pipeline:
        """Copy or deepcopy a Pipeline object.

        Parameters
        ----------
        deep : bool, default True
            If True, deepcopy.

        Returns
        -------
        :class:`~econuy.core.Pipeline`

        """
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)

    def get(self, name: str):
        """
        Main download method.

        Parameters
        ----------
        name : str
            Dataset to download, see available options in
            :mod:`~econuy.core.Pipeline.available_datasets`.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``name`` argument.

        """
        if name not in self.available_datasets().keys():
            raise ValueError("Invalid dataset selected.")

        if self.location is None:
            prev_data = pd.DataFrame()
        else:
            prev_data = ops._io(
                operation="read",
                data_loc=self.location,
                name=name,
                file_fmt=self.read_fmt,
                multiindex=self.read_header,
            )
        if not self.download and not prev_data.empty:
            self._dataset = prev_data
            print(f"{name}: previous data found.")
        else:
            if not self.download:
                print(f"{name}: previous data not found, downloading.")
            selection = self.available_datasets()[name]
            if name in [
                "trade_balance",
                "terms_of_trade",
                "rxr_custom",
                "commodity_index",
                "cpi_measures",
                "_lin_gdp",
                "net_public_debt",
                "balance_summary",
                "core_industrial",
                "regional_embi_yields",
                "regional_rxr",
                "regional_stocks",
                "real_wages",
                "labor_rates_people",
            ]:
                # Some datasets require retrieving other datasets. Passing the
                # class instance allows running these retrieval operations
                # with the same parameters (for example, save file formats).
                new_data = selection["function"](pipeline=self)
            elif name in ["reserves_changes", "nxr_daily"]:
                # Datasets that require many requests (mostly daily data) benefit
                # from having previous data, so they can start requests
                # from the last available date.
                new_data = selection["function"](pipeline=self, previous_data=prev_data)
            else:
                new_data = selection["function"]()
            data = ops._revise(new_data=new_data, prev_data=prev_data, revise_rows="nodup")
            self._dataset = data
        self._name = name
        if self.always_save and (self.download or prev_data.empty) and self.location is not None:
            self.save()
        return

    def resample(
        self,
        rule: Union[pd.DateOffset, pd.Timedelta, str],
        operation: str = "sum",
        interpolation: str = "linear",
        warn: bool = False,
    ):
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
        warn : bool, default False
            If False, don't raise warnings with incomplete time-range bins.

        Returns
        -------
        ``None``

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
        if self.dataset.empty:
            raise ValueError(
                "Can't use transformation methods without " "retrieving a dataset first."
            )
        output = transform.resample(
            self.dataset, rule=rule, operation=operation, interpolation=interpolation, warn=warn
        )
        self._dataset = output
        return

    def chg_diff(self, operation: str = "chg", period: str = "last"):
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
            If the dataframe is not of frequency ``M`` (month), ``Q`` or
            ``Q-DEC`` (quarter), or ``A`` or ``A-DEC`` (year).
        ValueError
            If the ``operation`` parameter does not have a valid argument.
        ValueError
            If the ``period`` parameter does not have a valid argument.
        ValueError
            If the input dataframe's columns do not have the appropiate levels.

        """
        if self.dataset.empty:
            raise ValueError(
                "Can't use transformation methods without " "retrieving a dataset first."
            )
        output = transform.chg_diff(self.dataset, operation=operation, period=period)
        self._dataset = output
        return

    def decompose(
        self,
        component: str = "seas",
        method: str = "x13",
        force_x13: bool = False,
        fallback: str = "loess",
        trading: bool = True,
        outlier: bool = True,
        x13_binary: Union[str, PathLike] = "search",
        search_parents: int = 0,
        ignore_warnings: bool = True,
        **kwargs,
    ):
        """Apply seasonal decomposition.

        Decompose the series in a Pandas dataframe using either X13 ARIMA, Loess
        or moving averages. X13 can be forced in case of failure by alternating
        the underlying function's parameters. If not, it will fall back to one of
        the other methods. If the X13 method is chosen, the X13 binary has to be
        provided. Please refer to the README for instructions on where to get this
        binary.

        Parameters
        ----------
        component : {'seas', 'trend'}
            Return seasonally adjusted or trend component.
        method : {'x13', 'loess', 'ma'}
            Decomposition method. ``X13`` refers to X13 ARIMA from the US Census,
            ``loess`` refers to Loess decomposition and ``ma`` refers to moving
            average decomposition, in all cases as implemented by
            `statsmodels <https://www.statsmodels.org/dev/tsa.html>`_.
        force_x13 : bool, default False
            Whether to try different ``outlier`` and ``trading`` parameters
            in statsmodels' `x13 arima analysis <https://www.statsmodels.org/dev/
            generated/statsmodels.tsa.x13.x13_arima_analysis.html>`_ for each
            series that fails. If ``False``, jump to the ``fallback`` method for
            the whole dataframe at the first error.
        fallback : {'loess', 'ma'}
            Decomposition method to fall back to if ``method="x13"`` fails and
            ``force_x13=False``.
        trading : bool, default True
            Whether to automatically detect trading days in X13 ARIMA.
        outlier : bool, default True
            Whether to automatically detect outliers in X13 ARIMA.
        x13_binary: str, os.PathLike or None, default 'search'
            Location of the X13 binary. If ``search`` is used, will attempt to find
            the binary in the project structure. If ``None``, statsmodels will
            handle it.
        search_parents: int, default 0
            If ``x13_binary=search``, this parameter controls how many parent
            directories to go up before recursively searching for the binary.
        ignore_warnings : bool, default True
            Whether to suppress X13Warnings from statsmodels.
        kwargs
            Keyword arguments passed to statsmodels' ``x13_arima_analysis``,
            ``STL`` and ``seasonal_decompose``.

        Returns
        -------
        ``None``

        Raises
        ------
        ValueError
            If the ``method`` parameter does not have a valid argument.
        ValueError
            If the ``component`` parameter does not have a valid argument.
        ValueError
            If the ``fallback`` parameter does not have a valid argument.
        ValueError
            If the ``errors`` parameter does not have a valid argument.
        FileNotFoundError
            If the path provided for the X13 binary does not point to a file and
            ``method='x13'``.

        """
        if self.dataset.empty:
            raise ValueError(
                "Can't use transformation methods without " "retrieving a dataset first."
            )
        valid_component = ["seas", "trend"]
        if component not in valid_component:
            raise ValueError(
                f"Only {', '.join(valid_component)} are allowed." f"See underlying 'decompose'."
            )

        output = transform.decompose(
            self.dataset,
            component=component,
            method=method,
            force_x13=force_x13,
            fallback=fallback,
            trading=trading,
            outlier=outlier,
            x13_binary=x13_binary,
            search_parents=search_parents,
            ignore_warnings=ignore_warnings,
            errors=self.errors,
            **kwargs,
        )
        self._dataset = output
        return

    def convert(
        self,
        flavor: str,
        start_date: Union[str, datetime, None] = None,
        end_date: Union[str, datetime, None] = None,
    ):
        """Convert dataframe from UYU to USD, from UYU to real UYU or
        from UYU/USD to % GDP.

        ``flavor=usd``: Convert a dataframe's columns from Uruguayan pesos to US dollars. Call the
        :mod:`~econuy.core.Pipeline.get` function to obtain nominal
        exchange rates, and take into account whether the input dataframe's
        ``Type``, as defined by its multiindex, is flow or stock, in order to `
        choose end of period or monthly average NXR. Also take into account the
        input dataframe's frequency and whether columns represent rolling averages
        or sums.

        ``flavor=real``: Convert a dataframe's columns to real prices. Call the
        :mod:`~econuy.core.Pipeline.get` method to obtain the consumer price
        index. take into account the input dataframe's frequency and whether
        columns represent rolling averages or sums. Allow choosing a single period,
        a range of dates or no period as a base (i.e., period for which the
        average/sum of input dataframe and output dataframe is the same).

        ``flavor=gdp``: Convert a dataframe's columns to percentage of GDP. Call the
        the :mod:`~econuy.core.Pipeline.get` method to obtain UYU and USD quarterly GDP series.
        Take into account the input dataframe's
        currency for chossing UYU or USD GDP. If frequency of input dataframe is
        higher than quarterly, GDP will be upsampled and linear interpolation will
        be performed to complete missing data.
        If input dataframe's "Acum." level is not 12 for monthly frequency or 4
        for quarterly frequency, calculate rolling input dataframe.

        In all cases, if input dataframe's frequency is higher than monthly
        (daily, business, etc.), resample to monthly frequency.

        Parameters
        ----------
        pipeline : econuy.core.Pipeline or None, default None
            An instance of the econuy Pipeline class.
        start_date : str, datetime.date or None, default None
            Only used if ``flavor=real``. If set to a date-like string or a
            date, and ``end_date`` is None, the base period will be
            ``start_date``.
        end_date : str, datetime.date or None, default None
            Only used if ``flavor=real``. If ``start_date`` is set, calculate
            so that the data is in constant prices of ``start_date-end_date``.
        errors : {'raise', 'coerce', 'ignore'}
            What to do when a column in the input dataframe is not expressed in
            Uruguayan pesos. ``raise`` will raise a ValueError, ``coerce`` will
            force the entire column into ``np.nan`` and ``ignore`` will leave the
            input column as is.

        Returns
        -------
        ``None``

        Raises
        ------
        ValueError
            If the ``errors`` parameter does not have a valid argument.
        ValueError
            If the input dataframe's columns do not have the appropiate levels.

        """
        if self.dataset.empty:
            raise ValueError(
                "Can't use transformation methods without " "retrieving a dataset first."
            )
        if flavor not in ["usd", "real", "gdp", "pcgdp"]:
            raise ValueError("'flavor' can be one of 'usd', 'real', " "or 'gdp'.")

        if flavor == "usd":
            output = transform.convert_usd(self.dataset, errors=self.errors, pipeline=self)
        elif flavor == "real":
            output = transform.convert_real(
                self.dataset,
                start_date=start_date,
                end_date=end_date,
                errors=self.errors,
                pipeline=self,
            )
        else:
            output = transform.convert_gdp(self.dataset, errors=self.errors, pipeline=self)

        self._dataset = output
        return

    def rebase(
        self,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime, None] = None,
        base: Union[float, int] = 100.0,
    ):
        """Rebase all dataframe columns to a date or range of dates.

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
        ``None``

        """
        if self.dataset.empty:
            raise ValueError(
                "Can't use transformation methods without " "retrieving a dataset first."
            )
        output = transform.rebase(
            self.dataset, start_date=start_date, end_date=end_date, base=base
        )
        self._dataset = output
        return

    def rolling(self, window: Optional[int] = None, operation: str = "sum"):
        """
        Wrapper for the `rolling method <https://pandas.pydata.org/pandas-docs/
        stable/reference/api/pandas.DataFrame.rolling.html>`_ in Pandas that
        integrates with econuy dataframes' metadata.

        If ``periods`` is ``None``, try to infer the frequency and set ``periods``
        according to the following logic: ``{'A': 1, 'Q-DEC': 4, 'M': 12}``, that
        is, each period will be calculated as the sum or mean of the last year.

        Parameters
        ----------
        window : int, default None
            How many periods the window should cover.
        operation : {'sum', 'mean'}
            Operation used to calculate rolling windows.

        Returns
        -------
        ``None``

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
        if self.dataset.empty:
            raise ValueError(
                "Can't use transformation methods without " "retrieving a dataset first."
            )
        output = transform.rolling(self.dataset, window=window, operation=operation)
        self._dataset = output
        return

    def save(self):
        """Write held dataset.

        Raises
        -------
        ValueError
            If `dataset` is an empty DataFrame or `self.location` is None.

        """
        if self.dataset.empty:
            raise ValueError("Can't save without " "retrieving a dataset first.")
        if self.location is None:
            raise ValueError("No save location defined.")
        else:
            ops._io(
                operation="save",
                data_loc=self.location,
                name=self.name,
                file_fmt=self.save_fmt,
                multiindex=self.save_header,
                data=self.dataset,
            )
        return
