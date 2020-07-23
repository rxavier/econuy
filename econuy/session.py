import logging
import warnings
from datetime import date
from os import PathLike, path, makedirs
from pathlib import Path
from typing import Union, Optional

import pandas as pd
from sqlalchemy.engine.base import Connection, Engine

from econuy import frequent, transform
from econuy.retrieval import (cpi, nxr, fiscal_accounts, national_accounts,
                              labor, rxr, commodity_index, reserves, trade,
                              public_debt, industrial_production)
from econuy.utils import logutil, ops


class Session(object):
    """
    Main class to access download and processing methods.

    Attributes
    ----------
    location : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
               default 'econuy-data'
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.
    dataset : pd.DataFrame, default pd.DataFrame(index=[], columns=[])
        Current working dataset.
    log : {str, 0, 1, 2}
        Controls how logging works. ``0``: don't log; ``1``: log to console;
        ``2``: log to console and file with default file; ``str``: log to
        console and file with filename=str
    logger : logging.Logger, default None
        Logger object. For most cases this attribute should be ``None``,
        allowing :attr:`log` to control how logging works.
    inplace : bool, default False
        If True, transformation methods will modify the :attr:`dataset`
        inplace and return the input :class:`Session` instance.

    """

    def __init__(self,
                 location: Union[str, PathLike,
                                 Connection, Engine, None] = "econuy-data",
                 revise_rows: Union[int, str] = "nodup",
                 only_get: bool = False,
                 dataset: Union[dict, pd.DataFrame] = pd.DataFrame(),
                 log: Union[int, str] = 1,
                 logger: Optional[logging.Logger] = None,
                 inplace: bool = False):
        self.location = location
        self.revise_rows = revise_rows
        self.only_get = only_get
        self.dataset = dataset
        self.log = log
        self.logger = logger
        self.inplace = inplace

        if isinstance(location, (str, PathLike)):
            if not path.exists(self.location):
                makedirs(self.location)
            loc_text = location
        elif isinstance(location, (Engine, Connection)):
            loc_text = location.engine.url
        else:
            loc_text = "none set."

        if logger is not None:
            self.log = "custom"
        else:
            if isinstance(log, int) and (log < 0 or log > 2):
                raise ValueError("'log' takes either 0 (don't log info),"
                                 " 1 (log to console), 2 (log to console and"
                                 " default file), or str (log to console and "
                                 "file with filename=str).")
            elif log == 2:
                logfile = Path(self.location) / "info.log"
                log_obj = logutil.setup(file=logfile)
                log_method = f"console and file ({logfile.as_posix()})"
            elif isinstance(log, str) and log != "custom":
                logfile = (Path(self.location) / log).with_suffix(".log")
                log_obj = logutil.setup(file=logfile)
                log_method = f"console and file ({logfile.as_posix()})"
            elif log == 1:
                log_obj = logutil.setup(file=None)
                log_method = "console"
            else:
                log_obj = logutil.setup(null=True)
                log_method = "no logging"
            self.logger = log_obj

            if (isinstance(dataset, pd.DataFrame) and
                    dataset.equals(pd.DataFrame(columns=[], index=[]))):
                dataset_message = "empty dataframe"
            else:
                dataset_message = "custom dataset"
            if isinstance(revise_rows, int):
                revise_method = f"{revise_rows} rows to replace"
            else:
                revise_method = revise_rows
            log_obj.info(f"Created Session object with the "
                         f"following attributes:\n"
                         f"Location for downloads and updates: {loc_text}\n"
                         f"Offline: {only_get.__str__()}\n"
                         f"Update method: '{revise_method}'\n"
                         f"Dataset: {dataset_message}\n"
                         f"Logging method: {log_method}")

    def get(self,
            dataset: str,
            update: bool = True,
            save: bool = True,
            **kwargs):
        """
        Main download method.

        Parameters
        ----------
        dataset : {'cpi', 'nxr_monthly', 'nxr_daily', 'fiscal', \
                'public_debt', 'naccounts', 'labor', 'wages', 'rxr_official', \
                'reserves', 'reserves_changes', 'trade', \
                'industrial_production'}
            Type of data to download.
        update : bool, default True
            Whether to update an existing dataset.
        save : bool, default True
            Whether to save the dataset.
        **kwargs
            Keyword arguments.

        Returns
        -------
        :class:`~econuy.session.Session`
            Loads the pd.DataFrame output into the :attr:`dataset`
            attribute.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``dataset`` argument.

        """
        if update is True:
            if isinstance(self.location, (str, PathLike)):
                update_loc = Path(self.location)
            else:
                update_loc = self.location
        else:
            update_loc = None
        if save is True:
            if isinstance(self.location, (str, PathLike)):
                save_loc = Path(self.location)
            else:
                save_loc = self.location
        else:
            save_loc = None

        if dataset == "cpi" or dataset == "prices":
            output = cpi.get(update_loc=update_loc,
                             revise_rows=self.revise_rows,
                             save_loc=save_loc,
                             only_get=self.only_get,
                             **kwargs)
        elif dataset == "industrial_production":
            output = industrial_production.get(update_loc=update_loc,
                                               revise_rows=self.revise_rows,
                                               save_loc=save_loc,
                                               only_get=self.only_get,
                                               **kwargs)
        elif dataset == "fiscal":
            output = fiscal_accounts.get(update_loc=update_loc,
                                         revise_rows=self.revise_rows,
                                         save_loc=save_loc,
                                         only_get=self.only_get,
                                         **kwargs)
        elif dataset == "public_debt":
            output = public_debt.get(update_loc=update_loc,
                                     revise_rows=self.revise_rows,
                                     save_loc=save_loc,
                                     only_get=self.only_get,
                                     **kwargs)
        elif dataset == "nxr_monthly" or dataset == "nxr_m":
            output = nxr.get_monthly(update_loc=update_loc,
                                     revise_rows=self.revise_rows,
                                     save_loc=save_loc,
                                     only_get=self.only_get,
                                     **kwargs)
        elif dataset == "nxr_daily" or dataset == "nxr_d":
            output = nxr.get_daily(update_loc=update_loc,
                                   save_loc=save_loc,
                                   only_get=self.only_get,
                                   **kwargs)
        elif dataset == "naccounts" or dataset == "na":
            output = national_accounts.get(update_loc=update_loc,
                                           revise_rows=self.revise_rows,
                                           save_loc=save_loc,
                                           only_get=self.only_get,
                                           **kwargs)
        elif dataset == "labor" or dataset == "labour":
            output = labor.get_rates(update_loc=update_loc,
                                     revise_rows=self.revise_rows,
                                     save_loc=save_loc,
                                     only_get=self.only_get,
                                     **kwargs)
        elif dataset == "wages":
            output = labor.get_wages(update_loc=update_loc,
                                     revise_rows=self.revise_rows,
                                     save_loc=save_loc,
                                     only_get=self.only_get,
                                     **kwargs)
        elif dataset == "rxr_custom" or dataset == "rxr-custom":
            warnings.warn("'rxr_custom' will only be accesible through the "
                          "'get_custom' method in a future version",
                          FutureWarning)
            output = rxr.get_custom(update_loc=update_loc,
                                    revise_rows=self.revise_rows,
                                    save_loc=save_loc,
                                    only_get=self.only_get,
                                    **kwargs)
        elif dataset == "rxr_official" or dataset == "rxr-official":
            output = rxr.get_official(update_loc=update_loc,
                                      revise_rows=self.revise_rows,
                                      save_loc=save_loc,
                                      only_get=self.only_get,
                                      **kwargs)
        elif dataset == "commodity_index" or dataset == "comm_index":
            warnings.warn("'commodity_index' will only be accesible through "
                          "the 'get_custom' method in a future version",
                          FutureWarning)
            output = commodity_index.get(update_loc=update_loc,
                                         save_loc=save_loc,
                                         only_get=self.only_get,
                                         **kwargs)
        elif dataset == "reserves":
            output = reserves.get(update_loc=update_loc,
                                  save_loc=save_loc,
                                  only_get=self.only_get,
                                  **kwargs)
        elif dataset == "reserves_changes" or dataset == "reserves_chg":
            output = reserves.get_changes(update_loc=update_loc,
                                          save_loc=save_loc,
                                          only_get=self.only_get,
                                          **kwargs)
        elif dataset == "trade":
            output = trade.get(update_loc=update_loc,
                               revise_rows=self.revise_rows,
                               save_loc=save_loc,
                               only_get=self.only_get,
                               **kwargs)
        else:
            raise ValueError("Invalid keyword for 'dataset' parameter.")

        self.dataset = output
        self.logger.info(f"Retrieved '{dataset}' dataset.")

        return self

    def get_frequent(self,
                     dataset: str,
                     update: bool = True,
                     save: bool = True,
                     **kwargs): #pragma: no cover
        """
        Get frequently used datasets.

        This method will be renamed to 'get_custom' in a future version.

        Parameters
        ----------
        dataset : {'cpi_measures', 'fiscal', 'labor', 'real_wages', \
                'net_trade', 'tot', 'net_public_debt', 'commodity_index', \
                'rxr_custom'}
            Type of data to download.
        update : bool, default True
            Whether to update an existing dataset.
        save : bool, default  True
            Whether to save the dataset.
        **kwargs
            Keyword arguments.

        Returns
        -------
        :class:`~econuy.session.Session`
            Loads the downloaded dataframe into the :attr:`dataset` attribute.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``dataset`` argument.

        """
        warnings.warn("This method will be renamed 'get_custom' in a "
                      "future version", FutureWarning)

        if update is True:
            if isinstance(self.location, (str, PathLike)):
                update_loc = Path(self.location)
            else:
                update_loc = self.location
        else:
            update_loc = None
        if save is True:
            if isinstance(self.location, (str, PathLike)):
                save_loc = Path(self.location)
            else:
                save_loc = self.location
        else:
            save_loc = None

        if dataset == "cpi_measures" or dataset == "price_measures":
            output = frequent.cpi_measures(update_loc=update_loc,
                                           save_loc=save_loc,
                                           only_get=self.only_get,
                                           **kwargs)
        elif dataset == "fiscal":
            output = frequent.fiscal(update_loc=update_loc,
                                     save_loc=save_loc,
                                     only_get=self.only_get,
                                     **kwargs)
        elif dataset == "net_public_debt":
            output = frequent.net_public_debt(update_loc=update_loc,
                                              save_loc=save_loc,
                                              only_get=self.only_get,
                                              **kwargs)
        elif dataset == "labor" or dataset == "labour":
            output = frequent.labor_rate_people(update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=self.only_get,
                                                **kwargs)
        elif dataset == "wages" or dataset == "real_wages":
            output = frequent.labor_real_wages(update_loc=update_loc,
                                               save_loc=save_loc,
                                               only_get=self.only_get,
                                               **kwargs)
        elif dataset == "net_trade":
            output = frequent.trade_balance(update_loc=update_loc,
                                            save_loc=save_loc,
                                            only_get=self.only_get,
                                            **kwargs)
        elif dataset == "tot" or dataset == "terms_of_trade":
            output = frequent.terms_of_trade(update_loc=update_loc,
                                             save_loc=save_loc,
                                             only_get=self.only_get,
                                             **kwargs)
        elif dataset == "rxr_custom" or dataset == "rxr-custom":
            output = rxr.get_custom(update_loc=update_loc,
                                    save_loc=save_loc,
                                    only_get=self.only_get,
                                    **kwargs)
        elif dataset == "commodity_index" or dataset == "comm_index":
            output = commodity_index.get(update_loc=update_loc,
                                         save_loc=save_loc,
                                         only_get=self.only_get,
                                         **kwargs)
        else:
            raise ValueError("Invalid keyword for 'dataset' parameter.")

        self.dataset = output
        self.logger.info(f"Retrieved '{dataset}' dataset.")

        return self

    def get_custom(self,
                   dataset: str,
                   update: bool = True,
                   save: bool = True,
                   **kwargs):
        """
        Get custom datasets.

        Parameters
        ----------
        dataset : {'cpi_measures', 'fiscal', 'labor', 'real_wages', \
                'net_trade', 'tot', 'net_public_debt', 'commodity_index', \
                'rxr_custom', 'core_industrial'}
            Type of data to download.
        update : bool, default True
            Whether to update an existing dataset.
        save : bool, default  True
            Whether to save the dataset.
        **kwargs
            Keyword arguments.

        Returns
        -------
        :class:`~econuy.session.Session`
            Loads the downloaded dataframe into the :attr:`dataset` attribute.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``dataset`` argument.

        """
        if update is True:
            if isinstance(self.location, (str, PathLike)):
                update_loc = Path(self.location)
            else:
                update_loc = self.location
        else:
            update_loc = None
        if save is True:
            if isinstance(self.location, (str, PathLike)):
                save_loc = Path(self.location)
            else:
                save_loc = self.location
        else:
            save_loc = None

        if dataset == "cpi_measures" or dataset == "price_measures":
            output = frequent.cpi_measures(update_loc=update_loc,
                                           save_loc=save_loc,
                                           only_get=self.only_get,
                                           **kwargs)
        elif dataset == "core_industrial":
            output = frequent.core_industrial(update_loc=update_loc,
                                              save_loc=save_loc,
                                              only_get=self.only_get,
                                              **kwargs)
        elif dataset == "fiscal":
            output = frequent.fiscal(update_loc=update_loc,
                                     save_loc=save_loc,
                                     only_get=self.only_get,
                                     **kwargs)
        elif dataset == "net_public_debt":
            output = frequent.net_public_debt(update_loc=update_loc,
                                              save_loc=save_loc,
                                              only_get=self.only_get,
                                              **kwargs)
        elif dataset == "labor" or dataset == "labour":
            output = frequent.labor_rate_people(update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=self.only_get,
                                                **kwargs)
        elif dataset == "wages" or dataset == "real_wages":
            output = frequent.labor_real_wages(update_loc=update_loc,
                                               save_loc=save_loc,
                                               only_get=self.only_get,
                                               **kwargs)
        elif dataset == "net_trade":
            output = frequent.trade_balance(update_loc=update_loc,
                                            save_loc=save_loc,
                                            only_get=self.only_get,
                                            **kwargs)
        elif dataset == "tot" or dataset == "terms_of_trade":
            output = frequent.terms_of_trade(update_loc=update_loc,
                                             save_loc=save_loc,
                                             only_get=self.only_get,
                                             **kwargs)
        elif dataset == "rxr_custom" or dataset == "rxr-custom":
            output = rxr.get_custom(update_loc=update_loc,
                                    save_loc=save_loc,
                                    only_get=self.only_get,
                                    **kwargs)
        elif dataset == "commodity_index" or dataset == "comm_index":
            output = commodity_index.get(update_loc=update_loc,
                                         save_loc=save_loc,
                                         only_get=self.only_get,
                                         **kwargs)
        else:
            raise ValueError("Invalid keyword for 'dataset' parameter.")

        self.dataset = output
        self.logger.info(f"Retrieved '{dataset}' dataset.")

        return self

    def resample(self, target: str, operation: str = "sum",
                 interpolation: str = "linear"):
        """
        Resample to target frequencies.

        See Also
        --------
        :func:`~econuy.transform.resample`

        """
        if isinstance(self.dataset, dict):
            output = {}
            for key, value in self.dataset.items():
                table = transform.resample(value, target=target,
                                           operation=operation,
                                           interpolation=interpolation)
                output.update({key: table})
        else:
            output = transform.resample(self.dataset, target=target,
                                        operation=operation,
                                        interpolation=interpolation)
        self.logger.info(f"Applied 'resample' transformation with '{target}' "
                         f"and '{operation}' operation.")
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(location=self.location,
                           revise_rows=self.revise_rows,
                           only_get=self.only_get,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    def chg_diff(self, operation: str = "chg", period_op: str = "last"):
        """
        Calculate pct change or difference.

        See Also
        --------
        :func:`~econuy.transform.chg_diff`

        """
        if isinstance(self.dataset, dict):
            output = {}
            for key, value in self.dataset.items():
                table = transform.chg_diff(value, operation=operation,
                                           period_op=period_op)
                output.update({key: table})
        else:
            output = transform.chg_diff(self.dataset, operation=operation,
                                        period_op=period_op)
        self.logger.info(f"Applied 'chg_diff' transformation with "
                         f"'{operation}' operation and '{period_op}' period.")
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(location=self.location,
                           revise_rows=self.revise_rows,
                           only_get=self.only_get,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    def decompose(self, flavor: str = "both", method: str = "x13",
                  force_x13: bool = False, fallback: str = "loess",
                  trading: bool = True, outlier: bool = True,
                  x13_binary: Union[str, PathLike] = "search",
                  search_parents: int = 1, ignore_warnings: bool = True,
                  **kwargs):
        """
        Apply seasonal decomposition.

        Parameters
        ----------
        flavor : {'both', 'seas', 'trend'}
            Return both seasonally adjusted and trend dataframes or choose
            between them.
        method : {'x13', 'loess', 'ma'}
            Decomposition method. ``X13`` refers to X13 ARIMA from the US
            Census, ``loess`` refers to Loess decomposition and ``ma`` refers
            to moving average decomposition, in all cases as implemented by
            `statsmodels <https://www.statsmodels.org/dev/tsa.html>`_.
        force_x13 : bool, default False
            Whether to try different ``outlier`` and ``trading`` parameters
            in statsmodels' `x13 arima analysis <https://www.statsmodels.org/
            dev/ generated/statsmodels.tsa.x13.x13_arima_analysis.html>`_ for
            each series that fails. If ``False``, jump to the ``fallback``
            method for the whole dataframe at the first error.
        fallback : {'loess', 'ma'}
            Decomposition method to fall back to if ``method="x13"`` fails and
            ``force_x13=False``.
        trading : bool, default True
            Whether to automatically detect trading days in X13 ARIMA.
        outlier : bool, default True
            Whether to automatically detect outliers in X13 ARIMA.
        x13_binary: str, os.PathLike or None, default 'search'
            Location of the X13 binary. If ``search`` is used, will attempt to
            find the binary in the project structure. If ``None``, statsmodels
            will handle it.
        search_parents: int, default 1
            If ``x13_binary=search``, this parameter controls how many parent
            directories to go up before recursively searching for the binary.
        ignore_warnings : bool, default True
            Whether to suppress X13Warnings from statsmodels.
        kwargs
            Keyword arguments passed to statsmodels' ``x13_arima_analysis``,
            ``STL`` and ``seasonal_decompose``.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``flavor`` argument.

        See Also
        --------
        :func:`~econuy.transform.decompose`

        """
        if isinstance(self.dataset, dict):
            output = {}
            for key, value in self.dataset.items():
                table = transform.decompose(value,
                                            flavor=flavor,
                                            method=method,
                                            force_x13=force_x13,
                                            fallback=fallback,
                                            trading=trading,
                                            outlier=outlier,
                                            x13_binary=x13_binary,
                                            search_parents=search_parents,
                                            ignore_warnings=ignore_warnings,
                                            **kwargs)
                output.update({key: table})
        else:
            output = transform.decompose(self.dataset,
                                         flavor=flavor,
                                         method=method,
                                         force_x13=force_x13,
                                         fallback=fallback,
                                         trading=trading,
                                         outlier=outlier,
                                         x13_binary=x13_binary,
                                         search_parents=search_parents,
                                         ignore_warnings=ignore_warnings,
                                         **kwargs)
        self.logger.info(f"Applied 'decompose' transformation with "
                         f"'{method}' method and '{flavor}' flavor.")
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(location=self.location,
                           revise_rows=self.revise_rows,
                           only_get=self.only_get,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    def convert(self, flavor: str, update: bool = True,
                save: bool = True, only_get: bool = True, **kwargs):
        """
        Convert to other units.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``flavor`` argument.

        See Also
        --------
        :func:`~econuy.transform.convert_usd`,
        :func:`~econuy.transform.convert_real`,
        :func:`~econuy.transform.convert_gdp`

        """
        if update is True:
            if isinstance(self.location, (str, PathLike)):
                update_loc = Path(self.location)
            else:
                update_loc = self.location
        else:
            update_loc = None
        if save is True:
            if isinstance(self.location, (str, PathLike)):
                save_loc = Path(self.location)
            else:
                save_loc = self.location
        else:
            save_loc = None

        if isinstance(self.dataset, dict):
            output = {}
            for key, value in self.dataset.items():
                if flavor == "usd":
                    table = transform.convert_usd(value,
                                                  update_loc=update_loc,
                                                  save_loc=save_loc,
                                                  only_get=only_get)
                elif flavor == "real":
                    table = transform.convert_real(value,
                                                   update_loc=update_loc,
                                                   save_loc=save_loc,
                                                   only_get=only_get,
                                                   **kwargs)
                elif flavor == "pcgdp" or flavor == "gdp":
                    table = transform.convert_gdp(value,
                                                  update_loc=update_loc,
                                                  save_loc=save_loc,
                                                  only_get=only_get)
                else:
                    raise ValueError("'flavor' can be one of 'usd', 'real', "
                                     "or 'pcgdp'.")

                output.update({key: table})
        else:
            if flavor == "usd":
                output = transform.convert_usd(self.dataset,
                                               update_loc=update_loc,
                                               save_loc=save_loc,
                                               only_get=only_get)
            elif flavor == "real":
                output = transform.convert_real(self.dataset,
                                                update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=only_get,
                                                **kwargs)
            elif flavor == "pcgdp" or flavor == "gdp":
                output = transform.convert_gdp(self.dataset,
                                               update_loc=update_loc,
                                               save_loc=save_loc,
                                               only_get=only_get)
            else:
                raise ValueError("'flavor' can be one of 'usd', 'real', "
                                 "or 'pcgdp'.")
        self.logger.info(f"Applied 'convert' transformation "
                         f"with '{flavor}' flavor.")
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(location=self.location,
                           revise_rows=self.revise_rows,
                           only_get=self.only_get,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    def base_index(self, start_date: Union[str, date],
                   end_date: Union[str, date, None] = None, base: float = 100):
        """
        Scale to a period or range of periods.

        See Also
        --------
        :func:`~econuy.transform.base_index`

        """
        if isinstance(self.dataset, dict):
            output = {}
            for key, value in self.dataset.items():
                table = transform.base_index(value, start_date=start_date,
                                             end_date=end_date, base=base)
                output.update({key: table})
        else:
            output = transform.base_index(self.dataset, start_date=start_date,
                                          end_date=end_date, base=base)
        self.logger.info("Applied 'base_index' transformation.")
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(location=self.location,
                           revise_rows=self.revise_rows,
                           only_get=self.only_get,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    def rolling(self, periods: Optional[int] = None,
                operation: str = "sum"):
        """
        Calculate rolling averages or sums.

        See Also
        --------
        :func:`~econuy.transform.rolling`

        """
        if isinstance(self.dataset, dict):
            output = {}
            for key, value in self.dataset.items():
                table = transform.rolling(value, periods=periods,
                                          operation=operation)
                output.update({key: table})
        else:
            output = transform.rolling(self.dataset, periods=periods,
                                       operation=operation)
        self.logger.info(f"Applied 'rolling' transformation with "
                         f"{periods} periods and '{operation}' operation.")
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(location=self.location,
                           revise_rows=self.revise_rows,
                           only_get=self.only_get,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    def save(self, name: str, index_label: str = "index"):
        """Save :attr:`dataset` attribute to a CSV or SQL."""
        name = Path(name).with_suffix("").as_posix()

        if isinstance(self.dataset, dict):
            for key, value in self.dataset.items():
                ops._io(operation="save", data_loc=self.location,
                        data=value, name=f"{name}_{key}",
                        index_label=index_label)
        else:
            ops._io(operation="save", data_loc=self.location,
                    data=self.dataset, name=name,
                    index_label=index_label)

        self.logger.info(f"Saved dataset to '{self.location}'.")

    def final(self):
        """Return :attr:`dataset` attribute."""
        self.logger.info("Retrieved dataset from Session() object.")

        return self.dataset
