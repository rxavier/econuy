import logging
from datetime import date
from os import PathLike, path, makedirs
from pathlib import Path
from typing import Union, Optional

import pandas as pd
from sqlalchemy.engine.base import Connection, Engine

from econuy import transform
from econuy.retrieval import (prices, fiscal_accounts, economic_activity,
                              labor, external_sector, financial_sector, income,
                              international, regional)
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
        dataset : str, see available options in datasets.py
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
            output = prices.cpi(update_loc=update_loc,
                                revise_rows=self.revise_rows,
                                save_loc=save_loc,
                                only_get=self.only_get,
                                **kwargs)
        elif dataset == "industrial_production":
            output = economic_activity.industrial_production(
                update_loc=update_loc,
                revise_rows=self.revise_rows,
                save_loc=save_loc,
                only_get=self.only_get,
                **kwargs)
        elif dataset == "balance":
            output = fiscal_accounts.balance(update_loc=update_loc,
                                             revise_rows=self.revise_rows,
                                             save_loc=save_loc,
                                             only_get=self.only_get,
                                             **kwargs)
        elif dataset == "public_debt":
            output = fiscal_accounts.public_debt(update_loc=update_loc,
                                                 revise_rows=self.revise_rows,
                                                 save_loc=save_loc,
                                                 only_get=self.only_get,
                                                 **kwargs)
        elif dataset == "nxr_monthly" or dataset == "nxr_m":
            output = prices.nxr_monthly(update_loc=update_loc,
                                        revise_rows=self.revise_rows,
                                        save_loc=save_loc,
                                        only_get=self.only_get,
                                        **kwargs)
        elif dataset == "nxr_daily" or dataset == "nxr_d":
            output = prices.nxr_daily(update_loc=update_loc,
                                      save_loc=save_loc,
                                      only_get=self.only_get,
                                      **kwargs)
        elif dataset == "naccounts" or dataset == "na":
            output = economic_activity.national_accounts(
                update_loc=update_loc,
                revise_rows=self.revise_rows,
                save_loc=save_loc,
                only_get=self.only_get,
                **kwargs)
        elif dataset == "labor" or dataset == "labour":
            output = labor.labor_rates(update_loc=update_loc,
                                       revise_rows=self.revise_rows,
                                       save_loc=save_loc,
                                       only_get=self.only_get,
                                       **kwargs)
        elif dataset == "wages":
            output = labor.nominal_wages(update_loc=update_loc,
                                         revise_rows=self.revise_rows,
                                         save_loc=save_loc,
                                         only_get=self.only_get,
                                         **kwargs)
        elif dataset == "real_wages":
            output = labor.real_wages(update_loc=update_loc,
                                      save_loc=save_loc,
                                      only_get=self.only_get,
                                      **kwargs)
        elif dataset == "rxr_official" or dataset == "rxr-official":
            output = external_sector.rxr_official(update_loc=update_loc,
                                                  revise_rows=self.revise_rows,
                                                  save_loc=save_loc,
                                                  only_get=self.only_get,
                                                  **kwargs)
        elif dataset == "reserves":
            output = external_sector.reserves(update_loc=update_loc,
                                              save_loc=save_loc,
                                              only_get=self.only_get,
                                              **kwargs)
        elif dataset == "reserves_changes" or dataset == "reserves_chg":
            output = external_sector.reserves_changes(update_loc=update_loc,
                                                      save_loc=save_loc,
                                                      only_get=self.only_get,
                                                      **kwargs)
        elif dataset == "trade":
            output = external_sector.trade(update_loc=update_loc,
                                           revise_rows=self.revise_rows,
                                           save_loc=save_loc,
                                           only_get=self.only_get,
                                           **kwargs)
        elif dataset == "call":
            output = financial_sector.call_rate(update_loc=update_loc,
                                                revise_rows=self.revise_rows,
                                                save_loc=save_loc,
                                                only_get=self.only_get,
                                                **kwargs)
        elif dataset == "deposits":
            output = financial_sector.deposits(update_loc=update_loc,
                                               revise_rows=self.revise_rows,
                                               save_loc=save_loc,
                                               only_get=self.only_get,
                                               **kwargs)
        elif dataset == "credit" or dataset == "credits":
            output = financial_sector.credit(update_loc=update_loc,
                                             revise_rows=self.revise_rows,
                                             save_loc=save_loc,
                                             only_get=self.only_get,
                                             **kwargs)
        elif dataset == "interest_rates":
            output = financial_sector.interest_rates(
                update_loc=update_loc,
                revise_rows=self.revise_rows,
                save_loc=save_loc,
                only_get=self.only_get,
                **kwargs)
        elif dataset == "taxes":
            output = fiscal_accounts.tax_revenue(update_loc=update_loc,
                                                 revise_rows=self.revise_rows,
                                                 save_loc=save_loc,
                                                 only_get=self.only_get,
                                                 **kwargs)
        elif dataset == "diesel":
            output = economic_activity.diesel(update_loc=update_loc,
                                              revise_rows=self.revise_rows,
                                              save_loc=save_loc,
                                              only_get=self.only_get,
                                              **kwargs)
        elif dataset == "gasoline":
            output = economic_activity.gasoline(update_loc=update_loc,
                                                revise_rows=self.revise_rows,
                                                save_loc=save_loc,
                                                only_get=self.only_get,
                                                **kwargs)
        elif dataset == "electricity":
            output = economic_activity.electricity(
                update_loc=update_loc,
                revise_rows=self.revise_rows,
                save_loc=save_loc,
                only_get=self.only_get,
                **kwargs)
        elif dataset == "hours":
            output = labor.hours(update_loc=update_loc,
                                 revise_rows=self.revise_rows,
                                 save_loc=save_loc,
                                 only_get=self.only_get,
                                 **kwargs)
        elif dataset == "household_income":
            output = income.income_household(update_loc=update_loc,
                                             revise_rows=self.revise_rows,
                                             save_loc=save_loc,
                                             only_get=self.only_get,
                                             **kwargs)
        elif dataset == "capita_income":
            output = income.income_capita(update_loc=update_loc,
                                          revise_rows=self.revise_rows,
                                          save_loc=save_loc,
                                          only_get=self.only_get,
                                          **kwargs)
        elif dataset == "cattle":
            output = economic_activity.cattle(update_loc=update_loc,
                                              revise_rows=self.revise_rows,
                                              save_loc=save_loc,
                                              only_get=self.only_get,
                                              **kwargs)
        elif dataset == "milk":
            output = economic_activity.milk(update_loc=update_loc,
                                            revise_rows=self.revise_rows,
                                            save_loc=save_loc,
                                            only_get=self.only_get,
                                            **kwargs)
        elif dataset == "cement":
            output = economic_activity.cement(update_loc=update_loc,
                                              revise_rows=self.revise_rows,
                                              save_loc=save_loc,
                                              only_get=self.only_get,
                                              **kwargs)
        elif dataset == "consumer_confidence":
            output = income.consumer_confidence(update_loc=update_loc,
                                                revise_rows=self.revise_rows,
                                                save_loc=save_loc,
                                                only_get=self.only_get,
                                                **kwargs)
        elif dataset == "sovereign_risk":
            output = financial_sector.sovereign_risk(
                update_loc=update_loc,
                revise_rows=self.revise_rows,
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
        dataset : str, see available options in datasets.py
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
            output = prices.cpi_measures(update_loc=update_loc,
                                         revise_rows=self.revise_rows,
                                         save_loc=save_loc,
                                         only_get=self.only_get,
                                         **kwargs)
        elif dataset == "core_industrial":
            output = economic_activity.core_industrial(update_loc=update_loc,
                                                       save_loc=save_loc,
                                                       only_get=self.only_get,
                                                       **kwargs)
        elif dataset == "balance_fss":
            output = fiscal_accounts.balance_fss(update_loc=update_loc,
                                                 save_loc=save_loc,
                                                 only_get=self.only_get,
                                                 **kwargs)
        elif dataset == "net_public_debt":
            output = fiscal_accounts.net_public_debt(update_loc=update_loc,
                                                     save_loc=save_loc,
                                                     only_get=self.only_get,
                                                     **kwargs)
        elif dataset == "rates_people":
            output = labor.rates_people(update_loc=update_loc,
                                        save_loc=save_loc,
                                        only_get=self.only_get,
                                        **kwargs)
        elif dataset == "net_trade":
            output = external_sector.trade_balance(update_loc=update_loc,
                                                   save_loc=save_loc,
                                                   only_get=self.only_get,
                                                   **kwargs)
        elif dataset == "tot" or dataset == "terms_of_trade":
            output = external_sector.terms_of_trade(update_loc=update_loc,
                                                    save_loc=save_loc,
                                                    only_get=self.only_get,
                                                    **kwargs)
        elif dataset == "rxr_custom" or dataset == "rxr-custom":
            output = external_sector.rxr_custom(update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=self.only_get,
                                                **kwargs)
        elif dataset == "commodity_index" or dataset == "comm_index":
            output = external_sector.commodity_index(update_loc=update_loc,
                                                     save_loc=save_loc,
                                                     only_get=self.only_get,
                                                     **kwargs)
        elif dataset == "bonds":
            output = financial_sector.bonds(update_loc=update_loc,
                                            revise_rows=self.revise_rows,
                                            save_loc=save_loc,
                                            only_get=self.only_get,
                                            **kwargs)
        elif dataset == "global_gdp":
            output = international.gdp(update_loc=update_loc,
                                       revise_rows=self.revise_rows,
                                       save_loc=save_loc,
                                       only_get=self.only_get,
                                       **kwargs)
        elif dataset == "global_stocks":
            output = international.stocks(update_loc=update_loc,
                                          revise_rows=self.revise_rows,
                                          save_loc=save_loc,
                                          only_get=self.only_get,
                                          **kwargs)
        elif dataset == "global_policy_rates":
            output = international.policy_rates(update_loc=update_loc,
                                                revise_rows=self.revise_rows,
                                                save_loc=save_loc,
                                                only_get=self.only_get,
                                                **kwargs)
        elif dataset == "global_long_rates":
            output = international.long_rates(update_loc=update_loc,
                                              revise_rows=self.revise_rows,
                                              save_loc=save_loc,
                                              only_get=self.only_get,
                                              **kwargs)
        elif dataset == "global_nxr":
            output = international.nxr(update_loc=update_loc,
                                       revise_rows=self.revise_rows,
                                       save_loc=save_loc,
                                       only_get=self.only_get,
                                       **kwargs)
        elif dataset == "regional_gdp":
            output = regional.gdp(update_loc=update_loc,
                                  revise_rows=self.revise_rows,
                                  save_loc=save_loc,
                                  only_get=self.only_get,
                                  **kwargs)
        elif dataset == "regional_monthly_gdp":
            output = regional.monthly_gdp(update_loc=update_loc,
                                          revise_rows=self.revise_rows,
                                          save_loc=save_loc,
                                          only_get=self.only_get,
                                          **kwargs)
        elif dataset == "regional_cpi":
            output = regional.cpi(update_loc=update_loc,
                                  revise_rows=self.revise_rows,
                                  save_loc=save_loc,
                                  only_get=self.only_get,
                                  **kwargs)
        elif dataset == "regional_nxr":
            output = regional.nxr(update_loc=update_loc,
                                  revise_rows=self.revise_rows,
                                  save_loc=save_loc,
                                  only_get=self.only_get,
                                  **kwargs)
        elif dataset == "regional_embi_spreads":
            output = regional.embi_spreads(update_loc=update_loc,
                                           revise_rows=self.revise_rows,
                                           save_loc=save_loc,
                                           only_get=self.only_get,
                                           **kwargs)
        elif dataset == "regional_embi_yields":
            output = regional.embi_yields(update_loc=update_loc,
                                          revise_rows=self.revise_rows,
                                          save_loc=save_loc,
                                          only_get=self.only_get,
                                          **kwargs)
        elif dataset == "regional_policy_rates":
            output = regional.policy_rates(update_loc=update_loc,
                                           revise_rows=self.revise_rows,
                                           save_loc=save_loc,
                                           only_get=self.only_get,
                                           **kwargs)
        elif dataset == "regional_stocks":
            output = regional.stocks(update_loc=update_loc,
                                     revise_rows=self.revise_rows,
                                     save_loc=save_loc,
                                     only_get=self.only_get,
                                     **kwargs)
        elif dataset == "regional_rxr":
            output = regional.rxr(update_loc=update_loc,
                                  revise_rows=self.revise_rows,
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
