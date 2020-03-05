import logging
from datetime import date
from os import PathLike, path, makedirs, mkdir
from pathlib import Path
from typing import Union, Optional

import pandas as pd

from econuy import frequent, transform
from econuy.resources import logutil
from econuy.retrieval import (cpi, nxr, fiscal_accounts, national_accounts,
                              labor, rxr, commodity_index, reserves)


class Session(object):
    """
    Main class to access download and processing methods.

    Attributes
    ----------
    loc_dir : str or os.PathLike, default 'econuy-data'
        Directory indicating where data will be saved to and retrieved from.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    force_update : bool, default False
        Whether to force download even if data was recently modified.
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
                 loc_dir: Union[str, PathLike] = "econuy-data",
                 revise_rows: Union[int, str] = "nodup",
                 force_update: bool = False,
                 dataset: Union[dict, pd.DataFrame] = pd.DataFrame(),
                 log: Union[int, str] = 1,
                 logger: Optional[logging.Logger] = None,
                 inplace: bool = False):
        self.loc_dir = loc_dir
        self.revise_rows = revise_rows
        self.force_update = force_update
        self.dataset = dataset
        self.log = log
        self.logger = logger
        self.inplace = inplace

        if not path.exists(self.loc_dir):
            makedirs(self.loc_dir)

        if logger is not None:
            self.log = "custom"
        else:
            if isinstance(log, int) and (log < 0 or log > 2):
                raise ValueError("'log' takes either 0 (don't log info),"
                                 " 1 (log to console), 2 (log to console and"
                                 " default file), or str (log to console and "
                                 "file with filename=str).")
            elif log == 2:
                logfile = Path(self.loc_dir) / "info.log"
                log_obj = logutil.setup(file=logfile)
                log_method = f"console and file ({logfile.as_posix()})"
            elif isinstance(log, str) and log != "custom":
                logfile = (Path(self.loc_dir) / log).with_suffix(".log")
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
                         f"Directory for downloads and updates: {loc_dir}\n"
                         f"Update method: {revise_method}\n"
                         f"Dataset: {dataset_message}\n"
                         f"Logging method: {log_method}")

    @logutil.log_getter
    def get(self,
            dataset: str,
            update: bool = True,
            save: bool = True,
            override: Optional[str] = None,
            **kwargs):
        """
        Main download method.

        Parameters
        ----------
        dataset : {'cpi', 'nxr', 'fiscal', 'naccounts', 'labor', \
                'comm_index', 'rxr_custom', 'rxr_official', 'reserves', \
                'fx_ops'}
            Type of data to download.
        update : bool, default True
            Whether to update an existing dataset.
        save : bool, default True
            Whether to save the dataset.
        override : str, default None
            If not None, overrides the saved dataset's default filename.
        **kwargs
            These arguments are passed only to
            :func:`econuy.retrieval.commodity_index.get`. There's two
            options: ``force_update_weights: bool`` and
            ``force_update_prices: bool`` which are self-explanatory. Generally
            you will need to update prices but not weights since the latter are
            annual and take a long time to download.

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
            update_path = Path(self.loc_dir)
        else:
            update_path = None
        if save is True:
            save_path = Path(self.loc_dir)
        else:
            save_path = None

        if kwargs:
            keywords = ", ".join(f"{k}={v}" for k, v in kwargs.items())
            self.logger.info(f"Used the following keyword "
                             f"arguments: {keywords}")

        if dataset == "cpi" or dataset == "prices":
            output = cpi.get(update=update_path,
                             revise_rows=self.revise_rows,
                             save=save_path,
                             force_update=self.force_update,
                             name=override)
        elif dataset == "fiscal":
            output = fiscal_accounts.get(update=update_path,
                                         revise_rows=self.revise_rows,
                                         save=save_path,
                                         force_update=self.force_update,
                                         name=override)
        elif dataset == "nxr":
            output = nxr.get(update=update_path,
                             revise_rows=self.revise_rows,
                             save=save_path,
                             force_update=self.force_update,
                             name=override)
        elif dataset == "naccounts" or dataset == "na":
            output = national_accounts.get(update=update_path,
                                           revise_rows=self.revise_rows,
                                           save=save_path,
                                           force_update=self.force_update,
                                           name=override)
        elif dataset == "labor" or dataset == "labour":
            output = labor.get(update=update_path,
                               revise_rows=self.revise_rows,
                               save=save_path,
                               force_update=self.force_update,
                               name=override)
        elif dataset == "rxr_custom" or dataset == "rxr-custom":
            output = rxr.get_custom(update=update_path,
                                    revise_rows=self.revise_rows,
                                    save=save_path,
                                    force_update=self.force_update,
                                    name=override)
        elif dataset == "rxr_official" or dataset == "rxr-official":
            output = rxr.get_official(update=update_path,
                                      revise_rows=self.revise_rows,
                                      save=save_path,
                                      force_update=self.force_update,
                                      name=override)
        elif dataset == "commodity_index" or dataset == "comm_index":
            output = commodity_index.get(update=update_path,
                                         save=save_path,
                                         name=override,
                                         **kwargs)
        elif dataset == "reserves":
            output = reserves.get_chg(update=update_path,
                                      save=save_path,
                                      name=override)
        elif dataset == "fx_ops" or dataset == "fxops":
            output = reserves.get_operations(update=update_path,
                                             save=save_path,
                                             name=override)
        else:
            raise ValueError("Invalid keyword for 'dataset' parameter.")

        self.dataset = output

        return self

    @logutil.log_getter
    def get_tfm(self,
                dataset: str,
                update: bool = True,
                save: bool = True,
                override: Optional[str] = None,
                **kwargs):
        """
        Get frequently used datasets.

        Parameters
        ----------
        dataset : {'inflation', 'fiscal', 'nxr', 'naccounts', 'labor'}
            Type of data to download.
        update : bool, default True
            Whether to update an existing dataset.
        save : bool, default  True
            Whether to save the dataset.
        override : str, default None
            If not None, overrides the saved dataset's default filename.
        **kwargs
            Keyword arguments passed to functions in
            :mod:`econuy.frequent`.

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
            update_path = Path(self.loc_dir)
        else:
            update_path = None
        if save is True:
            save_path = Path(self.loc_dir)
        else:
            save_path = None

        if dataset == "inflation":
            called_args = logutil.get_called_args(frequent.inflation,
                                                  kwargs)
            output = frequent.inflation(update=update_path,
                                        save=save_path,
                                        name=override)
        elif dataset == "fiscal":
            called_args = logutil.get_called_args(frequent.fiscal,
                                                  kwargs)
            output = frequent.fiscal(update=update_path,
                                     save=save_path,
                                     name=override,
                                     **kwargs)
        elif dataset == "nxr":
            called_args = logutil.get_called_args(frequent.exchange_rate,
                                                  kwargs)
            output = frequent.exchange_rate(update=update_path,
                                            save=save_path,
                                            name=override,
                                            **kwargs)
        elif dataset == "naccounts" or dataset == "na":
            called_args = logutil.get_called_args(frequent.nat_accounts,
                                                  kwargs)
            output = frequent.nat_accounts(update=update_path,
                                           save=save_path,
                                           name=override,
                                           **kwargs)
        elif dataset == "labor" or dataset == "labour":
            called_args = logutil.get_called_args(frequent.labor_mkt,
                                                  kwargs)
            output = frequent.labor_mkt(update=update_path,
                                        save=save_path,
                                        name=override,
                                        **kwargs)
        else:
            raise ValueError("Invalid keyword for 'dataset' parameter.")

        self.dataset = output
        self.logger.info(f"Used the following keyword "
                         f"arguments: {called_args}")

        return self

    @logutil.log_transformer
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
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(loc_dir=self.loc_dir,
                           revise_rows=self.revise_rows,
                           force_update=self.force_update,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    @logutil.log_transformer
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
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(loc_dir=self.loc_dir,
                           revise_rows=self.revise_rows,
                           force_update=self.force_update,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    @logutil.log_transformer
    def decompose(self, flavor: str = "both",
                  trading: bool = True, outlier: bool = True,
                  x13_binary: Union[str, PathLike] = "search",
                  search_parents: int = 1):
        """
        Use `X-13 ARIMA <https://www.census.gov/srd/www/x13as/>`_ to
        decompose time series.

        Parameters
        ----------
        flavor : {'both', 'trend', 'seas'}
            Whether to get the trend component, the seasonally adjusted series
            or both.
        trading : bool, default True
            Whether to automatically detect trading days.
        outlier : bool, default True
            Whether to automatically detect outliers.
        x13_binary: str or os.PathLike, default 'search'
            Location of the X13 binary. If ``search`` is used, will attempt to
            find the binary in the project structure.
        search_parents: int, default 1
            If ``search`` is chosen for ``x13_binary``, this parameter controls
            how many parent directories to go up before recursively searching
            for the binary.

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
                full = transform.decompose(value,
                                           trading=trading,
                                           outlier=outlier,
                                           x13_binary=x13_binary,
                                           search_parents=search_parents)
                if flavor == "trend":
                    table = full[0]
                elif flavor == "seas" or type == "seasonal":
                    table = full[1]
                elif flavor == "both":
                    table = full
                else:
                    raise ValueError("'flavor' can be one of 'both', 'trend', "
                                     "or 'seas'.")

                output.update({key: table})
        else:
            full = transform.decompose(self.dataset,
                                       trading=trading,
                                       outlier=outlier,
                                       x13_binary=x13_binary,
                                       search_parents=search_parents)
            if flavor == "trend":
                output = full[0]
            elif flavor == "seas" or type == "seasonal":
                output = full[1]
            elif flavor == "both":
                output = full
            else:
                raise ValueError("'flavor' can be one of 'both', 'trend', or"
                                 "'seas'.")
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(loc_dir=self.loc_dir,
                           revise_rows=self.revise_rows,
                           force_update=self.force_update,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    @logutil.log_transformer
    def convert(self, flavor: str, update: Union[str, PathLike, None] = None,
                save: Union[str, PathLike, None] = None, **kwargs):
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
        if isinstance(self.dataset, dict):
            output = {}
            for key, value in self.dataset.items():
                if flavor == "usd":
                    table = transform.convert_usd(value, update=update,
                                                  save=save)
                elif flavor == "real":
                    table = transform.convert_real(value, update=update,
                                                   save=save, **kwargs)
                elif flavor == "pcgdp" or flavor == "gdp":
                    table = transform.convert_gdp(value, update=update,
                                                  save=save)
                else:
                    raise ValueError("'flavor' can be one of 'usd', 'real', "
                                     "or 'pcgdp'.")

                output.update({key: table})
        else:
            if flavor == "usd":
                output = transform.convert_usd(self.dataset, update=update,
                                               save=save)
            elif flavor == "real":
                output = transform.convert_real(self.dataset, update=update,
                                                save=save, **kwargs)
            elif flavor == "pcgdp" or flavor == "gdp":
                output = transform.convert_gdp(self.dataset, update=update,
                                               save=save)
            else:
                raise ValueError("'flavor' can be one of 'usd', 'real', "
                                 "or 'pcgdp'.")
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(loc_dir=self.loc_dir,
                           revise_rows=self.revise_rows,
                           force_update=self.force_update,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    @logutil.log_transformer
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
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(loc_dir=self.loc_dir,
                           revise_rows=self.revise_rows,
                           force_update=self.force_update,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    @logutil.log_transformer
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
        if self.inplace is True:
            self.dataset = output
            return self
        else:
            return Session(loc_dir=self.loc_dir,
                           revise_rows=self.revise_rows,
                           force_update=self.force_update,
                           dataset=output,
                           logger=self.logger,
                           inplace=self.inplace)

    def save(self, name: str):
        """Save :attr:`dataset` attribute to a CSV."""
        name = Path(name).with_suffix("").as_posix()

        if isinstance(self.dataset, dict):
            for key, value in self.dataset.items():
                save_path = (Path(self.loc_dir) /
                             f"{name}_{key}").with_suffix(".csv")
                if not path.exists(path.dirname(save_path)):
                    mkdir(path.dirname(save_path))
                value.to_csv(save_path)
        else:
            save_path = (Path(self.loc_dir) / name).with_suffix(".csv")
            if not path.exists(path.dirname(save_path)):
                mkdir(path.dirname(save_path))
            self.dataset.to_csv(save_path)

        self.logger.info(f"Saved dataset to directory {self.loc_dir}.")

    def final(self):
        """Return :attr:`dataset` attribute."""
        self.logger.info(f"Retrieved dataset from Session() object.")

        return self.dataset
