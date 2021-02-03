import logging
from datetime import datetime
from inspect import signature
from os import PathLike, makedirs, path
from pathlib import Path
from typing import Callable, Optional, Union

import pandas as pd
from sqlalchemy.engine.base import Connection, Engine

from econuy import transform
from econuy.utils import datasets, logutil, ops


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
    errors : {'raise', 'coerce', 'ignore'}
        How to handle errors that arise from transformations. ``raise`` will
        raise a ValueError, ``coerce`` will force the data into ``np.nan`` and
        ``ignore`` will leave the input data as is.

    """

    def __init__(self,
                 location: Union[str, PathLike,
                                 Connection, Engine, None] = "econuy-data",
                 revise_rows: Union[int, str] = "nodup",
                 only_get: bool = False,
                 dataset: Union[dict, pd.DataFrame] = pd.DataFrame(),
                 log: Union[int, str] = 1,
                 logger: Optional[logging.Logger] = None,
                 inplace: bool = False,
                 errors: str = "raise"):
        self.location = location
        self.revise_rows = revise_rows
        self.only_get = only_get
        self.log = log
        self.logger = logger
        self.inplace = inplace
        self.errors = errors
        self._dataset = dataset
        self._dataset_name = None

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

            if isinstance(revise_rows, int):
                revise_method = f"{revise_rows} rows to replace"
            else:
                revise_method = revise_rows
            log_obj.info(f"Created Session object with the "
                         f"following attributes:\n"
                         f"Location for downloads and updates: {loc_text}\n"
                         f"Offline: {only_get}\n"
                         f"Update method: '{revise_method}'\n"
                         f"Dataset: {self._dataset_name.__str__()}\n"
                         f"Logging method: {log_method}\n"
                         f"Inplace: {inplace}\n"
                         f"Error handling: {errors}")

    @property
    def available(self):
        return {"original": {k: v["description"]
                             for k, v in datasets.original.items()},
                "custom": {k: v["description"]
                           for k, v in datasets.custom.items()}}

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def dataset(self):
        return self._dataset

    @dataset.setter
    def dataset(self, value):
        self._dataset = value
        self._dataset_name = "Custom"

    @staticmethod
    def _download(original: bool, dataset: str, **kwargs):
        if original is True:
            function = datasets.original[dataset]["function"]
        else:
            function = datasets.custom[dataset]["function"]
        accepted_params = dict(signature(function).parameters)
        new_kwargs = {k: v for k, v in kwargs.items()
                      if k in accepted_params.keys()}
        return function(**new_kwargs)

    def _apply_transformation(self, transformation: Callable, **kwargs):
        if isinstance(self.dataset, dict):
            output = {}
            for name, data in self.dataset.items():
                transformed = transformation(data, **kwargs)
                output.update({name: transformed})
            return output
        else:
            return transformation(self.dataset, **kwargs)

    def _parse_location(self, process: bool):
        if process is True:
            if isinstance(self.location, (str, PathLike)):
                return Path(self.location)
            else:
                return self.location
        else:
            return None

    def get(self,
            dataset: str,
            update: bool = True,
            save: bool = True,
            **kwargs):
        """
        Main download method.

        Parameters
        ----------
        dataset : str, see available options in datasets.py or in 
                  :attr:`available`
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
        if dataset not in datasets.original.keys():
            raise ValueError("Invalid dataset selected.")

        update_loc = self._parse_location(process=update)
        save_loc = self._parse_location(process=save)
        output = self._download(original=True, dataset=dataset,
                                update_loc=update_loc, save_loc=save_loc,
                                revise_rows=self.revise_rows,
                                only_get=self.only_get, **kwargs)
        self.logger.info(f"Retrieved '{dataset}' dataset.")
        if self.inplace is True:
            self.dataset = output
            self._dataset_name = dataset
            return self
        else:
            new_session = Session(location=self.location,
                                  revise_rows=self.revise_rows,
                                  only_get=self.only_get,
                                  dataset=output,
                                  logger=self.logger,
                                  inplace=self.inplace,
                                  errors=self.errors)
            new_session._dataset_name = dataset
            return new_session

    def get_custom(self,
                   dataset: str,
                   update: bool = True,
                   save: bool = True,
                   **kwargs):
        """
        Get custom datasets.

        Parameters
        ----------
        dataset : str, see available options in datasets.py or in 
                  :attr:`available`
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
        update_loc = self._parse_location(process=update)
        save_loc = self._parse_location(process=save)

        if dataset not in datasets.custom.keys():
            raise ValueError("Invalid dataset selected.")

        update_loc = self._parse_location(process=update)
        save_loc = self._parse_location(process=save)
        output = self._download(original=False, dataset=dataset,
                                update_loc=update_loc, save_loc=save_loc,
                                revise_rows=self.revise_rows,
                                only_get=self.only_get, **kwargs)
        self.logger.info(f"Retrieved '{dataset}' dataset.")
        if self.inplace is True:
            self.dataset = output
            self._dataset_name = dataset
            return self
        else:
            new_session = Session(location=self.location,
                                  revise_rows=self.revise_rows,
                                  only_get=self.only_get,
                                  dataset=output,
                                  logger=self.logger,
                                  inplace=self.inplace,
                                  errors=self.errors)
            new_session._dataset_name = dataset
            return new_session

    def resample(self, rule: Union[pd.DateOffset, pd.Timedelta, str],
                 operation: str = "sum",
                 interpolation: str = "linear"):
        """
        Resample to target frequencies.

        See Also
        --------
        :func:`~econuy.transform.resample`

        """
        output = self._apply_transformation(transform.resample, rule=rule,
                                            operation=operation,
                                            interpolation=interpolation)
        self.logger.info(f"Applied 'resample' transformation with '{rule}' "
                         f"and '{operation}' operation.")
        old_name = self.dataset_name
        if self.inplace is True:
            self.dataset = output
            self._dataset_name = old_name
            return self
        else:
            new_session = Session(location=self.location,
                                  revise_rows=self.revise_rows,
                                  only_get=self.only_get,
                                  dataset=output,
                                  logger=self.logger,
                                  inplace=self.inplace,
                                  errors=self.errors)
            new_session._dataset_name = old_name
            return new_session

    def chg_diff(self, operation: str = "chg", period: str = "last"):
        """
        Calculate pct change or difference.

        See Also
        --------
        :func:`~econuy.transform.chg_diff`

        """
        output = self._apply_transformation(transform.chg_diff,
                                            operation=operation, period=period)
        self.logger.info(f"Applied 'chg_diff' transformation with "
                         f"'{operation}' operation and '{period}' period.")
        old_name = self.dataset_name
        if self.inplace is True:
            self.dataset = output
            self._dataset_name = old_name
            return self
        else:
            new_session = Session(location=self.location,
                                  revise_rows=self.revise_rows,
                                  only_get=self.only_get,
                                  dataset=output,
                                  logger=self.logger,
                                  inplace=self.inplace,
                                  errors=self.errors)
            new_session._dataset_name = old_name
            return new_session

    def decompose(self, component: str = "both", method: str = "x13",
                  force_x13: bool = False, fallback: str = "loess",
                  trading: bool = True, outlier: bool = True,
                  x13_binary: Union[str, PathLike] = "search",
                  search_parents: int = 1, ignore_warnings: bool = True,
                  errors: str = None, **kwargs):
        """
        Apply seasonal decomposition.

        Raises
        ------
        ValueError
            If the ``method`` parameter does not have a valid argument.
        ValueError
            If the ``fallback`` parameter does not have a valid argument.
        ValueError
            If the path provided for the X13 binary does not point to a file
            and ``method='x13'``.

        See Also
        --------
        :func:`~econuy.transform.decompose`

        """
        if errors is None:
            errors = self.errors

        output = self._apply_transformation(transform.decompose,
                                            component=component,
                                            method=method,
                                            force_x13=force_x13,
                                            fallback=fallback,
                                            trading=trading,
                                            outlier=outlier,
                                            x13_binary=x13_binary,
                                            search_parents=search_parents,
                                            ignore_warnings=ignore_warnings,
                                            errors=errors,
                                            **kwargs)
        self.logger.info(f"Applied 'decompose' transformation with "
                         f"'{method}' method and '{component}' component.")
        old_name = self.dataset_name
        if self.inplace is True:
            self.dataset = output
            self._dataset_name = old_name
            return self
        else:
            new_session = Session(location=self.location,
                                  revise_rows=self.revise_rows,
                                  only_get=self.only_get,
                                  dataset=output,
                                  logger=self.logger,
                                  inplace=self.inplace,
                                  errors=self.errors)
            new_session._dataset_name = old_name
            return new_session

    def convert(self, flavor: str, update: bool = True,
                save: bool = True, only_get: bool = True,
                errors: str = None,
                start_date: Union[str, datetime, None] = None,
                end_date: Union[str, datetime, None] = None):
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
        if flavor not in ["usd", "real", "gdp", "pcgdp"]:
            raise ValueError("'flavor' can be one of 'usd', 'real', "
                             "or 'gdp'.")

        if errors is None:
            errors = self.errors

        update_loc = self._parse_location(process=update)
        save_loc = self._parse_location(process=save)

        if flavor == "usd":
            output = self._apply_transformation(transform.convert_usd,
                                                update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=only_get,
                                                errors=errors)
        elif flavor == "real":
            output = self._apply_transformation(transform.convert_real,
                                                update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=only_get,
                                                errors=errors,
                                                start_date=start_date,
                                                end_date=end_date)
        else:
            output = self._apply_transformation(transform.convert_gdp,
                                                update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=only_get,
                                                errors=errors)

        self.logger.info(f"Applied 'convert' transformation "
                         f"with '{flavor}' flavor.")
        old_name = self.dataset_name
        if self.inplace is True:
            self.dataset = output
            self._dataset_name = old_name
            return self
        else:
            new_session = Session(location=self.location,
                                  revise_rows=self.revise_rows,
                                  only_get=self.only_get,
                                  dataset=output,
                                  logger=self.logger,
                                  inplace=self.inplace,
                                  errors=self.errors)
            new_session._dataset_name = old_name
            return new_session

    def rebase(self, start_date: Union[str, datetime],
               end_date: Union[str, datetime, None] = None,
               base: float = 100.0):
        """
        Scale to a period or range of periods.

        See Also
        --------
        :func:`~econuy.transform.rebase`

        """
        output = self._apply_transformation(transform.rebase,
                                            start_date=start_date,
                                            end_date=end_date, base=base)
        self.logger.info("Applied 'rebase' transformation.")
        old_name = self.dataset_name
        if self.inplace is True:
            self.dataset = output
            self._dataset_name = old_name
            return self
        else:
            new_session = Session(location=self.location,
                                  revise_rows=self.revise_rows,
                                  only_get=self.only_get,
                                  dataset=output,
                                  logger=self.logger,
                                  inplace=self.inplace,
                                  errors=self.errors)
            new_session._dataset_name = old_name
            return new_session

    def rolling(self, window: Optional[int] = None,
                operation: str = "sum"):
        """
        Calculate rolling averages or sums.

        See Also
        --------
        :func:`~econuy.transform.rolling`

        """
        output = self._apply_transformation(transform.rolling,
                                            window=window,
                                            operation=operation)
        self.logger.info(f"Applied 'rolling' transformation with "
                         f"{window} periods and '{operation}' operation.")
        old_name = self.dataset_name
        if self.inplace is True:
            self.dataset = output
            self._dataset_name = old_name
            return self
        else:
            new_session = Session(location=self.location,
                                  revise_rows=self.revise_rows,
                                  only_get=self.only_get,
                                  dataset=output,
                                  logger=self.logger,
                                  inplace=self.inplace,
                                  errors=self.errors)
            new_session._dataset_name = old_name
            return new_session

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
