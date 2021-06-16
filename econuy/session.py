from __future__ import annotations
import logging
import copy
from datetime import datetime
from inspect import signature, getmodule
from os import PathLike, makedirs, path
from pathlib import Path
from typing import Callable, Optional, Union, Sequence, Dict, List

import pandas as pd
from sqlalchemy.engine.base import Connection, Engine

from econuy import transform
from econuy.retrieval.core import Retriever
from econuy.utils import datasets, logutil, ops
from econuy.utils.exceptions import RetryLimitError


class Session(object):
    """
    Main class to access download and transformation methods.

    Attributes
    ----------
    location : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
               default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating and saving, SQLAlchemy connection or engine object,
        or ``None``, don't save or update.
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
    errors : {'raise', 'coerce', 'ignore'}
        How to handle errors that arise from transformations. ``raise`` will
        raise a ValueError, ``coerce`` will force the data into ``np.nan`` and
        ``ignore`` will leave the input data as is.
    max_retries : int, default 3
        Number of retries for :mod:`~econuy.session.Session.get` and
        :mod:`~econuy.session.Session.get_custom` in case any of the selected
        datasets cannot be retrieved.

    """

    def __init__(self,
                 location: Union[str, PathLike,
                                 Connection, Engine, None] = None,
                 revise_rows: Union[int, str] = "nodup",
                 only_get: bool = False,
                 log: Union[int, str] = 1,
                 logger: Optional[logging.Logger] = None,
                 errors: str = "raise",
                 max_retries: int = 3):
        self.location = location
        self.revise_rows = revise_rows
        self.only_get = only_get
        self.log = log
        self.logger = logger
        self.errors = errors
        self.max_retries = max_retries

        self._datasets = {}
        self._retries = 1

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
                         f"Logging method: {log_method}\n"
                         f"Error handling: {errors}")

    @staticmethod
    def available_datasets(functions: bool = False) -> Dict[str, Dict]:
        """Return available ``dataset`` arguments for use in
        :mod:`~econuy.session.Session.get` and
        :mod:`~econuy.session.Session.get_custom`.

        Returns
        -------
        Dataset : Dict[str, str]
        """
        if functions:
            return {"original": {k: v
                                 for k, v in datasets.original().items()},
                    "custom": {k: v
                               for k, v in datasets.custom().items()}}
        else:
            return {"original": {k: v["description"]
                                 for k, v in datasets.original().items()},
                    "custom": {k: v["description"]
                               for k, v in datasets.custom().items()}}

    @property
    def datasets(self) -> Dict[str, pd.DataFrame]:
        """Holds retrieved datasets.

        Returns
        -------
        Datasets : Dict[str, pd.DataFrame]
        """
        return self._datasets

    def copy(self, deep: bool = True):
        """Copy or deepcopy a Session object.

        Parameters
        ----------
        deep : bool, default True
            If True, deepcopy.

        Returns
        -------
        :class:`~econuy.session.Session`

        """
        if deep:
            return copy.deepcopy(self)
        else:
            return copy.copy(self)

    @staticmethod
    def _download(original: bool, dataset: str, **kwargs) -> pd.DataFrame:
        """Helper method to handle parameters passed to retrieval functions.

        Parameters
        ----------
        original : bool
            Whether to retrieve original or custom datasets.
        dataset : str
            Name of dataset to retrieve.

        Returns
        -------
        Downloaded dataset : pd.DataFrame

        """
        if original is True:
            function = datasets.original[dataset]["function"]
        else:
            function = datasets.custom[dataset]["function"]
        accepted_params = dict(signature(function).parameters)
        new_kwargs = {k: v for k, v in kwargs.items()
                      if k in accepted_params.keys()}
        return function(**new_kwargs)

    def _select_datasets(self, select: Union[str, int, Sequence[str],
                                             Sequence[int]]) -> List[str]:
        keys = list(self.datasets.keys())
        if isinstance(select, Sequence) and not isinstance(select, str):
            if not all(type(select[i]) == type(select[0])
                       for i in range(len(select))):
                raise ValueError("`select` must be all `int` or all `str`")
            if isinstance(select[0], int):
                proc_select = [keys[i] for i in select]
            else:
                proc_select = [i for i in keys if i in select]
        elif isinstance(select, int):
            proc_select = [keys[select]]
        elif select == "all":
            proc_select = keys
        else:
            proc_select = [select]
        return proc_select

    def _apply_transformation(self, select: Union[str, int, Sequence[str],
                                                  Sequence[int]],
                              transformation: Callable,
                              **kwargs) -> Dict[str, pd.DataFrame]:
        """Helper method to apply transformations on :attr:`datasets`.

        Parameters
        ----------
        select : str, int, Sequence[str] or Sequence[int], default "all"
            Datasets in :attr:`datasets` to apply transformation on.
        transformation : Callable
            Function representing the transformation to apply.

        Returns
        -------
        Transformed datasets : Dict[str, pd.DataFrame]

        """
        proc_select = self._select_datasets(select=select)

        new_kwargs = {}
        for k, v in kwargs.items():
            if not isinstance(v, Sequence) or isinstance(v, str):
                new_kwargs.update({k: [v] * len(proc_select)})
            elif len(v) != len(proc_select):
                raise ValueError(f"Wrong number of arguments for '{k}'")
            else:
                new_kwargs.update({k: v})

        output = self.datasets.copy()
        for i, name in enumerate(proc_select):
            current_kwargs = {k: v[i] for k, v in new_kwargs.items()}
            data = self.datasets[name]
            if isinstance(data, Dict):
                transformed = {}
                for subname, subdata in data.items():
                    subtransformed = transformation(subdata,
                                                    **current_kwargs)
                    transformed.update({subname: subtransformed})
            else:
                transformed = transformation(data, **current_kwargs)
            output.update({name: transformed})

        return output

    def _parse_location(self, process: bool) -> Union[Path, str, None]:
        if process is True:
            if isinstance(self.location, (str, PathLike)):
                return Path(self.location)
            else:
                return self.location
        else:
            return None

    def get(self, dataset: Union[str, Sequence[str]], update: bool = True,
            save: bool = True, **kwargs) -> Session:
        """
        Main download method.

        Parameters
        ----------
        dataset : Union[str, Sequence[str]]
            Type of data to download, see available options in datasets.py
            or in :mod:`available_datasets`. Either a string representing
            a dataset name or a sequence of strings in order to download
            several datasets.
        update : bool, default True
            Whether to update an existing dataset.
        save : bool, default True
            Whether to save the dataset.
        **kwargs
            Keyword arguments.

        Returns
        -------
        :class:`~econuy.session.Session`
            Loads the downloaded dataframes into the :attr:`datasets`
            attribute.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``dataset`` argument.

        """
        if isinstance(dataset, str):
            dataset = [dataset]
        if any(name not in datasets.original.keys() for name in dataset):
            raise ValueError("Invalid dataset selected.")

        update_loc = self._parse_location(process=update)
        save_loc = self._parse_location(process=save)
        failed = []
        not_failed = []
        for name in dataset:
            try:
                retrieved = self._download(original=True, dataset=name,
                                           update_loc=update_loc,
                                           save_loc=save_loc,
                                           revise_rows=self.revise_rows,
                                           only_get=self.only_get, **kwargs)
                self._datasets.update({name: retrieved})
                not_failed.append(name)
            except:
                failed.append(name)
                continue
        if len(not_failed) > 0:
            self.logger.info(f"Retrieved {', '.join(not_failed)}")
        if len(failed) > 0:
            if self._retries < self.max_retries:
                self._retries += 1
                self.logger.info(f"Failed to retrieve {', '.join(failed)}. "
                                 f"Retrying (run {self._retries}).")
                self.get(dataset=failed, update=update, save=save, **kwargs)
            else:
                self.logger.info(f"Could not retrieve {', '.join(failed)}")
                self._retries = 1
                raise RetryLimitError(f"Maximum retries ({self.max_retries})"
                                      f" reached.")
            self._retries = 1
            return
        self._retries = 1
        return

    def get_custom(self, dataset: Union[str, Sequence[str]],
                   update: bool = True, save: bool = True,
                   **kwargs) -> Session:
        """
        Get custom datasets.

        Parameters
        ----------
        dataset : Union[str, Sequence[str]]
            Type of data to download, see available options in datasets.py
            or in :mod:`available_datasets`. Either a string representing a
            dataset name or a sequence of strings in order to download several
            datasets.
        update : bool, default True
            Whether to update an existing dataset.
        save : bool, default  True
            Whether to save the dataset.
        **kwargs
            Keyword arguments.

        Returns
        -------
        :class:`~econuy.session.Session`
            Loads the downloaded dataframes into the :attr:`datasets`
            attribute.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``dataset`` argument.

        """
        if isinstance(dataset, str):
            dataset = [dataset]
        if any(name not in datasets.custom.keys() for name in dataset):
            raise ValueError("Invalid dataset selected.")

        update_loc = self._parse_location(process=update)
        save_loc = self._parse_location(process=save)
        failed = []
        not_failed = []
        for name in dataset:
            try:
                retrieved = self._download(original=False, dataset=name,
                                           update_loc=update_loc,
                                           save_loc=save_loc,
                                           revise_rows=self.revise_rows,
                                           only_get=self.only_get, **kwargs)
                self._datasets.update({name: retrieved})
                not_failed.append(name)
            except:
                failed.append(name)
                continue
        if len(not_failed) > 0:
            self.logger.info(f"Retrieved {', '.join(not_failed)}")
        if len(failed) > 0:
            if self._retries < self.max_retries:
                self._retries += 1
                self.logger.info(f"Failed to retrieve {', '.join(failed)}. "
                                 f"Retrying (run {self._retries}).")
                self.get_custom(dataset=failed, update=update,
                                save=save, **kwargs)
            else:
                self.logger.info(f"Could not retrieve {', '.join(failed)}")
                self._retries = 1
                raise RetryLimitError(f"Maximum retries ({self.max_retries})"
                                      f" reached.")
            self._retries = 1
            return
        self._retries = 1
        return

    def get_bulk(self, dataset: str, update: bool = True,
                 save: bool = True, **kwargs) -> Session:
        """
        Get datasets in bulk.

        Parameters
        ----------
        dataset : {'all', 'original', 'custom', 'economic_activity', \
                 'prices', 'fiscal_accounts', 'labor', 'external_sector', \
                 'financial_sector', 'income', 'international', 'regional'}
            Type of data to download. `all` gets all available datasets,
            `original` gets all original datatsets and `custom` gets all
            custom datasets. The remaining options get all datasets for that
            area.
        update : bool, default True
            Whether to update an existing dataset.
        save : bool, default  True
            Whether to save the dataset.
        **kwargs
            Keyword arguments.

        Returns
        -------
        :class:`~econuy.session.Session`
            Loads the downloaded dataframes into the :attr:`datasets`
            attribute.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``dataset`` argument.

        """
        valid_datasets = ["all", "original", "custom", "economic_activity",
                          "prices", "fiscal_accounts", "labor", "external_sector",
                          "financial_sector", "income", "international",
                          "regional"]
        if dataset not in valid_datasets:
            raise ValueError(f"'dataset' can only be one of "
                             f"{', '.join(valid_datasets)}.")
        available_datasets = self.available_datasets(functions=True)
        original_datasets = list(available_datasets["original"].keys())
        custom_datasets = list(available_datasets["custom"].keys())
        new_session = self.copy(deep=True)

        if dataset == "original":
            new_session.get(dataset=original_datasets, update=update,
                            save=save, **kwargs)
        elif dataset == "custom":
            new_session.get_custom(dataset=custom_datasets, update=update,
                                   save=save, **kwargs)
        elif dataset == "all":
            new_session.get(dataset=original_datasets, update=update,
                            save=save, **kwargs)
            new_session.get_custom(dataset=custom_datasets, update=update,
                                   save=save, **kwargs)
        else:
            original_area_datasets = []
            for k, v in available_datasets["original"].items():
                if dataset in getmodule(v["function"]).__name__:
                    original_area_datasets.append(k)
            custom_area_datasets = []
            for k, v in available_datasets["custom"].items():
                if dataset in getmodule(v["function"]).__name__:
                    custom_area_datasets.append(k)
            if len(original_area_datasets) > 0:
                new_session.get(dataset=original_area_datasets,
                                update=update, save=save, **kwargs)
            if len(custom_area_datasets) > 0:
                new_session.get_custom(dataset=custom_area_datasets,
                                       update=update, save=save, **kwargs)

        self._datasets.update(new_session.datasets)
        return

    def resample(self, rule: Union[pd.DateOffset, pd.Timedelta, str, List],
                 operation: Union[str, List] = "sum",
                 interpolation: Union[str, List] = "linear",
                 select: Union[str, int, Sequence[str],
                               Sequence[int]] = "all") -> Session:
        """
        Resample to target frequencies.

        See Also
        --------
        :func:`~econuy.transform.resample`

        """
        output = self._apply_transformation(select=select,
                                            transformation=transform.resample,
                                            rule=rule,
                                            operation=operation,
                                            interpolation=interpolation)
        self.logger.info(f"Applied 'resample' transformation with '{rule}' "
                         f"and '{operation}' operation.")
        self._datasets = output
        return

    def chg_diff(self, operation: Union[str, List] = "chg",
                 period: Union[str, List] = "last",
                 select: Union[str, int, Sequence[str],
                               Sequence[int]] = "all") -> Session:
        """
        Calculate pct change or difference.

        See Also
        --------
        :func:`~econuy.transform.chg_diff`

        """
        output = self._apply_transformation(select=select,
                                            transformation=transform.chg_diff,
                                            operation=operation, period=period)
        self.logger.info(f"Applied 'chg_diff' transformation with "
                         f"'{operation}' operation and '{period}' period.")
        self._datasets = output
        return

    def decompose(self, component: Union[str, List] = "both",
                  method: Union[str, List] = "x13",
                  force_x13: Union[bool, List] = False,
                  fallback: Union[str, List] = "loess",
                  trading: Union[bool, List] = True,
                  outlier: Union[bool, List] = True,
                  x13_binary: Union[str, PathLike, List] = "search",
                  search_parents: Union[int, List] = 1,
                  ignore_warnings: Union[bool, List] = True,
                  errors: Union[str, List, None] = None,
                  select: Union[str, int, Sequence[str],
                                Sequence[int]] = "all",
                  **kwargs) -> Session:
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

        output = self._apply_transformation(select=select,
                                            transformation=transform.decompose,
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
        self._datasets = output
        return

    def convert(self, flavor: Union[str, List],
                update: Union[bool, List] = True,
                save: Union[bool, List] = True,
                only_get: Union[bool, List] = True,
                errors: Union[str, None, List] = None,
                start_date: Union[str, datetime, None, List] = None,
                end_date: Union[str, datetime, None, List] = None,
                select: Union[str, int, Sequence[str],
                              Sequence[int]] = "all") -> Session:
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
            output = self._apply_transformation(select=select,
                                                transformation=transform.convert_usd,
                                                update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=only_get,
                                                errors=errors)
        elif flavor == "real":
            output = self._apply_transformation(select=select,
                                                transformation=transform.convert_real,
                                                update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=only_get,
                                                errors=errors,
                                                start_date=start_date,
                                                end_date=end_date)
        else:
            output = self._apply_transformation(select=select,
                                                transformation=transform.convert_gdp,
                                                update_loc=update_loc,
                                                save_loc=save_loc,
                                                only_get=only_get,
                                                errors=errors)

        self.logger.info(f"Applied 'convert' transformation "
                         f"with '{flavor}' flavor.")
        self._datasets = output
        return

    def rebase(self, start_date: Union[str, datetime, List],
               end_date: Union[str, datetime, None, List] = None,
               base: Union[float, List] = 100.0,
               select: Union[str, int, Sequence[str],
                             Sequence[int]] = "all") -> Session:
        """
        Scale to a period or range of periods.

        See Also
        --------
        :func:`~econuy.transform.rebase`

        """
        output = self._apply_transformation(select=select,
                                            transformation=transform.rebase,
                                            start_date=start_date,
                                            end_date=end_date, base=base)
        self.logger.info("Applied 'rebase' transformation.")
        self._datasets = output
        return

    def rolling(self, window: Union[int, List, None] = None,
                operation: Union[str, List] = "sum",
                select: Union[str, int, Sequence[str],
                              Sequence[int]] = "all") -> Session:
        """
        Calculate rolling averages or sums.

        See Also
        --------
        :func:`~econuy.transform.rolling`

        """
        output = self._apply_transformation(select=select,
                                            transformation=transform.rolling,
                                            window=window,
                                            operation=operation)
        self.logger.info(f"Applied 'rolling' transformation with "
                         f"{window} periods and '{operation}' operation.")
        self._datasets = output
        return

    def concat(self, select: Union[str, int, Sequence[str],
                                   Sequence[int]] = "all",
               name: Optional[str] = None,
               force_suffix: bool = False) -> Session:
        """
        Concatenate datasets in :attr:`datasets` and add as a new dataset.

        Resample to lowest frequency of selected datasets.

        Parameters
        ----------
        select : str, int, Sequence[str] or Sequence[int], default "all"
            Datasets to concatenate.
        name : Optional[str], default None
            Name used as a key for the output dataset. The default None sets
            the name to "com_{dataset_1_name}_..._{dataset_n_name}".
        force_suffix : bool, default False
            Whether to include each dataset's full name as a prefix in all
            indicator columns.

        Returns
        -------
        :class:`~econuy.session.Session`
            Loads the concatenated dataframes into the :attr:`datasets`
            attribute.

        """
        proc_select = self._select_datasets(select=select)
        proc_select = [x for x in proc_select if "com_" not in x]
        selected_datasets = [d for n, d in self.datasets.items()
                             if n in proc_select]

        indicator_names = [col for df in selected_datasets
                           for col in df.columns.get_level_values(0)]
        if (len(indicator_names) > len(set(indicator_names))
                or force_suffix is True):
            for n, d in zip(proc_select, selected_datasets):
                try:
                    full_name = datasets.original[n]["description"]
                except KeyError:
                    full_name = datasets.custom[n]["description"]
                d.columns = d.columns.set_levels(
                    f"{full_name}_" + d.columns.get_level_values(0),
                    level=0
                )

        freqs = [pd.infer_freq(df.index) for df in selected_datasets]
        if all(freq == freqs[0] for freq in freqs):
            combined = pd.concat(selected_datasets, axis=1)
        else:
            for freq_opt in ["A-DEC", "A", "Q-DEC", "Q",
                             "M", "2W-SUN", "W-SUN"]:
                if freq_opt in freqs:
                    output = []
                    for df in selected_datasets:
                        freq_df = pd.infer_freq(df.index)
                        if freq_df == freq_opt:
                            df_match = df.copy()
                        else:
                            type_df = df.columns.get_level_values("Tipo")[0]
                            unit_df = df.columns.get_level_values("Unidad")[0]
                            if type_df == "Stock":
                                df_match = transform.resample(df, rule=freq_opt,
                                                              operation="last")
                            elif (type_df == "Flujo" and
                                  not any(x in unit_df for
                                          x in ["%", "=", "Cambio"])):
                                df_match = transform.resample(df, rule=freq_opt,
                                                              operation="sum")
                            else:
                                df_match = transform.resample(df, rule=freq_opt,
                                                              operation="mean")
                        output.append(df_match)
                    combined = pd.concat(output, axis=1)
                    break
                else:
                    continue
        if name is None:
            name = "com_" + "_".join(proc_select)
        self._datasets.update({name: combined})
        return

    def save(self,
             select: Union[str, int, Sequence[str], Sequence[int]] = "all",
             file_fmt: str = "csv", multiindex: Optional[str] = "included"):
        """Save :attr:`datasets` attribute to CSV or SQL.

        Parameters
        ----------
        select : str, int, Sequence[str] or Sequence[int], default "all"
            Datasets to save.
        file_fmt : {'csv', 'xlsx'}
            File format. Ignored if ``self.location`` refers to a SQL object.
        multiindex : {'included', 'separate', None}
            How to handle multiindexes for metadata. ``None`` keeps only the
            first level, ``included`` keeps as DataFrame columns and
            ``separate`` saves it to another sheet (only valid for Excel-type
            formats).

        Raises
        ------
        ValueError
            If `self.location` is None.
        """
        if self.location is None:
            raise ValueError("No save location defined.")

        proc_select = self._select_datasets(select=select)

        for name, dataset in self.datasets.items():
            if name in proc_select:
                if isinstance(dataset, dict):
                    for subname, subdataset in dataset.items():
                        ops._io(operation="save", data_loc=self.location,
                                data=subdataset, name=f"{name}_{subname}",
                                file_fmt=file_fmt, multiindex=multiindex)
                else:
                    ops._io(operation="save", data_loc=self.location,
                            data=dataset, name=name, file_fmt=file_fmt,
                            multiindex=multiindex)
            else:
                continue

        self.logger.info(f"Saved {', '.join(proc_select)} "
                         f"to '{self.location}'.")

        return
