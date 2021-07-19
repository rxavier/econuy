from __future__ import annotations
import logging
import copy
import traceback
from datetime import datetime
from inspect import getmodule
from os import PathLike
from pathlib import Path
from typing import Optional, Union, Sequence, Dict, List

import pandas as pd
from sqlalchemy.engine.base import Connection, Engine

from econuy import transform
from econuy.core import Pipeline
from econuy.utils import datasets, logutil, ops
from econuy.utils.exceptions import RetryLimitError


class Session(object):
    """
    A download and transformation session that creates a Pipeline object and
    simplifies working with multiple datasets.

    Alternatively, can be created directly from a Pipeline by using the
    :mod:`~econuy.session.Session.from_pipeline` class method.

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
    log : {str, 0, 1, 2}
        Controls how logging works. ``0``: don't log; ``1``: log to console;
        ``2``: log to console and file with default file; ``str``: log to
        console and file with filename=str
    logger : logging.Logger, default None
        Logger object. For most cases this attribute should be ``None``,
        allowing :attr:`log` to control how logging works.
    max_retries : int, default 3
        Number of retries for :mod:`get` in case any of the selected
        datasets cannot be retrieved.

    """

    def __init__(
        self,
        location: Union[str, PathLike, Connection, Engine, None] = None,
        download: bool = True,
        always_save: bool = True,
        read_fmt: str = "csv",
        read_header: Optional[str] = "included",
        save_fmt: str = "csv",
        save_header: Optional[str] = "included",
        errors: str = "raise",
        log: Union[int, str] = 1,
        logger: Optional[logging.Logger] = None,
        max_retries: int = 3,
    ):
        self.location = location
        self.download = download
        self.always_save = always_save
        self.read_fmt = read_fmt
        self.read_header = read_header
        self.save_fmt = save_fmt
        self.save_header = save_header
        self.errors = errors
        self.log = log
        self.logger = logger
        self.max_retries = max_retries

        self._datasets = {}
        self._retries = 1

        if logger is not None:
            self.log = "custom"
        else:
            if isinstance(log, int) and (log < 0 or log > 2):
                raise ValueError(
                    "'log' takes either 0 (don't log info),"
                    " 1 (log to console), 2 (log to console and"
                    " default file), or str (log to console and "
                    "file with filename=str)."
                )
            elif log == 2:
                logfile = Path(self.location) / "info.log"
                log_obj = logutil.setup(file=logfile)
            elif isinstance(log, str) and log != "custom":
                logfile = (Path(self.location) / log).with_suffix(".log")
                log_obj = logutil.setup(file=logfile)
            elif log == 1:
                log_obj = logutil.setup(file=None)
            else:
                log_obj = logutil.setup(null=True)
            self.logger = log_obj

    @classmethod
    def from_pipeline(cls, pipeline: Pipeline) -> Session:
        # Alternative constructor
        s = Session(
            location=pipeline.location,
            download=pipeline.download,
            always_save=pipeline.location,
            read_fmt=pipeline.read_fmt,
            read_header=pipeline.read_header,
            save_fmt=pipeline.read_fmt,
            save_header=pipeline.save_header,
            errors=pipeline.errors,
        )
        if not pipeline.dataset.empty:
            s._datasets = {pipeline.name: pipeline.dataset}
        return s

    @property
    def pipeline(self) -> Pipeline:
        # Define a property so that changes to Session attributes are passed
        # down to the Pipeline and taken into account in get().
        p = Pipeline(
            location=self.location,
            download=self.download,
            always_save=self.always_save,
            read_fmt=self.read_fmt,
            read_header=self.read_header,
            save_fmt=self.save_fmt,
            save_header=self.save_header,
            errors=self.errors,
        )
        return p

    @property
    def datasets(self) -> Dict[str, pd.DataFrame]:
        """Holds retrieved datasets.

        Returns
        -------
        Datasets : Dict[str, pd.DataFrame]
        """
        return self._datasets

    @property
    def datasets_flat(self) -> Dict[str, pd.DataFrame]:
        """Holds retrieved datasets.

        Returns
        -------
        Datasets : Dict[str, pd.DataFrame]
        """
        nometa = copy.deepcopy(self._datasets)
        for v in nometa.values():
            v.columns = v.columns.get_level_values(0)
        return nometa

    def __repr__(self):
        return (
            f"Session(location={self.location})\n"
            f"Current dataset(s): {list(self.datasets.keys())}"
        )

    def copy(self, deep: bool = False) -> Session:
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
    def available_datasets(functions: bool = False) -> Dict[str, Dict]:
        """Return available ``dataset`` arguments for use in
        :mod:`~econuy.session.Session.get`.

        Returns
        -------
        Dataset : Dict[str, Dict]
        """
        original = datasets.original()
        custom = datasets.custom()

        original_final, custom_final = {}, {}
        for d, f in zip([original, custom], [original_final, custom_final]):
            for k, v in d.items():
                if not functions:
                    f.update({k: v["description"]})
                else:
                    f.update({k: v})

        output = {"original": original_final, "custom": custom_final}
        aux = copy.deepcopy(output)
        for k in aux["custom"].keys():
            # Avoid auxiliary datasets in Session methods (like _lin_gdp)
            if k.startswith("_"):
                output["custom"].pop(k, None)
        return output

    def _select_datasets(self, select: Union[str, int, Sequence[str], Sequence[int]]) -> List[str]:
        """Generate list of dataset names based on selection.

        Parameters
        ----------
        select : str, int, Sequence[str] or Sequence[int], default "all"
            Datasets in :attr:`datasets` to apply transformation on. Can be
            defined with their names or index position.

        Returns
        -------
        List[str]
            List of dataset names.

        Raises
        ------
        ValueError
            If names and indexes are combined.
        """
        keys = list(self.datasets.keys())
        if isinstance(select, Sequence) and not isinstance(select, str):
            if not all(isinstance(select[i], type(select[0])) for i in range(len(select))):
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

    def _apply_transformation(
        self,
        transformation: str,
        select: Union[str, int, Sequence[str], Sequence[int]] = "all",
        **kwargs,
    ) -> Dict[str, pd.DataFrame]:
        """Helper method to apply transformations on :attr:`datasets`.

        Parameters
        ----------
        transformation : {'resample', 'chg_diff', 'convert', 'decompose', \
                          'rolling', 'rebase'}
            String representing transformation methods in
            :class:`~econuy.retrieval.core.Pipeline`.
        select : str, int, Sequence[str] or Sequence[int], default "all"
            Datasets in :attr:`datasets` to apply transformations on.

        Returns
        -------
        Transformed datasets : Dict[str, pd.DataFrame]

        """
        p = self.pipeline.copy()
        methods = {
            "resample": p.resample,
            "chg_diff": p.chg_diff,
            "convert": p.convert,
            "decompose": p.decompose,
            "rolling": p.rolling,
            "rebase": p.rebase,
        }

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
            p._dataset = self.datasets[name]
            methods[transformation](**current_kwargs)
            transformed = p.dataset
            output.update({name: transformed})

        return output

    def get(self, names: Union[str, Sequence[str]]):
        """
        Main download method.

        Parameters
        ----------
        names : Union[str, Sequence[str]]
            Dataset to download, see available options in
            :mod:`~econuy.session.Session.available_datasets`. Either a string representing
            a dataset name or a sequence of strings in order to download
            several datasets.

        Raises
        ------
        ValueError
            If an invalid string is found in the ``names`` argument.

        """
        if isinstance(names, str):
            names = [names]
        if any(x not in self.pipeline.available_datasets().keys() for x in names):
            raise ValueError("Invalid dataset selected.")

        # Deepcopy the Pipeline so that its dataset attribute is not
        # overwritten each time it's accessed within this method.
        p = self.pipeline.copy()

        failed = []
        not_failed = []
        for name in names:
            try:
                p.get(name=name)
                self._datasets.update({name: p.dataset})
                not_failed.append(name)
            except BaseException:
                traceback.print_exc()
                failed.append(name)
                continue
        if len(failed) > 0:
            if self._retries < self.max_retries:
                self._retries += 1
                self.logger.info(
                    f"Failed to retrieve {', '.join(failed)}. " f"Retrying (run {self._retries})."
                )
                self.get(names=failed)
            else:
                self.logger.info(f"Could not retrieve {', '.join(failed)}")
                self._retries = 1
                raise RetryLimitError(f"Maximum retries ({self.max_retries})" f" reached.")
            self._retries = 1
            return
        self._retries = 1
        return

    def get_bulk(self, names: str):
        """
        Get datasets in bulk.

        Parameters
        ----------
        names : {'all', 'original', 'custom', 'economic_activity', \
                 'prices', 'fiscal_accounts', 'labor', 'external_sector', \
                 'financial_sector', 'income', 'international', 'regional'}
            Type of data to download. `all` gets all available datasets,
            `original` gets all original datatsets and `custom` gets all
            custom datasets. The remaining options get all datasets for that
            area.

        Raises
        ------
        ValueError
            If an invalid string is given to the ``names`` argument.

        """
        valid_datasets = [
            "all",
            "original",
            "custom",
            "economic_activity",
            "prices",
            "fiscal_accounts",
            "labor",
            "external_sector",
            "financial_sector",
            "income",
            "international",
            "regional",
        ]
        if names not in valid_datasets:
            raise ValueError(f"'names' can only be one of " f"{', '.join(valid_datasets)}.")
        available_datasets = self.available_datasets(functions=True)
        original_datasets = list(available_datasets["original"].keys())
        custom_datasets = list(available_datasets["custom"].keys())

        if names == "original":
            self.get(names=original_datasets)
        elif names == "custom":
            self.get(names=custom_datasets)
        elif names == "all":
            self.get(names=original_datasets + custom_datasets)
        else:
            original_area_datasets = []
            for k, v in available_datasets["original"].items():
                if names in getmodule(v["function"]).__name__:
                    original_area_datasets.append(k)
            custom_area_datasets = []
            for k, v in available_datasets["custom"].items():
                if names in getmodule(v["function"]).__name__:
                    custom_area_datasets.append(k)
            self.get(names=original_area_datasets + custom_area_datasets)

        return

    def resample(
        self,
        rule: Union[pd.DateOffset, pd.Timedelta, str, List],
        operation: Union[str, List] = "sum",
        interpolation: Union[str, List] = "linear",
        warn: Union[bool, List] = False,
        select: Union[str, int, Sequence[str], Sequence[int]] = "all",
    ):
        """
        Resample to target frequencies.

        See Also
        --------
        :mod:`~econuy.core.Pipeline.resample`

        """
        output = self._apply_transformation(
            select=select,
            transformation="resample",
            rule=rule,
            operation=operation,
            interpolation=interpolation,
            warn=warn,
        )

        self._datasets = output
        return

    def chg_diff(
        self,
        operation: Union[str, List] = "chg",
        period: Union[str, List] = "last",
        select: Union[str, int, Sequence[str], Sequence[int]] = "all",
    ):
        """
        Calculate pct change or difference.

        See Also
        --------
        :mod:`~econuy.core.Pipeline.chg_diff`.

        """
        output = self._apply_transformation(
            select=select, transformation="chg_diff", operation=operation, period=period
        )

        self._datasets = output
        return

    def decompose(
        self,
        component: Union[str, List] = "seas",
        method: Union[str, List] = "x13",
        force_x13: Union[bool, List] = False,
        fallback: Union[str, List] = "loess",
        trading: Union[bool, List] = True,
        outlier: Union[bool, List] = True,
        x13_binary: Union[str, PathLike, List] = "search",
        search_parents: Union[int, List] = 0,
        ignore_warnings: Union[bool, List] = True,
        select: Union[str, int, Sequence[str], Sequence[int]] = "all",
        **kwargs,
    ):
        """
        Apply seasonal decomposition.

        See Also
        --------
        :mod:`~econuy.core.Pipeline.decompose`.

        """
        valid_component = ["seas", "trend"]
        if component not in valid_component:
            raise ValueError(
                f"Only {', '.join(valid_component)} are allowed." f"See underlying 'decompose'."
            )

        output = self._apply_transformation(
            select=select,
            transformation="decompose",
            component=component,
            method=method,
            force_x13=force_x13,
            fallback=fallback,
            trading=trading,
            outlier=outlier,
            x13_binary=x13_binary,
            search_parents=search_parents,
            ignore_warnings=ignore_warnings,
            **kwargs,
        )

        self._datasets = output
        return

    def convert(
        self,
        flavor: Union[str, List],
        start_date: Union[str, datetime, None, List] = None,
        end_date: Union[str, datetime, None, List] = None,
        select: Union[str, int, Sequence[str], Sequence[int]] = "all",
    ):
        """Convert to other units.

        See Also
        --------
        :mod:`~econuy.core.Pipeline.convert`.

        """
        if flavor not in ["usd", "real", "gdp", "pcgdp"]:
            raise ValueError("'flavor' can be one of 'usd', 'real', " "or 'gdp'.")

        if flavor == "real":
            output = self._apply_transformation(
                select=select,
                transformation="convert",
                flavor=flavor,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            output = self._apply_transformation(
                select=select, transformation="convert", flavor=flavor
            )

        self._datasets = output
        return

    def rebase(
        self,
        start_date: Union[str, datetime, List],
        end_date: Union[str, datetime, None, List] = None,
        base: Union[float, List] = 100.0,
        select: Union[str, int, Sequence[str], Sequence[int]] = "all",
    ):
        """
        Scale to a period or range of periods.

        See Also
        --------
        :mod:`~econuy.core.Pipeline.rebase`.

        """
        output = self._apply_transformation(
            select=select,
            transformation="rebase",
            start_date=start_date,
            end_date=end_date,
            base=base,
        )
        self._datasets = output
        return

    def rolling(
        self,
        window: Union[int, List, None] = None,
        operation: Union[str, List] = "sum",
        select: Union[str, int, Sequence[str], Sequence[int]] = "all",
    ):
        """
        Calculate rolling averages or sums.

        See Also
        --------
        :mod:`~econuy.core.Pipeline.rolling`.

        """
        output = self._apply_transformation(
            select=select, transformation="rolling", window=window, operation=operation
        )
        self._datasets = output
        return

    def concat(
        self,
        select: Union[str, int, Sequence[str], Sequence[int]] = "all",
        concat_name: Optional[str] = None,
        force_suffix: bool = False,
    ):
        """
        Concatenate datasets in :attr:`datasets` and add as a new dataset.

        Resample to lowest frequency of selected datasets.

        Parameters
        ----------
        select : str, int, Sequence[str] or Sequence[int], default "all"
            Datasets to concatenate.
        concat_name : Optional[str], default None
            Name used as a key for the output dataset. The default None sets
            the name to "concat_{dataset_1_name}_..._{dataset_n_name}".
        force_suffix : bool, default False
            Whether to include each dataset's full name as a prefix in all
            indicator columns.

        """
        proc_select = self._select_datasets(select=select)
        proc_select = [x for x in proc_select if "concat_" not in x]
        selected_datasets = [d for n, d in self.datasets.items() if n in proc_select]

        indicator_names = [
            col for df in selected_datasets for col in df.columns.get_level_values(0)
        ]
        if len(indicator_names) > len(set(indicator_names)) or force_suffix is True:
            for n, d in zip(proc_select, selected_datasets):
                try:
                    full_name = self.available_datasets()["original"][n]
                except KeyError:
                    full_name = self.available_datasets()["custom"][n]
                d.columns = d.columns.set_levels(
                    f"{full_name}_" + d.columns.get_level_values(0), level=0
                )

        freqs = [pd.infer_freq(df.index) for df in selected_datasets]
        if all(freq == freqs[0] for freq in freqs):
            combined = pd.concat(selected_datasets, axis=1)
        else:
            for freq_opt in ["A-DEC", "A", "Q-DEC", "Q", "M", "2W-SUN", "W-SUN"]:
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
                                df_match = transform.resample(df, rule=freq_opt, operation="last")
                            elif type_df == "Flujo" and not any(
                                x in unit_df for x in ["%", "=", "Cambio"]
                            ):
                                df_match = transform.resample(df, rule=freq_opt, operation="sum")
                            else:
                                df_match = transform.resample(df, rule=freq_opt, operation="mean")
                        output.append(df_match)
                    combined = pd.concat(output, axis=1)
                    break
                else:
                    continue
        if concat_name is None:
            concat_name = "concat_" + "_".join(proc_select)
        elif not concat_name.startswith("concat_"):
            concat_name = "concat_" + concat_name
        else:
            pass
        self._datasets.update({concat_name: combined})
        return

    def save(self, select: Union[str, int, Sequence[str], Sequence[int]] = "all"):
        """Write datasets.

        Parameters
        ----------
        select : str, int, Sequence[str] or Sequence[int], default "all"
            Datasets to save.

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
                ops._io(
                    operation="save",
                    data_loc=self.location,
                    data=dataset,
                    name=name,
                    file_fmt=self.save_fmt,
                    multiindex=self.save_header,
                )
            else:
                continue

        return
