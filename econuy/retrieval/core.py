from __future__ import annotations
from typing import Union, Optional, Dict, Callable
from os import PathLike
from datetime import datetime

import pandas as pd
from sqlalchemy.engine.base import Engine, Connection

from econuy.utils import ops, datasets
from econuy import transform


class Retriever(object):
    def __init__(self,
                 location: Union[str, PathLike, Engine,
                                 Connection, None] = None,
                 download: bool = True,
                 always_save: bool = True,
                 read_fmt: str = "csv",
                 read_header: Optional[str] = "included",
                 save_fmt: str = "csv",
                 save_header: Optional[str] = "included",
                 errors: str = "raise"):
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
    def dataset(self):
        return self._dataset

    @property
    def name(self):
        return self._name

    @staticmethod
    def available_datasets():
        all = datasets.original()
        all.update(datasets.custom())
        return all

    @property
    def description(self):
        try:
            return self.available_datasets()[self.name]["description"]
        except KeyError:
            return None

    def get(self, dataset: str) -> Retriever:
        prev_data = ops._io(operation="read", data_loc=self.location,
                            name=dataset, file_fmt=self.read_fmt,
                            multiindex=self.read_header)
        if not self.download and not prev_data.empty:
            self._dataset = prev_data
        else:
            selection = self.available_datasets()[dataset]
            if dataset in ["trade_balance", "terms_of_trade", "rxr_custom",
                           "commodity_index", "cpi_measures", "_lin_gdp",
                           "net_public_debt", "balance_summary"]:
                # Some datasets require retrieving other datasets. Passing the
                # class instance allows running these retrieval operations
                # with the same parameters (for example, save file formats).
                new_data = selection["function"](retriever=self)
            elif dataset in ["reserves_changes", "nxr_daily"]:
                # Datasets that require many requests (mostly daily data) benefit
                # from having previous data, so they can start requests
                # from the last available date.
                new_data = selection["function"](retriever=self,
                                                 previous_data=prev_data)
            else:
                new_data = selection["function"]()
            data = ops._revise(new_data=new_data, prev_data=prev_data,
                               revise_rows="nodup")
            self._dataset = data
        self._name = dataset
        if self.always_save:
            self.save()
        return

    def _apply_transformation(self,
                              transformation: Callable,
                              **kwargs) -> Dict[str, pd.DataFrame]:
        """Helper method to apply transformations on :attr:`dataset`.

        Parameters
        ----------
        transformation : Callable
            Function representing the transformation to apply.

        Returns
        -------
        Transformed dataset : pd.DataFrame

        """
        if isinstance(self.dataset, Dict):
            output = {}
            for subname, subdata in self.dataset.items():
                subtransformed = transformation(subdata,
                                                **kwargs)
                output.update({subname: subtransformed})
        else:
            output = transformation(self.dataset, **kwargs)

        return output

    def resample(self, rule: Union[pd.DateOffset, pd.Timedelta, str],
                 operation: str = "sum",
                 interpolation: str = "linear") -> Retriever:
        """
        Resample to target frequencies.

        See Also
        --------
        :func:`~econuy.transform.resample`

        """
        output = transform.resample(self.dataset,
                                    rule=rule,
                                    operation=operation,
                                    interpolation=interpolation)
        self._dataset = output
        return

    def chg_diff(self, operation: str = "chg",
                 period: str = "last") -> Retriever:
        """
        Calculate pct change or difference.

        See Also
        --------
        :func:`~econuy.transform.chg_diff`

        """
        output = transform.chg_diff(
            self.dataset, operation=operation, period=period)
        self._dataset = output
        return

    def decompose(self, component: str = "both",
                  method: str = "x13",
                  force_x13: bool = False,
                  fallback: str = "loess",
                  trading: bool = True,
                  outlier: bool = True,
                  x13_binary: Union[str, PathLike] = "search",
                  search_parents: int = 1,
                  ignore_warnings: bool = True,
                  errors: Optional[str] = None,
                  **kwargs) -> Retriever:
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

        output = transform.decompose(self.dataset, component=component,
                                     method=method,
                                     force_x13=force_x13,
                                     fallback=fallback,
                                     trading=trading,
                                     outlier=outlier,
                                     x13_binary=x13_binary,
                                     search_parents=search_parents,
                                     ignore_warnings=ignore_warnings,
                                     errors=self.errors,
                                     **kwargs)
        self._dataset = output
        return

    def convert(self, flavor: str,
                start_date: Union[str, datetime, None] = None,
                end_date: Union[str, datetime, None] = None) -> Retriever:
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

        if flavor == "usd":
            output = transform.convert_usd(self.dataset,
                                           errors=self.errors,
                                           retriever=self)
        elif flavor == "real":
            output = transform.convert_real(self.dataset, start_date=start_date,
                                            end_date=end_date,
                                            errors=self.errors,
                                            retriever=self)
        else:
            output = transform.convert_gdp(self.dataset, errors=self.errors,
                                           retriever=self)

        self._dataset = output
        return

    def rebase(self, start_date: Union[str, datetime],
               end_date: Union[str, datetime, None] = None,
               base: Union[float, int] = 100.0) -> Retriever:
        """
        Scale to a period or range of periods.

        See Also
        --------
        :func:`~econuy.transform.rebase`

        """
        output = transform.rebase(self.dataset, start_date=start_date,
                                  end_date=end_date, base=base)
        self._dataset = output
        return

    def rolling(self, window: Optional[int] = None,
                operation: str = "sum") -> Retriever:
        """
        Calculate rolling averages or sums.

        See Also
        --------
        :func:`~econuy.transform.rolling`

        """
        output = transform.rolling(self.dataset, window=window,
                                   operation=operation)
        self._dataset = output
        return

    def save(self):
        if isinstance(self.dataset, Dict):
            for k, v in self.dataset.items():
                ops._io(operation="save", data_loc=self.location,
                        name=f"{self.name}_{k}", file_fmt=self.save_fmt,
                        multiindex=self.save_header, data=v)
        else:
            ops._io(operation="save", data_loc=self.location,
                    name=self.name, file_fmt=self.save_fmt,
                    multiindex=self.save_header, data=self.dataset)
        return
