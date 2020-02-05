from datetime import date
from os import PathLike, path, makedirs
from pathlib import Path
from typing import Union, Optional

import pandas as pd
import pandas_flavor as pf

from econuy.processing import variations, freqs, seasonal, convert, index
from econuy.retrieval import (cpi, nxr, fiscal_accounts, national_accounts,
                              labor, rxr, commodity_index, reserves_chg,
                              fx_spot_ff)
from econuy.frequent.frequent import (inflation, fiscal, nat_accounts,
                                      exchange_rate, labor_mkt)


class Session(object):
    def __init__(self,
                 location: Union[str, PathLike] = "econuy-data",
                 monthly_revise: int = 12,
                 quarterly_revise: int = 4,
                 annual_revise: int = 3,
                 force_update: bool = False):
        self.location = location
        self.revise_rows = {"monthly": monthly_revise,
                            "quarterly": quarterly_revise,
                            "annual": annual_revise}
        self.force_update = force_update
        self.dataset = None

        if not path.exists(self.location):
            makedirs(self.location)

    def get(self,
            dataset: str,
            update: bool = True,
            save: bool = True,
            override: Optional[str] = None):

        if update is True:
            update_path = Path(self.location)
        else:
            update_path = None
        if save is True:
            save_path = Path(self.location)
        else:
            save_path = None

        if dataset == "cpi" or dataset == "prices":
            output = cpi.get(update=update_path,
                             revise_rows=self.revise_rows["monthly"],
                             save=save_path,
                             force_update=self.force_update,
                             name=override)
        elif dataset == "fiscal":
            output = fiscal_accounts.get(update=update_path,
                                         revise_rows=self.revise_rows[
                                             "monthly"],
                                         save=save_path,
                                         force_update=self.force_update,
                                         name=override)
        elif dataset == "nxr":
            output = nxr.get(update=update_path,
                             revise_rows=self.revise_rows["monthly"],
                             save=save_path,
                             force_update=self.force_update,
                             name=override)
        elif dataset == "naccounts" or dataset == "na":
            output = national_accounts.get(update=update_path,
                                           revise_rows=self.revise_rows[
                                               "quarterly"],
                                           save=save_path,
                                           force_update=self.force_update,
                                           name=override)
        elif dataset == "labor" or dataset == "labour":
            output = labor.get(update=update_path,
                               revise_rows=self.revise_rows[
                                   "monthly"],
                               save=save_path,
                               force_update=self.force_update,
                               name=override)
        elif dataset == "rxr_custom" or dataset == "rxr-custom":
            output = rxr.get_custom(update=update_path,
                                    revise_rows=self.revise_rows[
                                        "monthly"],
                                    save=save_path,
                                    force_update=self.force_update,
                                    name=override)
        elif dataset == "rxr_official" or dataset == "rxr-official":
            output = rxr.get_official(update=update_path,
                                      revise_rows=self.revise_rows[
                                          "monthly"],
                                      save=save_path,
                                      force_update=self.force_update,
                                      name=override)
        elif dataset == "commodity_index" or dataset == "comm_index":
            output = commodity_index.get(update=update_path,
                                         save=save_path,
                                         name=override)
        elif dataset == "reserves":
            output = reserves_chg.get(update=update_path,
                                      save=save_path,
                                      name=override)
        elif dataset == "fx_spot_ff" or dataset == "spot_ff":
            output = fx_spot_ff.get(update=update_path,
                                    save=save_path,
                                    name=override)
        else:
            raise ValueError("Invalid value set for `dataset`")

        self.dataset = output

        return output

    def get_tfm(self,
                dataset: str,
                update: bool = True,
                save: bool = True,
                override: Optional[str] = None,
                **kwargs):

        if update is True:
            update_path = Path(self.location)
        else:
            update_path = None
        if save is True:
            save_path = Path(self.location)
        else:
            save_path = None

        if dataset == "inflation":
            output = inflation(update=update_path,
                               save=save_path,
                               name=override)
        elif dataset == "fiscal":
            output = fiscal(update=update_path,
                            save=save_path,
                            name=override,
                            **kwargs)
        elif dataset == "nxr":
            output = exchange_rate(update=update_path,
                                   save=save_path,
                                   name=override,
                                   **kwargs)
        elif dataset == "naccounts" or dataset == "na":
            output = nat_accounts(update=update_path,
                                  save=save_path,
                                  name=override,
                                  **kwargs)
        elif dataset == "labor" or dataset == "labour":
            output = labor_mkt(update=update_path,
                               save=save_path,
                               name=override,
                               **kwargs)
        else:
            output = None

        return output


@pf.register_dataframe_method
def chg_diff(df: pd.DataFrame, operation: str = "chg",
             period_op: str = "last"):
    output = variations.chg_diff(df, operation=operation, period_op=period_op)
    return output


@pf.register_dataframe_method
def freq_resample(df: pd.DataFrame, target: str, operation: str = "sum",
                  interpolation: str = "linear"):
    output = freqs.freq_resample(df, target=target, operation=operation,
                                 interpolation=interpolation)
    return output


@pf.register_dataframe_method
def decompose(df: pd.DataFrame, flavor: Optional[str] = None,
              trading: bool = True, outlier: bool = True,
              x13_binary: Union[str, PathLike] = "search",
              search_parents: int = 1):
    result = seasonal.decompose(df,
                                trading=trading,
                                outlier=outlier,
                                x13_binary=x13_binary,
                                search_parents=search_parents)
    if flavor == "trend":
        output = result[0]
    elif flavor == "seas" or type == "seasonal":
        output = result[1]
    else:
        output = result

    return output


@pf.register_dataframe_method
def convert(df: pd.DataFrame, flavor: str,
            start_date: Union[str, date, None] = None,
            end_date: Union[str, date, None] = None, hifreq: bool = True):
    if flavor == "usd:":
        output = convert.usd(df)
    elif flavor == "real":
        output = convert.real(df, start_date=start_date, end_date=end_date)
    elif flavor == "pcgdp":
        output = convert.pcgdp(df, hifreq=hifreq)
    else:
        output = None

    return output


@pf.register_dataframe_method
def base_index(df: pd.DataFrame, start_date: Union[str, date],
               end_date: Union[str, date, None] = None, base: float = 100):
    output = index.base_index(df, start_date=start_date,
                              end_date=end_date, base=base)
    return output
