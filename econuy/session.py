from datetime import date
from os import PathLike, path, makedirs
from pathlib import Path
from typing import Union, Optional

from econuy.frequent.frequent import (inflation, fiscal, nat_accounts,
                                      exchange_rate, labor_mkt)
from econuy.processing import variations, freqs, seasonal, convert, index
from econuy.retrieval import (cpi, nxr, fiscal_accounts, national_accounts,
                              labor, rxr, commodity_index, reserves_chg,
                              fx_spot_ff)


class Session(object):
    def __init__(self,
                 loc_dir: Union[str, PathLike] = "econuy-data",
                 monthly_revise: int = 12,
                 quarterly_revise: int = 4,
                 annual_revise: int = 3,
                 force_update: bool = False):
        self.loc_dir = loc_dir
        self.revise_rows = {"monthly": monthly_revise,
                            "quarterly": quarterly_revise,
                            "annual": annual_revise}
        self.force_update = force_update
        self.dataset = None

        if not path.exists(self.loc_dir):
            makedirs(self.loc_dir)

    def get(self,
            dataset: str,
            update: bool = True,
            save: bool = True,
            override: Optional[str] = None,
            final: bool = False):

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
            output = None
        self.dataset = output

        if final is True:
            return self.dataset
        else:
            return self

    def get_tfm(self,
                dataset: str,
                update: bool = True,
                save: bool = True,
                override: Optional[str] = None,
                final: bool = False,
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
        self.dataset = output

        if final is True:
            return self.dataset
        else:
            return self

    def freq_resample(self, target: str, operation: str = "sum",
                      interpolation: str = "linear", final: bool = False):
        output = freqs.freq_resample(self.dataset, target=target,
                                     operation=operation,
                                     interpolation=interpolation)
        self.dataset = output

        if final is True:
            return self.dataset
        else:
            return self

    def chg_diff(self, operation: str = "chg",
                 period_op: str = "last", final: bool = False):
        output = variations.chg_diff(self.dataset, operation=operation,
                                     period_op=period_op)
        self.dataset = output

        if final is True:
            return self.dataset
        else:
            return self

    def decompose(self, flavor: Optional[str] = None,
                  trading: bool = True, outlier: bool = True,
                  x13_binary: Union[str, PathLike] = "search",
                  search_parents: int = 1, final: bool = False):
        result = seasonal.decompose(self.dataset,
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

        self.dataset = output

        if final is True:
            return self.dataset
        else:
            return self

    def unit_conv(self, flavor: str, update: Union[str, PathLike, None] = None,
                  save: Union[str, PathLike, None] = None,
                  final: bool = False, **kwargs):
        if flavor == "usd":
            output = convert.usd(self.dataset, update=update,
                                 save=save)
        elif flavor == "real":
            output = convert.real(self.dataset, update=update,
                                  save=save, **kwargs)
        elif flavor == "pcgdp":
            output = convert.pcgdp(self.dataset, update=update,
                                   save=save, **kwargs)
        else:
            output = None
        self.dataset = output

        if final is True:
            return self.dataset
        else:
            return self

    def base_index(self, start_date: Union[str, date],
                   end_date: Union[str, date, None] = None,
                   base: float = 100, final: bool = False):
        output = index.base_index(self.dataset, start_date=start_date,
                                  end_date=end_date, base=base)
        self.dataset = output

        if final is True:
            return self.dataset
        else:
            return self
