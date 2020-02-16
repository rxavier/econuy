from datetime import date
from os import PathLike
from pathlib import Path
from typing import Union, Optional

import pandas as pd

from econuy.processing import freqs, variations, seasonal, convert
from econuy.resources import columns
from econuy.resources.lstrings import fiscal_metadata
from econuy.retrieval import (nxr, national_accounts, cpi,
                              fiscal_accounts, labor)


def inflation(update: Union[str, PathLike, None] = None,
              save: Union[str, PathLike, None] = None,
              name: Optional[str] = None) -> pd.DataFrame:
    """
    Get common inflation measures.

    Parameters
    ----------
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Prices measures : pd.DataFrame
        Columns: CPI index, annual inflation, monthly inflation, seasonally
        adjusted monthly inflation and trend monthly inflation.

    """
    if name is None:
        name = "tfm_prices"
    data = cpi.get(update=update, revise_rows=6,
                   save=save, force_update=False)
    interannual = variations.chg_diff(data, operation="chg", period_op="inter")
    monthly = variations.chg_diff(data, operation="chg", period_op="last")
    trend, seasadj = seasonal.decompose(data, trading=True, outlier=False)
    monthly_sa = variations.chg_diff(seasadj, operation="chg",
                                     period_op="last")
    monthly_trend = variations.chg_diff(trend, operation="chg",
                                        period_op="last")
    output = pd.concat([data, interannual, monthly,
                        monthly_sa, monthly_trend], axis=1)

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        output.to_csv(save_path)

    return output


def exchange_rate(eop: bool = False, sell: bool = True,
                  seas_adj: Union[str, None] = None, cum: int = 1,
                  update: Union[str, PathLike, None] = None,
                  save: Union[str, PathLike, None] = None,
                  name: Optional[str] = None) -> pd.DataFrame:
    """
    Get nominal exchange rate data.

    Allow choosing end of period or average (monthly), sell/buy rates,
    calculating seasonal decomposition and rolling averages.

    Parameters
    ----------
    eop : bool, default False
        End of period data.
    sell : bool, default True
        Sell rate.
    seas_adj : {None, 'trend', 'seas'}
        Whether to seasonally adjust.
    cum : int, default 1
        How many periods to accumulate for rolling averages.
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Nominal exchange rate : pd.DataFrame

    """
    if name is None:
        name = "tfm_nxr"
    data = nxr.get(update=update, revise_rows=6,
                   save=save, force_update=False)

    if eop is False:
        output = data.iloc[:, [2, 3]]
    else:
        output = data.iloc[:, [0, 1]]

    if sell is True:
        output = output.iloc[:, 1].to_frame()
    else:
        output = output.iloc[:, 0].to_frame()

    if seas_adj in ["trend", "seas"] and cum == 1:
        trend, seasadj = seasonal.decompose(output, trading=True, outlier=True)
        if seas_adj == "trend":
            output = pd.concat([output, trend], axis=1)
        elif seas_adj == "seas":
            output = pd.concat([output, seasadj], axis=1)
        else:
            print("Only 'trend', 'seas' and None are "
                  "possible values for 'seas_adj'")

    if cum != 1:
        output = freqs.rolling(output, periods=cum, operation="average")

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        output.to_csv(save_path)

    return output


def fiscal(aggregation: str = "gps", fss: bool = True,
           unit: Union[str, None] = "gdp",
           start_date: Union[str, date, None] = None,
           end_date: Union[str, date, None] = None, cum: int = 1,
           seas_adj: Union[str, None] = None,
           update: Union[str, PathLike, None] = None,
           save: Union[str, PathLike, None] = None,
           name: Optional[str] = None) -> pd.DataFrame:
    """
    Get fiscal accounts data.

    Allow choosing government aggregation, whether to exclude the FSS
    (Fideicomiso  de la Seguridad Social, Social Security Trust Fund), the unit
    (UYU, real UYU, USD, real USD or percent of GDP), periods to accumuldate
    for rolling sums and seasonal adjustment.

    Parameters
    ----------
    aggregation : {'gps', 'nfps', 'gc'}
        Government aggregation. Can be ``gps`` (consolidated public sector),
        ``nfps`` (non-financial public sector) or ``gc`` (central government).
    fss : bool, default True
        If ``True``, exclude the `FSS's <https://www.impo.com.uy/bases/decretos
        /71-2018/25>_` income from gov't revenues and the FSS's
        interest revenues from gov't interest payments.
    unit : {'gdp', 'usd', 'real', 'real usd'}
        Unit in which data should be expressed. Possible values are ``real``,
        ``usd``, ``real usd`` and ``gdp``. If None or another string is set,
        no unit calculations will be performed, rendering the data as is
        (current UYU).
    start_date : str, datetime.date or None, default None
        If ``unit`` is set to ``real`` or ``real usd``, this parameter and
        ``end_date`` control how deflation is calculated.
    end_date :
        If ``unit`` is set to ``real`` or ``real usd``, this parameter and
        ``start_date`` control how deflation is calculated.
    cum : int, default 1
        How many periods to accumulate for rolling sums.
    seas_adj : {None, 'trend', 'seas'}
        Whether to seasonally adjust.
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Fiscal aggregation : pd.DataFrame

    """
    if name is None:
        name = "tfm_fiscal"
    data = fiscal_accounts.get(update=update, revise_rows=12,
                               save=save, force_update=False)
    gps = data["gps"]
    nfps = data["nfps"]
    gc = data["gc-bps"]

    proc = pd.DataFrame(index=gps.index)
    proc["Ingresos: SPNF-SPC"] = nfps["Ingresos: SPNF"]
    proc["Ingresos: GC-BPS"] = gc["Ingresos: GC-BPS"]
    proc["Egresos: Primarios SPNF-SPC"] = nfps["Egresos: Primarios SPNF"]
    proc["Egresos: Totales GC-BPS"] = gc["Egresos: GC-BPS"]
    proc["Egresos: Inversiones SPNF-SPC"] = nfps["Egresos: Inversiones"]
    proc["Egresos: Inversiones GC-BPS"] = gc["Egresos: Inversión"]
    proc["Intereses: SPNF"] = nfps["Intereses: Totales"]
    proc["Intereses: BCU"] = gps["Intereses: BCU"]
    proc["Intereses: SPC"] = proc["Intereses: SPNF"] + proc["Intereses: BCU"]
    proc["Intereses: GC-BPS"] = gc["Intereses: Total"]
    proc["Egresos: Totales SPNF"] = (proc["Egresos: Primarios SPNF-SPC"]
                                     + proc["Intereses: SPNF"])
    proc["Egresos: Totales SPC"] = (proc["Egresos: Totales SPNF"]
                                    + proc["Intereses: BCU"])
    proc["Egresos: Primarios GC-BPS"] = (proc["Egresos: Totales GC-BPS"]
                                         - proc["Intereses: GC-BPS"])
    proc["Resultado: Primario intendencias"] = nfps[
        "Resultado: Primario intendencias"
    ]
    proc["Resultado: Primario BSE"] = nfps["Resultado: Primario BSE"]
    proc["Resultado: Primario BCU"] = gps["Resultado: Primario BCU"]
    proc["Resultado: Primario SPNF"] = nfps["Resultado: Primario SPNF"]
    proc["Resultado: Global SPNF"] = nfps["Resultado: Global SPNF"]
    proc["Resultado: Primario SPC"] = gps["Resultado: Primario SPC"]
    proc["Resultado: Global SPC"] = gps["Resultado: Global SPC"]
    proc["Resultado: Primario GC-BPS"] = (proc["Ingresos: GC-BPS"]
                                          - proc["Egresos: Primarios GC-BPS"])
    proc["Resultado: Global GC-BPS"] = gc["Resultado: Global GC-BPS"]

    proc["Ingresos: FSS"] = gc["Ingresos: FSS"]
    proc["Intereses: FSS"] = gc["Intereses: BPS-FSS"]
    proc["Ingresos: SPNF-SPC aj. FSS"] = (proc["Ingresos: SPNF-SPC"]
                                          - proc["Ingresos: FSS"])
    proc["Ingresos: GC-BPS aj. FSS"] = (proc["Ingresos: GC-BPS"]
                                        - proc["Ingresos: FSS"])
    proc["Intereses: SPNF aj. FSS"] = (proc["Intereses: SPNF"]
                                       - proc["Intereses: FSS"])
    proc["Intereses: SPC aj. FSS"] = (proc["Intereses: SPC"]
                                      - proc["Intereses: FSS"])
    proc["Intereses: GC-BPS aj. FSS"] = (proc["Intereses: GC-BPS"]
                                         - proc["Intereses: FSS"])
    proc["Egresos: Totales SPNF aj. FSS"] = (proc["Egresos: Totales SPNF"]
                                             - proc["Intereses: FSS"])
    proc["Egresos: Totales SPC aj. FSS"] = (proc["Egresos: Totales SPC"]
                                            - proc["Intereses: FSS"])
    proc["Egresos: Totales GC-BPS aj. FSS"] = (proc["Egresos: Totales GC-BPS"]
                                               - proc["Intereses: FSS"])
    proc["Resultado: Primario SPNF aj. FSS"] = (proc["Resultado: Primario SPNF"]
                                                - proc["Ingresos: FSS"])
    proc["Resultado: Global SPNF aj. FSS"] = (proc["Resultado: Global SPNF"]
                                              - proc["Ingresos: FSS"]
                                              + proc["Intereses: FSS"])
    proc["Resultado: Primario SPC aj. FSS"] = (proc["Resultado: Primario SPC"]
                                               - proc["Ingresos: FSS"])
    proc["Resultado: Global SPC aj. FSS"] = (proc["Resultado: Global SPC"]
                                             - proc["Ingresos: FSS"]
                                             + proc["Intereses: FSS"])
    proc["Resultado: Primario GC-BPS aj. FSS"] = (proc["Resultado: Primario GC-BPS"]
                                                  - proc["Ingresos: FSS"])
    proc["Resultado: Global GC-BPS aj. FSS"] = (proc["Resultado: Global GC-BPS"]
                                                - proc["Ingresos: FSS"]
                                                + proc["Intereses: FSS"])

    output = proc.loc[:, fiscal_metadata[aggregation][fss]]
    columns._setmeta(output, area="Cuentas fiscales y deuda",
                     currency="UYU", inf_adj="No", index="No",
                     seas_adj="NSA", ts_type="Flujo", cumperiods=1)

    if unit == "gdp":
        output = freqs.rolling(output, periods=12, operation="sum")
        output = convert.pcgdp(output, hifreq=True)
    elif unit == "usd":
        output = convert.usd(output)
    elif unit == "real usd":
        output = convert.real(output, start_date=start_date, end_date=end_date)
        xr = nxr.get(update=update, revise_rows=6, save=save)
        output = output.divide(xr[start_date:end_date].mean()[3])
        columns._setmeta(output, currency="USD")
    elif unit == "real":
        output = convert.real(output, start_date=start_date, end_date=end_date)
    if seas_adj in ["trend", "seas"] and unit != "gdp" and cum == 1:
        output_trend, output_seasadj = seasonal.decompose(output, trading=True,
                                                          outlier=True)
        if seas_adj == "trend":
            output = output_trend
        elif seas_adj == "seas":
            output = output_seasadj
        else:
            print("Only 'trend', 'seas' and None are "
                  "possible values for 'seas_adj'")
    if cum != 1:
        output = freqs.rolling(output, periods=cum, operation="sum")

    if save is not False:
        save_path = (Path(save) / name).with_suffix(".csv")
        output.to_csv(save_path)

    return output


def labor_mkt(seas_adj: Union[str, None] = "trend",
              update: Union[str, PathLike, None] = None,
              save: Union[str, PathLike, None] = None,
              name: Optional[str] = None) -> pd.DataFrame:
    """
    Get labor market data.

    Allow choosing seasonal adjustment.

    Parameters
    ----------
    seas_adj : {'trend', 'seas', None}
        Whether to seasonally adjust.
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Labor market data : Pandas dataframe

    """
    if name is None:
        name = "tfm_labor"
    data = labor.get(update=update, revise_rows=6,
                     save=save, force_update=False)
    output = data

    if seas_adj in ["trend", "seas"]:
        trend, seasadj = seasonal.decompose(data, trading=True, outlier=True)
        if seas_adj == "trend":
            output = pd.concat([data, trend], axis=1)
        elif seas_adj == "seas":
            output = pd.concat([data, seasadj], axis=1)

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        output.to_csv(save_path)

    return output


def nat_accounts(supply: bool = True, real: bool = True, index: bool = False,
                 seas_adj: bool = False, usd: bool = False, cum: int = 1,
                 cust_seas_adj: Union[str, None] = None,
                 variation: Union[str, None] = None,
                 update: Union[str, PathLike, None] = None,
                 save: Union[str, PathLike, None] = None,
                 name: Optional[str] = None) -> pd.DataFrame:
    """Get national accounts data.

    Attempt to find one of the available data tables with the selected
    combination of parameters (``supply``, ``real``. ``index`` and
    ``seas_adj``).

    Parameters
    ----------
    supply : bool, default True
        Supply or demand side.
    real : bool, default True
        Constant or current.
    index : bool, default False
        Base 100 index or not.
    seas_adj : bool, default True
        Seasonally adjusted or not.
    usd : bool, default False
        If True, convert to USD.
    cum : int, default 1
        How many periods to accumulate for rolling sums.
    cust_seas_adj : {None, 'trend', 'seas'}
        Whether to seasonally adjust.
    variation : {None, 'last', 'inter', 'annual}
        Type of percentage change to calculate. Can be ``last``, ``inter`` or
        ``annual``.
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Selected national accounts : pd.DataFrame

    Raises
    ------
    KeyError:
        If the combined parameters do not correspond to an available table.

    """
    if name is None:
        name = "tfm_na"
    data = national_accounts.get(update=update, revise_rows=4,
                                 save=save, force_update=False)

    search_terms = []
    if supply is True:
        search_terms += ["ind"]
    else:
        search_terms += ["gas"]
    if real is True:
        search_terms += ["con"]
    else:
        search_terms += ["cur"]
    if index is True:
        search_terms += ["idx"]
    if seas_adj is True:
        search_terms += ["sa"]
    else:
        search_terms += ["nsa"]

    table = "_".join(search_terms)

    try:
        output = data[table]
    except KeyError:
        print("No available tables with selected parameters.")
        return

    if usd is True:
        output = convert.usd(output)

    if cust_seas_adj is not None and seas_adj is False and cum == 1:
        trend, seasadj = seasonal.decompose(output, trading=True, outlier=True)
        if cust_seas_adj == "trend":
            output = trend
        elif cust_seas_adj == "seas":
            output = seasadj
        else:
            print("Only 'trend', 'seas' and None are "
                  "possible values for 'seas_adj'")

    if cum != 1:
        output = freqs.rolling(output, periods=cum, operation="sum")

    if variation in ["last", "inter", "annual"]:
        output = variations.chg_diff(output, operation="chg",
                                     period_op=variation)
    elif variation is not None:
        print("Only 'last', 'inter' and 'annual' are "
              "possible values for 'variation'")

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        output.to_csv(save_path)

    return output
