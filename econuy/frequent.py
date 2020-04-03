from datetime import date
from os import PathLike, mkdir, path
from pathlib import Path
from typing import Union, Optional

import pandas as pd

from econuy import transform
from econuy.utils import metadata
from econuy.utils.lstrings import fiscal_metadata, wap_url
from econuy.retrieval import (nxr, national_accounts, cpi,
                              fiscal_accounts, labor)


def inflation(update_path: Union[str, PathLike, None] = None,
              save_path: Union[str, PathLike, None] = None,
              name: Optional[str] = None) -> pd.DataFrame:
    """
    Get common inflation measures.

    Parameters
    ----------
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
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
    data = cpi.get(update_path=update_path, revise_rows=6,
                   save_path=save_path, force_update=False)
    interannual = transform.chg_diff(data, operation="chg", period_op="inter")
    monthly = transform.chg_diff(data, operation="chg", period_op="last")
    trend, seasadj = transform.decompose(data, trading=True, outlier=False)
    monthly_sa = transform.chg_diff(seasadj, operation="chg",
                                    period_op="last")
    monthly_trend = transform.chg_diff(trend, operation="chg",
                                       period_op="last")
    output = pd.concat([data, interannual, monthly,
                        monthly_sa, monthly_trend], axis=1)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        output.to_csv(full_save_path)

    return output


def fiscal(aggregation: str = "gps", fss: bool = True,
           unit: Union[str, None] = "gdp",
           start_date: Union[str, date, None] = None,
           end_date: Union[str, date, None] = None, cum: int = 1,
           seas_adj: Union[str, None] = None,
           update_path: Union[str, PathLike, None] = None,
           save_path: Union[str, PathLike, None] = None,
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
        /71-2018/25>`_ income from gov't revenues and the FSS's
        interest revenues from gov't interest payments.
    unit : {'gdp', 'usd', 'real', 'real_usd'}
        Unit in which data should be expressed. Possible values are ``real``,
        ``usd``, ``real_usd`` and ``gdp``. If ``None`` or another string is
        set, no unit calculations will be performed, rendering the data as is
        (current UYU).
    start_date : str, datetime.date or None, default None
        If ``unit`` is set to ``real`` or ``real_usd``, this parameter and
        ``end_date`` control how deflation is calculated.
    end_date :
        If ``unit`` is set to ``real`` or ``real_usd``, this parameter and
        ``start_date`` control how deflation is calculated.
    cum : int, default 1
        How many periods to accumulate for rolling sums.
    seas_adj : {None, 'trend', 'seas'}
        Whether to seasonally adjust.
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Fiscal aggregation : pd.DataFrame

    Raises
    ------
    ValueError
        If ``seas_adj``, ``unit`` or ``aggregation`` are given an invalid
        keywords.

    """
    if name is None:
        name = "tfm_fiscal"

    if seas_adj not in ["trend", "seas", None]:
        raise ValueError("'seas_adj' can be 'trend', 'seas' or None.")
    if unit not in ["gdp", "usd", "real", "real_usd", None]:
        raise ValueError("'unit' can be 'gdp', 'usd', 'real', 'real_usd' or"
                         " None.")
    if aggregation not in ["gps", "nfps", "gc"]:
        raise ValueError("'aggregation' can be 'gps', 'nfps' or 'gc'.")

    data = fiscal_accounts.get(update_path=update_path, revise_rows=12,
                               save_path=save_path, force_update=False)
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
    proc["Resultado: Primario SPNF aj. FSS"] = (
        proc["Resultado: Primario SPNF"]
        - proc["Ingresos: FSS"])
    proc["Resultado: Global SPNF aj. FSS"] = (proc["Resultado: Global SPNF"]
                                              - proc["Ingresos: FSS"]
                                              + proc["Intereses: FSS"])
    proc["Resultado: Primario SPC aj. FSS"] = (proc["Resultado: Primario SPC"]
                                               - proc["Ingresos: FSS"])
    proc["Resultado: Global SPC aj. FSS"] = (proc["Resultado: Global SPC"]
                                             - proc["Ingresos: FSS"]
                                             + proc["Intereses: FSS"])
    proc["Resultado: Primario GC-BPS aj. FSS"] = (
        proc["Resultado: Primario GC-BPS"]
        - proc["Ingresos: FSS"])
    proc["Resultado: Global GC-BPS aj. FSS"] = (
        proc["Resultado: Global GC-BPS"]
        - proc["Ingresos: FSS"]
        + proc["Intereses: FSS"])

    output = proc.loc[:, fiscal_metadata[aggregation][fss]]
    metadata._set(output, area="Cuentas fiscales y deuda",
                  currency="UYU", inf_adj="No", unit="Millones",
                  seas_adj="NSA", ts_type="Flujo", cumperiods=1)

    if unit == "gdp":
        output = transform.rolling(output, periods=12, operation="sum")
        output = transform.convert_gdp(output)
    elif unit == "usd":
        output = transform.convert_usd(output)
    elif unit == "real_usd":
        output = transform.convert_real(output, start_date=start_date,
                                        end_date=end_date)
        xr = nxr.get_monthly(update_path=update_path, revise_rows=6,
                             save_path=save_path)
        output = output.divide(xr[start_date:end_date].mean()[1])
        metadata._set(output, currency="USD")
    elif unit == "real":
        output = transform.convert_real(output, start_date=start_date,
                                        end_date=end_date)
    if seas_adj in ["trend", "seas"] and unit != "gdp" and cum == 1:
        output_trend, output_seasadj = transform.decompose(output,
                                                           trading=True,
                                                           outlier=True)
        if seas_adj == "trend":
            output = output_trend
        elif seas_adj == "seas":
            output = output_seasadj

    if cum != 1:
        output = transform.rolling(output, periods=cum, operation="sum")

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        output.to_csv(full_save_path)

    return output


def labor_rate_people(seas_adj: Union[str, None] = None,
                      update_path: Union[str, PathLike, None] = None,
                      save_path: Union[str, PathLike, None] = None,
                      name: Optional[str] = None) -> pd.DataFrame:
    """
    Get labor data, both rates and persons. Allow choosing seasonal adjustment.

    Parameters
    ----------
    seas_adj : {'trend', 'seas', None}
        Whether to seasonally adjust.
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Labor market data : pd.DataFrame

    Raises
    ------
    ValueError
        If ``seas_adj`` is given an invalid keyword.

    """
    if name is None:
        name = "tfm_labor"

    if seas_adj not in ["trend", "seas", None]:
        raise ValueError("'seas_adj' can be 'trend', 'seas' or None.")

    rates = labor.get_rates(update_path=update_path, revise_rows=6,
                            save_path=save_path, force_update=False)

    if seas_adj in ["trend", "seas"]:
        trend, seasadj = transform.decompose(rates, trading=True, outlier=True)
        if seas_adj == "trend":
            rates = trend
        elif seas_adj == "seas":
            rates = seasadj

    working_age = pd.read_excel(wap_url, skiprows=7,
                                index_col=0, nrows=92).dropna(how="all")
    ages = list(range(14, 90)) + ["90 y más"]
    working_age = working_age.loc[ages].sum()
    working_age.index = pd.date_range(start="1996-06-30", end="2050-06-30",
                                      freq="A-JUN")
    monthly_working_age = working_age.resample("M").interpolate("linear")
    monthly_working_age = monthly_working_age.loc[rates.index]
    persons = rates.iloc[:, [0, 1]].div(100).mul(monthly_working_age, axis=0)
    persons["Desempleados"] = rates.iloc[:, 2].div(100).mul(persons.iloc[:, 0])
    persons.columns = ["Activos", "Empleados", "Desempleados"]
    seas_text = "NSA"
    if seas_adj == "trend":
        seas_text = "Trend"
    elif seas_adj == "seas":
        seas_text = "SA"
    metadata._set(persons, area="Mercado laboral", currency="-",
                  inf_adj="No", unit="Personas", seas_adj=seas_text,
                  ts_type="-", cumperiods=1)

    output = pd.concat([rates, persons], axis=1)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        output.to_csv(full_save_path)

    return output


def labor_real_wages(seas_adj: Union[str, None] = None,
                     update_path: Union[str, PathLike, None] = None,
                     save_path: Union[str, PathLike, None] = None,
                     name: Optional[str] = None) -> pd.DataFrame:
    """
    Get real wages. Allow choosing seasonal adjustment.

    Parameters
    ----------
    seas_adj : {'trend', 'seas', None}
        Whether to seasonally adjust.
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Real wages data : pd.DataFrame

    Raises
    ------
    ValueError
        If ``seas_adj`` is given an invalid keyword.

    """
    if name is None:
        name = "tfm_wages"

    if seas_adj not in ["trend", "seas", None]:
        raise ValueError("'seas_adj' can be 'trend', 'seas' or None.")

    wages = labor.get_wages(update_path=update_path, revise_rows=6,
                            save_path=save_path, force_update=False)
    real_wages = wages.copy()
    real_wages.columns = ["Índice medio de salarios reales",
                          "Índice medio de salarios reales privados",
                          "Índice medio de salarios reales públicos"]
    metadata._set(real_wages, area="Mercado laboral", currency="UYU",
                  inf_adj="Sí", seas_adj="NSA", ts_type="-", cumperiods=1)
    real_wages = transform.convert_real(real_wages)
    output = pd.concat([wages, real_wages], axis=1)

    if seas_adj in ["trend", "seas"]:
        trend, seasadj = transform.decompose(output,
                                             trading=True, outlier=False)
        if seas_adj == "trend":
            output = trend
        elif seas_adj == "seas":
            output = seasadj

    output = transform.base_index(output, start_date="2008-07-31")

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        output.to_csv(full_save_path)

    return output


def nat_accounts(supply: bool = True, real: bool = True, index: bool = False,
                 off_seas_adj: bool = False, usd: bool = False, cum: int = 1,
                 seas_adj: Union[str, None] = None,
                 variation: Union[str, None] = None,
                 update_path: Union[str, PathLike, None] = None,
                 save_path: Union[str, PathLike, None] = None,
                 name: Optional[str] = None) -> Optional[pd.DataFrame]:
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
    off_seas_adj : bool, default True
        Seasonally adjusted or not.
    usd : bool, default False
        If ``True``, convert to USD.
    cum : int, default 1
        How many periods to accumulate for rolling sums.
    seas_adj : {None, 'trend', 'seas'}
        Whether to seasonally adjust.
    variation : {None, 'last', 'inter', 'annual}
        Type of percentage change to calculate. Can be ``last``, ``inter`` or
        ``annual``.
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Selected national accounts : pd.DataFrame or None

    Raises
    ------
    KeyError
        If the combined parameters do not correspond to an available table.
    ValueError
        If ``seas_adj`` or ``variation`` are given invalid keywords.

    """
    if name is None:
        name = "tfm_na"

    if seas_adj not in ["trend", "seas", None]:
        raise ValueError("'seas_adj' can be 'trend', 'seas' or None.")
    if variation not in ["last", "inter", "annual", None]:
        raise ValueError("'variation' can be 'last', 'inter', 'annual', or"
                         " None.")

    data = national_accounts.get(update_path=update_path, revise_rows=4,
                                 save_path=save_path, force_update=False)

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
    if off_seas_adj is True:
        search_terms += ["sa"]
    else:
        search_terms += ["nsa"]

    table = "_".join(search_terms)

    try:
        output = data[table]
    except KeyError:
        raise KeyError("No available tables with selected parameters.")

    if usd is True:
        output = transform.convert_usd(output)

    if seas_adj in ["trend", "seas"] and off_seas_adj is False and cum == 1:
        trend, seasadj = transform.decompose(output, trading=True,
                                             outlier=True)
        if seas_adj == "trend":
            output = trend
        elif seas_adj == "seas":
            output = seasadj

    if cum != 1:
        output = transform.rolling(output, periods=cum, operation="sum")

    if variation is not None:
        output = transform.chg_diff(output, operation="chg",
                                    period_op=variation)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        output.to_csv(full_save_path)

    return output
