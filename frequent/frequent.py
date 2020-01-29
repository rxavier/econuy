import os
from datetime import date
from typing import Union

import pandas as pd

from econuy.config import ROOT_DIR
from econuy.retrieval import nxr, national_accounts, cpi, fiscal_accounts, \
    labor
from econuy.processing import columns, freqs, variations, seasonal, convert
from econuy.resources.utils import fiscal_metadata

DATA_PATH = os.path.join(ROOT_DIR, "data")


def inflation():
    """Update CPI data and return common inflation measures.

    Returns
    -------
    dataframe : Pandas dataframe
        Columns: CPI index, annual inflation, monthly inflation, seasonally
        adjusted monthly inflation and trend monthly inflation.

    """
    data = cpi.get(update="cpi.csv", revise_rows=6,
                   save="cpi.csv", force_update=False)
    interannual = variations.chg_diff(data, operation="chg", period_op="inter")
    monthly = variations.chg_diff(data, operation="chg", period_op="last")
    trend, seasadj = seasonal.decompose(data, trading=True, outlier=False)
    monthly_sa = variations.chg_diff(seasadj, operation="chg",
                                     period_op="last")
    monthly_trend = variations.chg_diff(trend, operation="chg",
                                        period_op="last")

    output = pd.concat([data, interannual, monthly,
                        monthly_sa, monthly_trend], axis=1)

    return output


def exchange_rate(eop: bool = False, sell: bool = True,
                  seas_adj: Union[str, None] = None, cum: int = 1):
    """Get nominal exchange rate data.

    Allow choosing end of period or average (monthly), sell/buy rates,
    calculating seasonal decomposition and rolling averages.

    Parameters
    ----------
    eop : bool (default is False)
        End of period data.
    sell : bool (default is True)
        Sellr ate.
    seas_adj : str or None (default is None)
        Allowed strings: 'trend' or 'seas'.
    cum : int (default is 1)
        How many periods to accumulate for rolling averages.

    Returns
    -------
    dataframe : Pandas dataframe

    """
    data = nxr.get(update="nxr.csv", revise_rows=6,
                   save="nxr.csv", force_update=False)

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

    return output


def fiscal(aggregation: str = "gps", fss: bool = True,
           unit: Union[str, None] = "gdp",
           start_date: Union[str, date, None] = None,
           end_date: Union[str, date, None] = None, cum: int = 1,
           seas_adj: Union[str, None] = None):
    """Get fiscal accounts data.

    Allow choosing government aggregation, whether to exclude the FSS
    (Fideicomiso  de la Seguridad Social, Social Security Trust Fund), the unit
    (UYU, real UYU, USD, real USD or percent of GDP), periods to accumuldate
    for rolling sums and seasonal adjustment.

    Parameters
    ----------
    aggregation : str (default is 'gps')
        Government aggregation. Can be 'gps' (consolidated public sector),
        'nfps' (non-financial public sector) or 'gc' (central government).
    fss : bool (default is True)
        If True, exclude the FSS's income from gov't revenues and the FSS's
        interest revenues from gov't interest payments.
    unit : str or None (default is 'gdp')
        Unit in which data should be expressed. Possible values are 'real',
        'usd', 'real usd' and 'gdp'. If None or another string is set, no unit
        calculations will be performed, rendering the data as is (current UYU).
    start_date : str, date or None (default is None)
        If `unit` is set to 'real' or 'real usd', this parameter and `end_date`
        control how deflation is calculated.
    end_date :
        If `unit` is set to 'real' or 'real usd', this parameter and
        `start_date` control how deflation is calculated.
    cum : int (default is 1)
        How many periods to accumulate for rolling sums.
    seas_adj :
        Allowed strings: 'trend' or 'seas'.

    Returns
    -------
    dataframe : Pandas dataframe

    """
    data = fiscal_accounts.get(update=True, revise_rows=12,
                               save=True, force_update=False)
    gps = data["fiscal_gps"]
    nfps = data["fiscal_nfps"]
    gc = data["fiscal_gc-bps"]

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
    columns.set_metadata(output, area="Cuentas fiscales y deuda",
                         currency="UYU", inf_adj="No", index="No",
                         seas_adj="NSA", ts_type="Flujo", cumperiods=1)

    if unit == "gdp":
        output = freqs.rolling(output, periods=12, operation="sum")
        output = convert.pcgdp(output, hifreq=True)
    elif unit == "usd":
        output = convert.usd(output)
    elif unit == "real usd":
        output = convert.real(output, start_date=start_date, end_date=end_date)
        xr = nxr.get(update="nxr.csv", revise_rows=6, save="nxr.csv")
        output = output.divide(xr[start_date:end_date].mean()[3])
        columns.set_metadata(output, currency="USD")
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

    return output


def labor_mkt(seas_adj: Union[str, None] = "trend"):
    """Get labor market data.

    Allow choosing seasonal adjustment.

    Parameters
    ----------
    seas_adj :
        Allowed strings: 'trend' or 'seas'.

    Returns
    -------
    dataframe : Pandas dataframe

    """
    data = labor.get(update="labor.csv", revise_rows=6,
                     save="labor.csv", force_update=False)
    output = data

    if seas_adj in ["trend", "seas"]:
        trend, seasadj = seasonal.decompose(data, trading=True, outlier=True)
        if seas_adj == "trend":
            output = pd.concat([data, trend], axis=1)
        elif seas_adj == "seas":
            output = pd.concat([data, seasadj], axis=1)

    return output


def nat_accounts(supply: bool = True, real: bool = True, index: bool = False,
                 seas_adj: bool = True, usd: bool = False, cum: int = 1,
                 cust_seas_adj: Union[str, None] = None,
                 variation: Union[str, None] = None):
    """Get national accounts data.

    Attempt to find one of the available data tables with the selected
    combination of parameters (`supply`, `real`. `index` and `seas_adj`).

    Parameters
    ----------
    supply : bool (default is True)
        Supply or demand side.
    real : bool (default is True)
        Constant or current.
    index : bool (default is False)
        Base 100 index or not.
    seas_adj : bool (default is True)
        Seasonally adjusted or not.
    usd : bool (default is False)
        If True, convert to USD.
    cum : int (default is 1)
        How many periods to accumulate for rolling sums.
    cust_seas_adj : str or None (default is None)
        Allowed strings: 'trend' or 'seas'.
    variation : str or None (default is None)
        Type of percentage change to calculate. Can be 'last', 'inter' or
        'annual'.

    Returns
    -------
    dataframe : Pandas dataframe

    Raises
    ------
    KeyError
        If the combined parameters do not correspond to an available table.

    """
    data = national_accounts.get(update=True, revise_rows=4,
                                 save=True, force_update=False)

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

    table = f"na_{'_'.join(search_terms)}"

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

    return output
