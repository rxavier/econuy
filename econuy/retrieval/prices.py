import datetime as dt
import warnings
from os import PathLike
from typing import Union
from urllib.error import URLError, HTTPError
from io import BytesIO
from pathlib import Path
from zipfile import BadZipFile

import numpy as np
import pandas as pd
import requests
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from scipy.stats import stats
from scipy.stats.mstats_basic import winsorize
from sqlalchemy.engine.base import Connection, Engine

from econuy import transform
from econuy.utils import ops, metadata, get_project_root
from econuy.utils.lstrings import urls, cpi_details


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def cpi(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "cpi",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get CPI data.

    Parameters
    ----------
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    name : str, default 'cpi'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly CPI index : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    try:
        cpi = pd.read_excel(urls["cpi"]["dl"]["main"], engine="openpyxl",
                            skiprows=7, usecols="A:B",
                            index_col=0).dropna()
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files",
                               "ine_certs.pem")
            r = requests.get(urls["cpi"]["dl"]["main"],
                             verify=certificate)
            cpi = pd.read_excel(BytesIO(r.content),
                                skiprows=7, usecols="A:B",
                                index_col=0).dropna()
        else:
            raise err
    cpi.columns = ["Índice de precios al consumo"]
    cpi.rename_axis(None, inplace=True)
    cpi.index = cpi.index + MonthEnd(1)

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        cpi = ops._revise(new_data=cpi, prev_data=previous_data,
                          revise_rows=revise_rows)

    cpi = cpi.apply(pd.to_numeric, errors="coerce")
    metadata._set(cpi, area="Precios", currency="-",
                  inf_adj="No", unit="2010-10=100", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=cpi, name=name, index_label=index_label)

    return cpi


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=30,
)
def nxr_monthly(update_loc: Union[str, PathLike,
                                  Engine, Connection, None] = None,
                revise_rows: Union[str, int] = "nodup",
                save_loc: Union[str, PathLike,
                                Engine, Connection, None] = None,
                name: str = "nxr_monthly",
                index_label: str = "index",
                only_get: bool = False) -> pd.DataFrame:
    """Get monthly nominal exchange rate data.

    Parameters
    ----------
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    name : str, default 'nxr_monthly'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly nominal exchange rates : pd.DataFrame
        Sell rate, monthly average and end of period.

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output
    try:
        nxr_raw = pd.read_excel(urls["nxr_monthly"]["dl"]["main"], engine="openpyxl",
                                skiprows=4, index_col=0, usecols="A,C,F")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files",
                               "ine_certs.pem")
            r = requests.get(urls["nxr_monthly"]["dl"]["main"],
                             verify=certificate)
            nxr_raw = pd.read_excel(BytesIO(r.content),
                                    skiprows=4, index_col=0, usecols="A,C,F")
        else:
            raise err
    nxr = nxr_raw.dropna(how="any", axis=0)
    nxr.columns = ["Tipo de cambio venta, fin de período",
                   "Tipo de cambio venta, promedio"]
    nxr.index = nxr.index + MonthEnd(1)
    nxr = nxr.apply(pd.to_numeric, errors="coerce")

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        nxr = ops._revise(new_data=nxr, prev_data=previous_data,
                          revise_rows=revise_rows)

    metadata._set(nxr, area="Precios", currency="UYU/USD",
                  inf_adj="No", unit="-", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=nxr, name=name, index_label=index_label)

    return nxr


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=60,
)
def nxr_daily(update_loc: Union[str, PathLike,
                                Engine, Connection, None] = None,
              save_loc: Union[str, PathLike,
                              Engine, Connection, None] = None,
              name: str = "nxr_daily",
              index_label: str = "index",
              only_get: bool = False) -> pd.DataFrame:
    """Get daily nominal exchange rate data.

    Parameters
    ----------
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    name : str, default 'nxr_daily'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly nominal exchange rates : pd.DataFrame
        Sell rate, monthly average and end of period.

    """
    if only_get is True and update_loc is not None:
        return ops._io(operation="update", data_loc=update_loc,
                       name=name, index_label=index_label)

    start_date = dt.datetime(1999, 12, 31)

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        metadata._set(previous_data)
        try:
            start_date = previous_data.index[len(previous_data) - 1]
        except IndexError:
            pass

    today = dt.datetime.now() - dt.timedelta(days=1)
    runs = (today - start_date).days // 30
    data = []
    base_url = urls['nxr_daily']['dl']['main']
    if runs > 0:
        for i in range(1, runs + 1):
            from_ = (start_date + dt.timedelta(days=1)).strftime('%d/%m/%Y')
            to_ = (start_date + dt.timedelta(days=30)).strftime('%d/%m/%Y')
            dates = f"%22FechaDesde%22:%22{from_}%22,%22FechaHasta%22:%22{to_}"
            url = f"{base_url}{dates}%22,%22Grupo%22:%222%22}}" + "}"
            try:
                data.append(pd.read_excel(url, engine="openpyxl"))
                start_date = dt.datetime.strptime(to_, '%d/%m/%Y')
            except TypeError:
                pass
    from_ = (start_date + dt.timedelta(days=1)).strftime('%d/%m/%Y')
    to_ = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%d/%m/%Y')
    dates = f"%22FechaDesde%22:%22{from_}%22,%22FechaHasta%22:%22{to_}"
    url = f"{base_url}{dates}%22,%22Grupo%22:%222%22}}" + "}"
    try:
        data.append(pd.read_excel(url, engine="openpyxl"))
    except (TypeError, BadZipFile):
        pass
    try:
        output = pd.concat(data, axis=0)
        output = output.pivot(index="Fecha", columns="Moneda",
                              values="Venta").rename_axis(None)
        output.index = pd.to_datetime(output.index, format="%d/%m/%Y",
                                      errors="coerce")
        output.sort_index(inplace=True)
        output.replace(",", ".", regex=True, inplace=True)
        output.columns = ["Tipo de cambio US$, Cable"]
        output = output.apply(pd.to_numeric, errors="coerce")

        metadata._set(output, area="Precios", currency="UYU/USD",
                      inf_adj="No", unit="-", seas_adj="NSA",
                      ts_type="-", cumperiods=1)
        output.columns.set_levels(["-"], level=2, inplace=True)

        if update_loc is not None:
            output = pd.concat([previous_data, output])

        if save_loc is not None:
            ops._io(operation="save", data_loc=save_loc,
                    data=output, name=name, index_label=index_label)

    except ValueError as e:
        if str(e) == "No objects to concatenate":
            return previous_data

    return output


# The `_contains_nan` function needs to be monkey-patched to avoid an error
# when checking whether a Series is True
def _new_contains_nan(a, nan_policy='propagate'):
    policies = ['propagate', 'raise', 'omit']
    if nan_policy not in policies:
        raise ValueError("nan_policy must be one of {%s}" %
                         ', '.join("'%s'" % s for s in policies))
    try:
        with np.errstate(invalid='ignore'):
            # This [0] gets the value instead of the array, fixing the error
            contains_nan = np.isnan(np.sum(a))[0]
    except TypeError:
        try:
            contains_nan = np.nan in set(a.ravel())
        except TypeError:
            contains_nan = False
            nan_policy = 'omit'
            warnings.warn("The input array could not be properly checked for "
                          "nan values. nan values will be ignored.",
                          RuntimeWarning)

    if contains_nan and nan_policy == 'raise':
        raise ValueError("The input contains nan values")

    return contains_nan, nan_policy


stats._contains_nan = _new_contains_nan


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def cpi_measures(update_loc: Union[str, PathLike,
                                   Engine, Connection, None] = None,
                 revise_rows: Union[str, int] = "nodup",
                 save_loc: Union[str, PathLike, Engine,
                                 Connection, None] = None,
                 name: str = "cpi_measures", index_label: str = "index",
                 only_get: bool = False) -> pd.DataFrame:
    """
    Get core CPI, Winsorized CPI, tradabe CPI, non-tradable CPI and residual
    CPI.

    Parameters
    ----------
    update_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                  default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_loc : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
                default None
        Either Path or path-like string pointing to a directory where to save
        the CSV, SQL Alchemy connection or engine object, or ``None``,
        don't save.
    name : str, default 'cpi_measures'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly CPI measures : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output
    try:
        xls_10_14 = pd.ExcelFile(urls["cpi_measures"]["dl"]["2010-14"])
        xls_15 = pd.ExcelFile(urls["cpi_measures"]["dl"]["2015-"])
        prod_97 = (pd.read_excel(urls["cpi_measures"]["dl"]["1997"],
                                 skiprows=5, engine="openpyxl").dropna(how="any")
                   .set_index(
            "Rubros, Agrupaciones, Subrubros, Familias y Artículos")
                   .T)
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files",
                               "ine_certs.pem")
            r = requests.get(urls["cpi_measures"]["dl"]["2010-14"],
                             verify=certificate)
            xls_10_14 = pd.ExcelFile(BytesIO(r.content))
            r = requests.get(urls["cpi_measures"]["dl"]["2015-"],
                             verify=certificate)
            xls_15 = pd.ExcelFile(BytesIO(r.content))
            r = requests.get(urls["cpi_measures"]["dl"]["1997"],
                             verify=certificate)
            prod_97 = (pd.read_excel(BytesIO(r.content),
                                     skiprows=5).dropna(how="any")
                       .set_index(
                "Rubros, Agrupaciones, Subrubros, Familias y Artículos")
                       .T)
        else:
            raise err
    weights_97 = (pd.read_excel(urls["cpi_measures"]["dl"]["1997_weights"],
                                index_col=0, engine="openpyxl")
                  .drop_duplicates(subset="Descripción", keep="first"))
    weights = pd.read_excel(xls_10_14, sheet_name=xls_10_14.sheet_names[0],
                            usecols="A:C", skiprows=13,
                            index_col=0).dropna(how="any")
    weights.columns = ["Item", "Weight"]
    weights_8 = weights.loc[weights.index.str.len() == 8]

    sheets = []
    for excel_file in [xls_10_14, xls_15]:
        for sheet in excel_file.sheet_names:
            raw = pd.read_excel(excel_file, sheet_name=sheet, usecols="D:IN",
                                skiprows=8).dropna(how="all")
            proc = raw.loc[:, raw.columns.str.
                                  contains("Indice|Índice")].dropna(how="all")
            sheets.append(proc.T)
    complete_10 = pd.concat(sheets)
    complete_10 = complete_10.iloc[:, 1:]
    complete_10.columns = [weights["Item"], weights.index]
    complete_10.index = pd.date_range(start="2010-12-31",
                                      periods=len(complete_10), freq="M")
    diff_8 = complete_10.loc[:,
             complete_10.columns.get_level_values(
                 level=1).str.len()
             == 8].pct_change()
    win = pd.DataFrame(winsorize(diff_8, limits=(0.05, 0.05), axis=1))
    win.index = diff_8.index
    win.columns = diff_8.columns.get_level_values(level=1)
    cpi_win = win.mul(weights_8.loc[:, "Weight"].T)
    cpi_win = cpi_win.sum(axis=1).add(1).cumprod().mul(100)

    weights_97["Weight"] = (weights_97["Rubro"]
                            .fillna(
        weights_97["Agrupación, subrubro, familia"])
                            .fillna(weights_97["Artículo"])
                            .drop(columns=["Rubro",
                                           "Agrupación, subrubro, familia",
                                           "Artículo"]))
    prod_97 = prod_97.loc[:, list(cpi_details["1997_base"].keys())]
    prod_97.index = pd.date_range(start="1997-03-31",
                                  periods=len(prod_97), freq="M")
    weights_97 = (weights_97[weights_97["Descripción"]
                  .isin(cpi_details["1997_weights"])]
                  .set_index("Descripción")
                  .drop(columns=["Rubro", "Agrupación, subrubro, "
                                          "familia", "Artículo"])).div(100)
    weights_97.index = prod_97.columns
    prod_10 = complete_10.loc[:, list(cpi_details["2010_base"].keys())]
    prod_10 = prod_10.loc[:, ~prod_10.columns.get_level_values(level=0)
        .duplicated()]
    prod_10.columns = prod_10.columns.get_level_values(level=0)
    weights_10 = (weights.loc[weights["Item"]
                  .isin(list(cpi_details["2010_base"].keys()))]
                  .drop_duplicates(subset="Item",
                                   keep="first")).set_index("Item")
    items = []
    weights = []
    for item, weight, details in zip([prod_10, prod_97],
                                     [weights_10, weights_97],
                                     ["2010_base", "1997_base"]):
        for tradable in [True, False]:
            items.append(
                item.loc[:, [k for k, v in cpi_details[details].items()
                             if v["Tradable"] is tradable]])
            aux = weight.loc[[k for k, v in cpi_details[details].items()
                              if v["Tradable"] is tradable]]
            weights.append(aux.div(aux.sum()))
        for core in [True, False]:
            items.append(
                item.loc[:, [k for k, v in cpi_details[details].items()
                             if v["Core"] is core]])
            aux = weight.loc[[k for k, v in cpi_details[details].items()
                              if v["Core"] is core]]
            weights.append(aux.div(aux.sum()))

    intermediate = []
    for item, weight in zip(items, weights):
        intermediate.append(item.mul(weight.squeeze()).sum(1))

    output = []
    for x, y in zip(intermediate[:4], intermediate[4:]):
        aux = pd.concat([y.pct_change().loc[y.index < "2011-01-01"],
                         x.pct_change().loc[x.index > "2011-01-01"]])
        output.append(aux.fillna(0).add(1).cumprod().mul(100))

    cpi_re = cpi(update_loc=update_loc, save_loc=save_loc, only_get=True)
    cpi_re = cpi_re.loc[cpi_re.index >= "1997-03-31"]
    output = pd.concat([cpi_re] + output + [cpi_win], axis=1)
    output.columns = ["Índice de precios al consumo: total",
                      "Índice de precios al consumo: transables",
                      "Índice de precios al consumo: no transables",
                      "Índice de precios al consumo: subyacente",
                      "Índice de precios al consumo: residual",
                      "Índice de precios al consumo: Winsorized 0.05"]
    output = output.apply(pd.to_numeric, errors="coerce")
    metadata._set(output, area="Precios y salarios", currency="-",
                  inf_adj="No", unit="2010-12=100", seas_adj="NSA",
                  ts_type="-", cumperiods=1)
    output = transform.rebase(output, start_date="2010-12-01",
                              end_date="2010-12-31")

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output
