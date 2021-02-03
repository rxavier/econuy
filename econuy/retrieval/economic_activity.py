import datetime as dt
import re
import tempfile
from pathlib import Path
from io import BytesIO
from os import PathLike, listdir, path
from typing import Union, Dict
from urllib.error import HTTPError, URLError

import pandas as pd
import patoolib
import requests
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from sqlalchemy.engine.base import Engine, Connection

from econuy import transform
from econuy.utils import ops, metadata, get_project_root
from econuy.utils.lstrings import na_metadata, urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def national_accounts(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "naccounts",
        index_label: str = "index",
        only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get national accounts data.

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
    name : str, default 'naccounts'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Quarterly national accounts : Dict[str, pd.DataFrame]
        Each dataframe corresponds to a national accounts table.

    """
    if only_get is True and update_loc is not None:
        output = {}
        for filename, meta in na_metadata.items():
            data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{filename}", index_label=index_label
            )
            output.update({filename: data})
        if all(not value.equals(pd.DataFrame()) for value in output.values()):
            return output

    parsed_excels = {}
    for filename, meta in na_metadata.items():
        raw = pd.read_excel(meta["url"], skiprows=9, nrows=meta["Rows"])
        proc = (raw.drop(columns=["Unnamed: 0"]).
                dropna(axis=0, how="all").dropna(axis=1, how="all"))
        proc = proc.transpose()
        proc.columns = meta["Colnames"]
        proc.drop(["Unnamed: 1"], inplace=True)
        _fix_dates(proc)
        if meta["Unit"] == "Miles":
            proc = proc.divide(1000)
            unit_ = "Millones"
        else:
            unit_ = meta["Unit"]

        if update_loc is not None:
            previous_data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{filename}", index_label=index_label
            )
            proc = ops._revise(new_data=proc, prev_data=previous_data,
                               revise_rows=revise_rows)
        proc = proc.apply(pd.to_numeric, errors="coerce")

        metadata._set(proc, area="Actividad económica", currency="UYU",
                      inf_adj=meta["Inf. Adj."],
                      unit=unit_,
                      seas_adj=meta["Seas"], ts_type="Flujo",
                      cumperiods=1)

        if save_loc is not None:
            ops._io(
                operation="save", data_loc=save_loc, data=proc,
                name=f"{name}_{filename}", index_label=index_label
            )

        parsed_excels.update({filename: proc})

    return parsed_excels


def _fix_dates(df):
    """Cleanup dates inplace in BCU national accounts files."""
    df.index = df.index.str.replace("*", "")
    df.index = df.index.str.replace(r"\bI \b", "3-", regex=True)
    df.index = df.index.str.replace(r"\bII \b", "6-", regex=True)
    df.index = df.index.str.replace(r"\bIII \b", "9-", regex=True)
    df.index = df.index.str.replace(r"\bIV \b", "12-", regex=True)
    df.index = pd.to_datetime(df.index, format="%m-%Y") + MonthEnd(1)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def _lin_gdp(update_loc: Union[str, PathLike, Engine,
                               Connection, None] = None,
             save_loc: Union[str, PathLike, Engine,
                             Connection, None] = None,
             name: str = "lin_gdp",
             index_label: str = "index",
             only_get: bool = True,
             only_get_na: bool = True):
    """Get nominal GDP data in UYU and USD with forecasts.

    Update nominal GDP data for use in the `transform.convert_gdp()` function.
    Get IMF forecasts for year of last available data point and the next
    year (for example, if the last period available at the BCU website is
    september 2019, fetch forecasts for 2019 and 2020).

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
    name : str, default 'lin_gdp'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.
    only_get_na : bool, default True
        If True, don't download national accounts data,
        retrieve what is available from ``update_loc``.

    Returns
    -------
    output : Pandas dataframe
        Quarterly GDP in UYU and USD with 1 year forecasts.

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    data_uyu = national_accounts(update_loc=update_loc, only_get=only_get_na)[
        "gdp_cur_nsa"]
    data_uyu = transform.rolling(data_uyu, window=4, operation="sum")
    data_usd = transform.convert_usd(data_uyu,
                                     update_loc=update_loc,
                                     only_get=only_get)

    data = [data_uyu, data_usd]
    last_year = data_uyu.index.max().year
    if data_uyu.index.max().month == 12:
        last_year += 1

    results = []
    for table, gdp in zip(["NGDP", "NGDPD"], data):
        table_url = (f"https://www.imf.org/en/Publications/WEO/weo-database/"
                     f"2020/October/weo-report?c=298,&s={table},&sy="
                     f"{last_year - 1}&ey={last_year + 1}&ssm=0&scsm=1&scc=0&"
                     f"ssd=1&ssc=0&sic=0&sort=country&ds=.&br=1")
        imf_data = pd.to_numeric(pd.read_html(table_url)[0].iloc[0, [5, 6, 7]])
        imf_data = imf_data.reset_index(drop=True)
        fcast = (gdp.loc[[dt.datetime(last_year - 1, 12, 31)]].
                 multiply(imf_data.iloc[1]).divide(imf_data.iloc[0]))
        fcast = fcast.rename(index={dt.datetime(last_year - 1, 12, 31):
                                        dt.datetime(last_year, 12, 31)})
        next_fcast = (gdp.loc[[dt.datetime(last_year - 1, 12, 31)]].
                      multiply(imf_data.iloc[2]).divide(imf_data.iloc[0]))
        next_fcast = next_fcast.rename(
            index={dt.datetime(last_year - 1, 12, 31):
                       dt.datetime(last_year + 1, 12, 31)}
        )
        fcast = fcast.append(next_fcast)
        gdp = gdp.append(fcast)
        results.append(gdp)

    output = pd.concat(results, axis=1)
    output = output.resample("Q-DEC").interpolate("linear").dropna(how="all")
    metadata._modify_multiindex(output, levels=[0],
                                new_arrays=[["PBI UYU", "PBI USD"]])

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def industrial_production(update_loc: Union[str, PathLike,
                                            Engine, Connection, None] = None,
                          revise_rows: Union[str, int] = "nodup",
                          save_loc: Union[str, PathLike,
                                          Engine, Connection, None] = None,
                          name: str = "industrial_production",
                          index_label: str = "index",
                          only_get: bool = False) -> pd.DataFrame:
    """Get industrial production data.

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
    name : str, default 'industrial_production'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly industrial production index : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output
    try:
        raw = pd.read_excel(urls["industrial_production"]["dl"]["main"],
                            skiprows=4, usecols="B:EM", engine="openpyxl")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files",
                               "ine_certs.pem")
            r = requests.get(urls["industrial_production"]["dl"]["main"],
                             verify=certificate)
            raw = pd.read_excel(BytesIO(r.content),
                                skiprows=4, usecols="B:EM")
        else:
            raise err
    proc = raw.dropna(how="any", subset=["Mes"]).dropna(thresh=100, axis=1)
    output = proc[~proc["Mes"].str.contains("PROM|Prom", 
                                            regex=True)].drop("Mes", axis=1)
    output.index = pd.date_range(start="2002-01-31", freq="M",
                                 periods=len(output))
    output.columns = (["Industrias manufactureras",
                       "Industrias manufactureras sin refinería"]
                      + [col for col in output.columns
                         if col not in ["D", "D sin refinería"]])

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    output = output.apply(pd.to_numeric, errors="coerce")
    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="2006=100", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


def core_industrial(update_loc: Union[str, PathLike, Engine,
                                      Connection, None] = None,
                    save_loc: Union[str, PathLike, Engine,
                                    Connection, None] = None,
                    name: str = "core_industrial",
                    index_label: str = "index",
                    only_get: bool = True) -> pd.DataFrame:
    """
    Get total industrial production, industrial production excluding oil
    refinery and core industrial production.

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
    name : str, default 'core_industrial'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Measures of industrial production : pd.DataFrame

    """
    data = industrial_production(update_loc=update_loc, save_loc=save_loc,
                                 only_get=only_get)
    try:
        weights = pd.read_excel(urls["core_industrial"]["dl"]["weights"],
                                skiprows=3, engine="openpyxl").dropna(how="all")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files",
                               "ine_certs.pem")
            r = requests.get(urls["core_industrial"]["dl"]["weights"],
                             verify=certificate)
            weights = pd.read_excel(BytesIO(r.content),
                                    skiprows=3).dropna(how="all")
        else:
            raise err
    weights = weights.rename(columns={"Unnamed: 5": "Pond. división",
                                      "Unnamed: 6": "Pond. agrupación",
                                      "Unnamed: 7": "Pond. clase"})
    other_foods = (
            weights.loc[weights["clase"] == 1549]["Pond. clase"].values[0]
            * weights.loc[(weights["agrupacion"] == 154) &
                          (weights["clase"] == 0)][
                "Pond. agrupación"].values[0]
            * weights.loc[(weights["division"] == 15) &
                          (weights["agrupacion"] == 0)][
                "Pond. división"].values[0]
            / 1000000)
    pulp = (weights.loc[weights["clase"] == 2101]["Pond. clase"].values[0]
            * weights.loc[(weights["division"] == 21) &
                          (weights["agrupacion"] == 0)][
                "Pond. división"].values[0]
            / 10000)
    output = data.loc[:, ["Industrias manufactureras",
                          "Industrias manufactureras sin refinería"]]
    try:
        exclude = (data.loc[:, "1549"] * other_foods
                   + data.loc[:, "2101"] * pulp)
    except KeyError:
        exclude = (data.loc[:, 1549] * other_foods
                   + data.loc[:, 2101] * pulp)
    core = data["Industrias manufactureras sin refinería"] - exclude
    core = pd.concat([core], keys=["Núcleo industrial"],
                     names=["Indicador"], axis=1)
    output = pd.concat([output, core], axis=1)
    output = transform.rebase(output, start_date="2006-01-01",
                              end_date="2006-12-31")

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def cattle(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "cattle",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get weekly cattle slaughter data.

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
    name : str, default 'cattle'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Weekly cattle slaughter : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    temp = tempfile.NamedTemporaryFile(suffix=".xlsx").name
    with open(temp, "wb") as f:
        r = requests.get(urls["cattle"]["dl"]["main"])
        f.write(r.content)
    output = pd.read_excel(temp, skiprows=8, usecols="A,C:H", index_col=0,
                           engine="openpyxl")

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="Cabezas", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def milk(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "milk",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get monthly milk production in farms data.

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
    name : str, default 'milk'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monhtly milk production in farms : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    r = requests.get(urls["milk"]["dl"]["main"])
    soup = BeautifulSoup(r.content, features="lxml")
    link = soup.find_all(href=re.compile(".xls"))[0]
    raw = pd.read_excel(link["href"], skiprows=11, skipfooter=4)
    output = raw.iloc[:, 2:].drop(0, axis=0)
    output = pd.melt(output, id_vars="Año/ Mes")[["value"]].dropna()
    output.index = pd.date_range(start="2002-01-31", freq="M",
                                 periods=len(output))
    output = output.apply(pd.to_numeric)
    output.columns = ["Remisión de leche a planta"]

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="Miles de litros", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def cement(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "cement",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get monthly cement sales data.

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
    name : str, default 'cement'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly cement sales : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    output = pd.read_excel(urls["cement"]["dl"]["main"], skiprows=2,
                           usecols="B:E", index_col=0, skipfooter=1,
                           engine="openpyxl")
    output.index = output.index + MonthEnd(0)
    output.columns = ["Exportaciones", "Mercado interno", "Total"]

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="Toneladas", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def diesel(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "diesel",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """
    Get diesel sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

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
    name : str, default 'diesel'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly diesel dales : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls["diesel"]["dl"]["main"])
        soup = BeautifulSoup(r.content, features="lxml")
        rar_url = soup.find_all(href=re.compile("gas%20oil"))[0]
        f.write(requests.get(rar_url["href"]).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(path_temp, sheet_name="vta gas oil por depto",
                            skiprows=2, usecols="C:W")
        raw.index = pd.date_range(start="2004-01-31", freq="M",
                                  periods=len(raw))
        raw.columns = list(raw.columns.str.replace("\n", " "))[:-1] + ["Total"]
        output = raw

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="m3", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def gasoline(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "gasoline",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """
    Get gasoline sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

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
    name : str, default 'gasoline'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly gasoline dales : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls["gasoline"]["dl"]["main"])
        soup = BeautifulSoup(r.content, features="lxml")
        rar_url = soup.find_all(href=re.compile("gasolina"))[0]
        f.write(requests.get(rar_url["href"]).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(path_temp, sheet_name="vta gasolinas por depto",
                            skiprows=2, usecols="C:W")
        raw.index = pd.date_range(start="2004-01-31", freq="M",
                                  periods=len(raw))
        raw.columns = list(raw.columns.str.replace("\n", " "))[:-1] + ["Total"]
        output = raw

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="m3", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def electricity(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "electricity",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """
    Get electricity sales by sector data.

    This retrieval function requires the unrar binaries to be found in your
    system.

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
    name : str, default 'electricity'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly electricity dales : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls["electricity"]["dl"]["main"])
        soup = BeautifulSoup(r.content, features="lxml")
        rar_url = soup.find_all(href=re.compile("Facturaci[%A-z0-9]+sector"))[
            0]
        f.write(requests.get(rar_url["href"]).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(path_temp, sheet_name="fact ee",
                            skiprows=2, usecols="C:J")
        raw.index = pd.date_range(start="2000-01-31", freq="M",
                                  periods=len(raw))
        raw.columns = raw.columns.str.capitalize()
        output = raw

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="MWh", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output
