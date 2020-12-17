import datetime as dt
import os
import tempfile
import zipfile
from random import randint
from io import BytesIO
from os import PathLike, path
from pathlib import Path
from typing import Union
from urllib.error import HTTPError, URLError

import pandas as pd
import requests
from dotenv import load_dotenv
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from sqlalchemy.engine.base import Engine, Connection

from econuy.transform import decompose, base_index
from econuy.utils import metadata, ops, get_project_root
from econuy.utils.lstrings import urls, investing_headers


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def gdp(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "global_gdp",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get seasonally adjusted real quarterly GDP for select countries.

    Countries/aggregates are US, EU-27, Japan and China.

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
    name : str, default 'global_gdp'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Quarterly real GDP in seasonally adjusted terms : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    chn_y = dt.datetime.now().year + 1
    chn_r = requests.get(f"{urls['global_gdp']['dl']['chn_oecd']}{chn_y}-Q4")
    chn_json = chn_r.json()
    chn_datasets = []
    for dataset, start in zip(["0", "1"], ["2011-03-31", "1993-03-31"]):
        raw = chn_json["dataSets"][0]["series"][f"0:0:{dataset}:0"][
            "observations"]
        values = [x[0] for x in raw.values()]
        df = pd.DataFrame(data=values,
                          index=pd.date_range(start=start, freq="Q-DEC",
                                              periods=len(values)),
                          columns=["China"])
        chn_datasets.append(df)
    chn_qoq = chn_datasets[0]
    chn_yoy = chn_datasets[1]
    chn_obs = pd.read_excel(urls["global_gdp"]["dl"]["chn_obs"], index_col=0,
                            engine="openpyxl").dropna(how="all", axis=1).dropna(how="all", axis=0)
    chn_obs = chn_obs.loc[(chn_obs.index > "2011-01-01")
                          & (chn_obs.index < "2016-01-01")]
    chn_yoy["volume"] = chn_obs
    for row in reversed(range(len(chn_yoy.loc[chn_yoy.index < "2011-01-01"]))):
        if pd.isna(chn_yoy.iloc[row, 1]):
            chn_yoy.iloc[row, 1] = (chn_yoy.iloc[row + 4, 1]
                                    / (1 + chn_yoy.iloc[row + 4, 0] / 100))
    chn_sa = decompose(chn_yoy[["volume"]].loc[chn_yoy.index < "2016-01-01"],
                       flavor="seas", method="x13")
    chn_sa = pd.concat([chn_sa, chn_qoq], axis=1)
    for row in range(len(chn_sa)):
        if not pd.isna(chn_sa.iloc[row, 1]):
            chn_sa.iloc[row, 0] = (chn_sa.iloc[row - 1, 0]
                                   * (1 + chn_sa.iloc[row, 1] / 100))
    chn = chn_sa.iloc[:, [0]].div(10)

    gdps = []
    load_dotenv(Path(get_project_root(), ".env"))
    fred_api_key = os.environ.get("FRED_API_KEY")
    for series in ["GDPC1", "CLVMNACSCAB1GQEU272020", "JPNRGDPEXP"]:
        r = requests.get(f"{urls['global_gdp']['dl']['fred']}{series}&api_key="
                         f"{fred_api_key}&file_type=json")
        aux = pd.DataFrame.from_records(r.json()["observations"])
        aux = aux[["date", "value"]].set_index("date")
        aux.index = pd.to_datetime(aux.index)
        aux.index = aux.index.shift(3, freq="M") + MonthEnd(0)
        aux.columns = [series]
        aux = aux.apply(pd.to_numeric, errors="coerce")
        if series == "GDPC1":
            aux = aux.div(4)
        elif series == "CLVMNACSCAB1GQEU272020":
            aux = aux.div(1000)
        gdps.append(aux)
    gdps = pd.concat(gdps, axis=1)

    output = pd.concat([gdps, chn], axis=1)
    output.columns = ["Estados Unidos", "Unión Europea", "Japón", "China"]

    if update_loc is not None:
        previous_data = ops._io(operation="update", data_loc=update_loc,
                                name=name, index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Global", currency="USD",
                  inf_adj="Const.", unit="Miles de millones", seas_adj="SA",
                  ts_type="Flujo", cumperiods=1)
    metadata._modify_multiindex(output, levels=[3],
                                new_arrays=[["USD", "EUR", "JPY", "CNY"]])

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def stocks(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
           revise_rows: Union[str, int] = "nodup",
           save_loc: Union[str, PathLike, Engine, Connection, None] = None,
           name: str = "global_stocks",
           index_label: str = "index",
           only_get: bool = False) -> pd.DataFrame:
    """Get stock market index data.

    Indexes selected are S&P 500, Euronext 100, Nikkei 225 and Shanghai
    Composite.

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
    name : str, default 'global_stocks'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Daily stock market index in USD : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    yahoo = []
    for series in ["spy", "n100","nikkei", "sse"]:
        aux = pd.read_csv(urls["global_stocks"]["dl"][series],
                          index_col=0, usecols=[0, 4], parse_dates=True)
        aux.columns = [series]
        yahoo.append(aux)
    output = pd.concat(yahoo, axis=1).interpolate(method="linear",
                                                  limit_area="inside")
    output.columns = ["S&P 500", "Euronext 100", "Nikkei 225",
                      "Shanghai Stock Exchange Composite"]
    output = base_index(output, start_date="2019-01-02")

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Global", currency="USD",
                  inf_adj="No", seas_adj="NSA",
                  ts_type="-", cumperiods=1)
    metadata._modify_multiindex(output, levels=[3],
                                new_arrays=[["USD", "EUR", "JPY", "CNY"]])

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def policy_rates(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "global_policy_rates",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get central bank policy interest rates data.

    Countries/aggregates selected are US, Euro Area, Japan and China.

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
    name : str, default 'global_policy_rates'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Daily policy interest rates : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    r = requests.get(urls["global_policy_rates"]["dl"]["main"])
    temp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(BytesIO(r.content), "r") as f:
        f.extractall(path=temp_dir.name)
        path_temp = path.join(temp_dir.name,
                              "WEBSTATS_CBPOL_D_DATAFLOW_csv_row.csv")
        raw = pd.read_csv(path_temp, usecols=[0, 7, 19, 36, 37], index_col=0,
                          header=2, parse_dates=True).dropna(how="all")
    output = (raw.apply(pd.to_numeric, errors="coerce")
              .interpolate(method="linear", limit_area="inside"))
    output.columns = ["China", "Japón", "Estados Unidos", "Eurozona"]
    output = output[["Estados Unidos", "Eurozona", "Japón", "China"]]

    if update_loc is not None:
        previous_data = ops._io(operation="update", data_loc=update_loc,
                                name=name, index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Global", currency="USD",
                  inf_adj="No", seas_adj="NSA", unit="Tasa",
                  ts_type="-", cumperiods=1)
    metadata._modify_multiindex(output, levels=[3],
                                new_arrays=[["USD", "EUR", "JPY", "CNY"]])

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def long_rates(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "global_long_rates",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get 10-year government bonds interest rates.

    Countries/aggregates selected are US, Germany, France, Italy, Spain
    United Kingdom, Japan and China.

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
    name : str, default 'global_long_rates'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Daily 10-year government bonds interest rates : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    bonds = []
    load_dotenv(Path(get_project_root(), ".env"))
    fred_api_key = os.environ.get("FRED_API_KEY")
    r = requests.get(f"{urls['global_long_rates']['dl']['fred']}DGS10&api_key="
                     f"{fred_api_key}&file_type=json")
    us = pd.DataFrame.from_records(r.json()["observations"])
    us = us[["date", "value"]].set_index("date")
    us.index = pd.to_datetime(us.index)
    us.columns = ["United States"]
    bonds.append(us.apply(pd.to_numeric, errors="coerce").dropna())

    for country, sid in zip(["Germany", "France", "Italy", "Spain",
                            "United Kingdom", "Japan", "China"],
                           ["23693", "23778", "23738", "23806",
                            "23673", "23901", "29227"]):
        end_date_dt = dt.datetime(2000, 1, 1)
        start_date_dt = dt.datetime(2000, 1, 1)
        aux = []
        while end_date_dt < dt.datetime.now():
            end_date_dt = start_date_dt + dt.timedelta(days=5000)
            params = {
                "curr_id": sid,
                "smlID": str(randint(1000000, 99999999)),
                "header": f"{country} 10-Year Bond Yield Historical Data",
                "st_date": start_date_dt.strftime("%m/%d/%Y"),
                "end_date": end_date_dt.strftime("%m/%d/%Y"),
                "interval_sec": "Daily",
                "sort_col": "date",
                "sort_ord": "DESC",
                "action": "historical_data"
            }
            r = requests.post(urls["global_long_rates"]["dl"]["main"],
                              headers=investing_headers, data=params)
            aux.append(pd.read_html(r.content, match="Price",
                                    index_col=0, parse_dates=True)[0])
            start_date_dt = end_date_dt + dt.timedelta(days=1)
        aux = pd.concat(aux, axis=0)[["Price"]].sort_index()
        aux.columns = [country]
        bonds.append(aux)

    output = bonds[0].join(bonds[1:], how="left")
    output = output.interpolate(method="linear", limit_area="inside")
    output.columns = ["Estados Unidos", "Alemania", "Francia", "Italia",
                      "España", "Reino Unido", "Japón", "China"]

    if update_loc is not None:
        previous_data = ops._io(operation="update", data_loc=update_loc,
                                name=name, index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Global", currency="USD",
                  inf_adj="No", seas_adj="NSA", unit="Tasa",
                  ts_type="-", cumperiods=1)
    metadata._modify_multiindex(output, levels=[3],
                                new_arrays=[["USD", "EUR", "EUR", "EUR",
                                             "EUR", "GBP", "JPY", "CNY"]])

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=12,
    retry_window_after_first_call_in_seconds=60,
)
def nxr(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "global_nxr",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get currencies data.

    Selected currencies are the US dollar index, USDEUR, USDJPY and USDCNY.

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
    name : str, default 'global_nxr'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Daily currencies : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    output = []
    for series in ["dollar", "eur", "jpy", "cny"]:
        aux = pd.read_csv(urls["global_nxr"]["dl"][series],
                          index_col=0, usecols=[0, 4], parse_dates=True)
        aux.columns = [series]
        if series == "dollar":
            aux.dropna(inplace=True)
        output.append(aux)
    output = output[0].join(output[1:]).interpolate(method="linear",
                                                    limit_area="inside")
    output.columns = ["Índice Dólar", "Euro", "Yen", "Renminbi"]

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    metadata._set(output, area="Global", currency="USD",
                  inf_adj="No", seas_adj="NSA",
                  ts_type="-", cumperiods=1)
    metadata._modify_multiindex(output, levels=[3, 5],
                                new_arrays=[["USD", "EUR", "JPY", "CNY"],
                                            ["Canasta/USD", "EUR/USD", "JPY/USD",
                                             "CNY/USD"]])

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output
