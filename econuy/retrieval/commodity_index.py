import datetime as dt
import re
import tempfile
import zipfile
from io import BytesIO
from os import PathLike, path
from typing import Union
from urllib import error

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import YearEnd
from requests import exceptions
from sqlalchemy.engine.base import Connection, Engine

from econuy.utils import ops, metadata
from econuy.utils.lstrings import urls


@retry(
    retry_on_exceptions=(exceptions.HTTPError, exceptions.ConnectionError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=90,
)
def _weights(update_loc: Union[str, PathLike, Engine,
                               Connection, None] = None,
             revise_rows: Union[str, int] = "nodup",
             save_loc: Union[str, PathLike, Engine,
                             Connection, None] = None,
             only_get: bool = True) -> pd.DataFrame:
    """Get commodity export weights for Uruguay.

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
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Commodity weights : pd.DataFrame
        Export-based weights for relevant commodities to Uruguay.

    """
    name = "commodity_weights"
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, multiindex=False)
        if not output.equals(pd.DataFrame()):
            return output

    base_url = "http://comtrade.un.org/api/get?max=1000&type=C&freq=A&px=S3&ps"
    prods = "%2C".join(["0011", "011", "01251", "01252", "0176", "022", "041",
                        "042", "043", "2222", "24", "25", "268", "97"])
    raw = []
    for year in range(1992, dt.datetime.now().year - 1):
        full_url = f"{base_url}={year}&r=all&p=858&rg=1&cc={prods}"
        un_r = requests.get(full_url)
        raw.append(pd.DataFrame(un_r.json()["dataset"]))
    raw = pd.concat(raw, axis=0)

    table = raw.groupby(["period", "cmdDescE"]).sum().reset_index()
    table = table.pivot(index="period", columns="cmdDescE",
                        values="TradeValue")
    table.fillna(0, inplace=True)
    percentage = table.div(table.sum(axis=1), axis=0)
    percentage.index = (pd.to_datetime(percentage.index, format="%Y")
                        + YearEnd(1))
    roll = percentage.rolling(window=3, min_periods=3).mean()
    output = roll.resample("M").bfill()

    beef = ["BOVINE MEAT", "Edible offal of bovine animals, fresh or chilled",
            "Meat and offal (other than liver), of bovine animals, "
            "prepared or preserv", "Edible offal of bovine animals, frozen",
            "Bovine animals, live"]
    output["Beef"] = output[beef].sum(axis=1, min_count=len(beef))
    output.drop(beef, axis=1, inplace=True)
    output.columns = ["Barley", "Wood", "Gold", "Milk", "Pulp",
                      "Rice", "Soybeans", "Wheat", "Wool", "Beef"]

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name)

    return output


@retry(
    retry_on_exceptions=(error.HTTPError, error.URLError,
                         exceptions.HTTPError, exceptions.ConnectionError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def _prices(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
            revise_rows: Union[str, int] = "nodup",
            save_loc: Union[str, PathLike, Engine, Connection, None] = None,
            only_get: bool = True) -> pd.DataFrame:
    """Get commodity prices for Uruguay.

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
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Commodity prices : pd.DataFrame
        Prices and price indexes of relevant commodities for Uruguay.

    """
    bushel_conv = 36.74 / 100
    name = "commodity_prices"

    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name)
        if not output.equals(pd.DataFrame()):
            return output

    url = urls["commodity_index"]["dl"]
    raw_beef = (pd.read_excel(url["beef"], header=4, index_col=0)
                .dropna(how="all"))
    raw_beef.columns = raw_beef.columns.str.strip()
    proc_beef = raw_beef["Ing. Prom./Ton."].to_frame()
    proc_beef.index = pd.date_range(start="2002-01-04",
                                    periods=len(proc_beef), freq="W-SAT")
    proc_beef["Ing. Prom./Ton."] = np.where(
        proc_beef > np.mean(proc_beef) + np.std(proc_beef) * 2,
        proc_beef / 1000,
        proc_beef,
    )
    beef = proc_beef.resample("M").mean()

    raw_pulp_r = requests.get(url["pulp"])
    temp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(BytesIO(raw_pulp_r.content), "r") as f:
        f.extractall(path=temp_dir.name)
        path_temp = path.join(temp_dir.name, "monthly_values.csv")
        raw_pulp = pd.read_csv(path_temp, sep=";").dropna(how="any")
    proc_pulp = raw_pulp.copy().sort_index(ascending=False)
    proc_pulp.index = pd.date_range(start="1990-01-31",
                                    periods=len(proc_pulp), freq="M")
    proc_pulp.drop(["Label", "Codes"], axis=1, inplace=True)
    pulp = proc_pulp

    soy_wheat = []
    for link in [url["soybean"], url["wheat"]]:
        raw = pd.read_csv(link, index_col=0)
        proc = (raw["Settle"] * bushel_conv).to_frame()
        proc.index = pd.to_datetime(proc.index, format="%Y-%m-%d")
        proc.sort_index(inplace=True)
        soy_wheat.append(proc.resample("M").mean())
    soybean = soy_wheat[0]
    wheat = soy_wheat[1]

    milk_r = requests.get(url["milk1"])
    milk_soup = BeautifulSoup(milk_r.content, "html.parser")
    links = milk_soup.find_all(href=re.compile("Oceanía"))
    xls = links[0]["href"]
    raw_milk = pd.read_excel(requests.utils.quote(xls).replace("%3A", ":"),
                             skiprows=14,
                             nrows=dt.datetime.now().year - 2006)
    raw_milk.dropna(how="all", axis=1, inplace=True)
    raw_milk.drop(["Promedio ", "Variación"], axis=1, inplace=True)
    raw_milk.columns = ["Año/Mes"] + list(range(1, 13))
    proc_milk = pd.melt(raw_milk, id_vars=["Año/Mes"])
    proc_milk.sort_values(by=["Año/Mes", "variable"], inplace=True)
    proc_milk.index = pd.date_range(start="2007-01-31",
                                    periods=len(proc_milk), freq="M")
    proc_milk = proc_milk.iloc[:, 2].to_frame()

    prev_milk = pd.read_excel(url["milk2"], sheet_name="Dairy Products Prices",
                              index_col=0, usecols="A,D", skiprows=5)
    prev_milk = prev_milk.resample("M").mean()
    eurusd_r = requests.get(
        "http://fx.sauder.ubc.ca/cgi/fxdata",
        params=f"b=USD&c=EUR&rd=&fd=1&fm=1&fy=2001&ld=31&lm=12&ly="
               f"{dt.datetime.now().year}&y=monthly&q=volume&f=html&o=&cu=on"
    )
    eurusd = pd.read_html(eurusd_r.content)[0].drop("MMM YYYY", axis=1)
    eurusd.index = pd.date_range(start="2001-01-31", periods=len(eurusd),
                                 freq="M")
    eurusd = eurusd.reindex(prev_milk.index)
    prev_milk = prev_milk.divide(eurusd.values).multiply(10)
    prev_milk = prev_milk.loc[prev_milk.index < min(proc_milk.index)]
    prev_milk.columns, proc_milk.columns = ["Price"], ["Price"]
    milk = prev_milk.append(proc_milk)

    raw_imf = pd.read_excel(url["imf"])
    raw_imf.columns = raw_imf.iloc[0, :]
    proc_imf = raw_imf.iloc[3:, 1:]
    proc_imf.index = pd.date_range(start="1980-01-31",
                                   periods=len(proc_imf), freq="M")
    rice = proc_imf[proc_imf.columns[proc_imf.columns.str.contains("Rice")]]
    wood = proc_imf[proc_imf.columns[
        proc_imf.columns.str.contains("Sawnwood")
    ]]
    wood = wood.mean(axis=1).to_frame()
    wool = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Wool")]]
    wool = wool.mean(axis=1).to_frame()
    barley = proc_imf[proc_imf.columns[
        proc_imf.columns.str.startswith("Barley")
    ]]
    gold = proc_imf[proc_imf.columns[
        proc_imf.columns.str.startswith("Gold")
    ]]

    complete = pd.concat([beef, pulp, soybean, milk, rice, wood, wool, barley,
                          gold, wheat], axis=1)
    complete = complete.reindex(beef.index).dropna(thresh=8)
    complete.columns = ["Beef", "Pulp", "Soybeans", "Milk", "Rice", "Wood",
                        "Wool", "Barley", "Gold", "Wheat"]

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name)
        complete = ops._revise(new_data=complete, prev_data=previous_data,
                               revise_rows=revise_rows)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=complete, name=name)

    return complete


def get(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "commodity_index", index_label: str = "index",
        only_get: bool = False, only_get_prices: bool = False,
        only_get_weights: bool = True) -> pd.DataFrame:
    """Get export-weighted commodity price index for Uruguay.

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
    name : str, default 'commodity_weights'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc`` for the commodity index.
    only_get_prices : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc`` for commodity prices.
    only_get_weights : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc`` for commodity weights.

    Returns
    -------
    Monthly export-weighted commodity index : pd.DataFrame
        Export-weighted average of commodity prices relevant to Uruguay.

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    prices = _prices(update_loc=update_loc, revise_rows="nodup",
                     save_loc=save_loc, only_get=only_get_prices)
    prices = prices.interpolate(method="linear", limit=1).dropna(how="any")
    prices = prices.pct_change(periods=1)
    weights = _weights(update_loc=update_loc, revise_rows="nodup",
                       save_loc=save_loc, only_get=only_get_weights)
    weights = weights[prices.columns]
    weights = weights.reindex(prices.index, method="ffill")

    product = pd.DataFrame(prices.values * weights.values,
                           columns=prices.columns, index=prices.index)
    product = product.sum(axis=1).add(1).to_frame().cumprod().multiply(100)
    product.columns = ["Índice de precios de productos primarios"]

    metadata._set(product, area="Sector externo", currency="USD",
                  inf_adj="No", unit="2002-01=100", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=product, name=name, index_label=index_label)

    return product
