import datetime as dt
import tempfile
import zipfile
from io import BytesIO
from os import PathLike, path, mkdir
from pathlib import Path
from typing import Union, Optional

import numpy as np
import pandas as pd
import requests
from pandas.tseries.offsets import YearEnd

from econuy.utils import updates, metadata
from econuy.utils.lstrings import (beef_url, pulp_url, soybean_url,
                                   what_url, imf_url, milk1_url, milk2_url)


def _weights(update_path: Union[str, PathLike, None] = None,
             revise_rows: Union[str, int] = "nodup",
             save_path: Union[str, PathLike, None] = None,
             force_update: bool = False) -> pd.DataFrame:
    """Get commodity export weights for Uruguay.

    Parameters
    ----------
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    force_update : bool, default False
        If ``True``, fetch data and update existing data even if it was
        modified within its update window (for commodity weights, 85 days).

    Returns
    -------
    Commodity weights : pd.DataFrame
        Export-based weights for relevant commodities to Uruguay.

    """
    update_threshold = 85
    name = "commodity_weights"

    if update_path is not None:
        full_update_path = (Path(update_path) / name).with_suffix(".csv")
        delta, previous_data = updates._check_modified(full_update_path,
                                                       multiindex=False)

        if delta < update_threshold and force_update is False:
            print(f"{full_update_path} was modified within {update_threshold} "
                  f"day(s). Skipping download...")
            return previous_data

    raw = []
    prods = "%2C".join(["0011", "011", "01251", "01252", "0176", "022", "041",
                        "042", "043", "2222", "24", "25", "268", "97"])
    base_url = "http://comtrade.un.org/api/get?max=1000&type=C&freq=A&px=S3&ps"
    for year in range(1992, dt.datetime.now().year-1):
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

    if update_path is not None:
        output = updates._revise(new_data=output, prev_data=previous_data,
                                 revise_rows=revise_rows)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        output.to_csv(full_save_path)

    return output


def _prices(update_path: Union[str, PathLike, None] = None,
            revise_rows: Union[str, int] = "nodup",
            save_path: Union[str, PathLike, None] = None,
            force_update: bool = False) -> pd.DataFrame:
    """Get commodity prices for Uruguay.

    Parameters
    ----------
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    revise_rows : {'nodup', 'auto', int}
        Defines how to process data updates. An integer indicates how many rows
        to remove from the tail of the dataframe and replace with new data.
        String can either be ``auto``, which automatically determines number of
        rows to replace from the inferred data frequency, or ``nodup``,
        which replaces existing periods with new data.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    force_update : bool, default False
        If ``True``, fetch data and update existing data even if it was
        modified within its update window (for commodity prices, 10 days).

    Returns
    -------
    Commodity prices : pd.DataFrame
        Prices and price indexes of relevant commodities for Uruguay.

    """
    update_threshold = 10
    bushel_conv = 36.74 / 100
    name = "commodity_prices"

    if update_path is not None:
        full_update_path = (Path(update_path) / name).with_suffix(".csv")
        delta, previous_data = updates._check_modified(full_update_path,
                                                       multiindex=False)

        if delta < update_threshold and force_update is False:
            print(f"{full_update_path} was modified within {update_threshold} "
                  f"day(s). Skipping download...")
            return previous_data

    raw_beef = (pd.read_excel(beef_url, header=4, index_col=0)
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

    raw_pulp_r = requests.get(pulp_url)
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
    for url in [soybean_url, what_url]:
        raw = pd.read_csv(url, index_col=0)
        proc = (raw["Settle"] * bushel_conv).to_frame()
        proc.index = pd.to_datetime(proc.index, format="%Y-%m-%d")
        proc.sort_index(inplace=True)
        soy_wheat.append(proc.resample("M").mean())
    soybean = soy_wheat[0]
    wheat = soy_wheat[1]

    raw_milk = pd.read_excel(milk1_url, skiprows=13,
                             nrows=dt.datetime.now().year - 2006)
    raw_milk.dropna(how="all", axis=1, inplace=True)
    raw_milk.drop(["Promedio ", "Variación"], axis=1, inplace=True)
    raw_milk.columns = ["Año/Mes"] + list(range(1, 13))
    proc_milk = pd.melt(raw_milk, id_vars=["Año/Mes"])
    proc_milk.sort_values(by=["Año/Mes", "variable"], inplace=True)
    proc_milk.index = pd.date_range(start="2007-01-31",
                                    periods=len(proc_milk), freq="M")
    proc_milk = proc_milk.iloc[:, 2].to_frame()

    prev_milk = pd.read_excel(milk2_url, sheet_name="Dairy Products Prices",
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

    raw_imf = pd.read_excel(imf_url)
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

    if update_path is not None:
        complete = updates._revise(new_data=complete, prev_data=previous_data,
                                   revise_rows=revise_rows)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        complete.to_csv(full_save_path)

    return complete


def get(update_path: Union[str, PathLike, None] = None,
        save_path: Union[str, PathLike, None] = None,
        force_update_prices: bool = True, force_update_weights: bool = False,
        name: Optional[str] = None) -> pd.DataFrame:
    """Get export-weighted commodity price index for Uruguay.

    Parameters
    ----------
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't update.
    force_update_prices : bool, default False
        If ``True``, fetch data and update existing data even if it was
        modified within its update window for commodity prices.
    force_update_weights : bool, default False
        If ``True``, fetch data and update existing data even if it was
        modified within its update window for commodity weights.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Monthly export-weighted commodity index : pd.DataFrame
        Export-weighted average of commodity prices relevant to Uruguay.

    """
    if name is None:
        name = "commodity_index"

    prices = _prices(update_path=update_path, revise_rows="nodup",
                     save_path=save_path, force_update=force_update_prices)
    prices = prices.interpolate(method="linear", limit=1).dropna(how="any")
    prices = prices.pct_change(periods=1)
    weights = _weights(update_path=update_path, revise_rows="nodup",
                       save_path=save_path, force_update=force_update_weights)
    weights = weights[prices.columns]
    weights = weights.reindex(prices.index, method="ffill")

    product = pd.DataFrame(prices.values * weights.values,
                           columns=prices.columns, index=prices.index)
    product = product.sum(axis=1).add(1).to_frame().cumprod()
    product.columns = ["Índice de precios de productos primarios"]

    metadata._set(product, area="Sector externo", currency="USD",
                  inf_adj="No", unit="2002-01-31=1", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        product.to_csv(full_save_path)

    return product
