import datetime as dt
import re
import tempfile
import time
import zipfile
from pathlib import Path
from io import BytesIO
from json import JSONDecodeError
from os import PathLike, path
from typing import Union, Optional
from urllib import error
from urllib.error import HTTPError, URLError

import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd, YearEnd
from sqlalchemy.engine.base import Engine, Connection

from econuy import transform
from econuy.retrieval import regional
from econuy.core import Pipeline
from econuy.utils import ops, metadata, get_project_root
from econuy.utils.ops import get_download_sources, get_name_from_function
from econuy.utils.extras import TRADE_METADATA, RESERVES_COLUMNS, BOP_COLUMNS


def _trade_retriever(name: str) -> pd.DataFrame:
    """Helper function. See any of the `trade_...()` functions."""
    sources = get_download_sources(name)
    meta = TRADE_METADATA[name]
    try:
        xls = pd.ExcelFile(sources["main"])
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = requests.get(sources["main"], verify=certs_path)
            xls = pd.ExcelFile(r.content)
    sheets = []
    start_col = meta["start_col"]
    for sheet in xls.sheet_names:
        raw = (
            pd.read_excel(xls, sheet_name=sheet, index_col=start_col, skiprows=7)
            .iloc[:, start_col:]
            .dropna(thresh=5)
            .T
        )
        raw.index = pd.to_datetime(raw.index, errors="coerce") + MonthEnd(0)
        proc = raw[raw.index.notnull()].dropna(thresh=5, axis=1)
        if name != "trade_imports_category_value":
            try:
                proc = proc.loc[:, meta["colnames"].keys()]
            except KeyError:
                proc.insert(7, "Venezuela", 0)
                proc = proc.loc[:, meta["colnames"].keys()]
            proc.columns = meta["colnames"].values()
        else:
            proc = proc.loc[:, ~(proc == "miles de dólares").any()]
            proc = proc.drop(columns=["DESTINO ECONÓMICO"])
            proc.columns = meta["new_colnames"]
        sheets.append(proc)
    output = pd.concat(sheets).sort_index()
    output = output.apply(pd.to_numeric, errors="coerce")
    if meta["unit"] == "Millones":
        output = output.div(1000)
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Sector externo",
        currency=meta["currency"],
        inf_adj="No",
        unit=meta["unit"],
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_exports_sector_value() -> pd.DataFrame:
    """Get export values by product.

    Returns
    -------
    Export values by product : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_exports_sector_volume() -> pd.DataFrame:
    """Get export volumes by product.

    Returns
    -------
    Export volumes by product : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_exports_sector_price() -> pd.DataFrame:
    """Get export prices by product.

    Returns
    -------
    Export prices by product : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_exports_destination_value() -> pd.DataFrame:
    """Get export values by destination.

    Returns
    -------
    Export values by destination : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_exports_destination_volume() -> pd.DataFrame:
    """Get export volumes by destination.

    Returns
    -------
    Export volumes by destination : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_exports_destination_price() -> pd.DataFrame:
    """Get export prices by destination.

    Returns
    -------
    Export prices by destination : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_imports_category_value() -> pd.DataFrame:
    """Get import values by sector.

    Returns
    -------
    Import values by sector : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_imports_category_volume() -> pd.DataFrame:
    """Get import volumes by sector.

    Returns
    -------
    Import volumes by sector : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_imports_category_price() -> pd.DataFrame:
    """Get import prices by sector.

    Returns
    -------
    Import prices by sector : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_imports_origin_value() -> pd.DataFrame:
    """Get import values by origin.

    Returns
    -------
    Import values by origin : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_imports_origin_volume() -> pd.DataFrame:
    """Get import volumes by origin.

    Returns
    -------
    Import volumes by origin : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_imports_origin_price() -> pd.DataFrame:
    """Get import prices by origin.

    Returns
    -------
    Import prices by origin : pd.DataFrame

    """
    name = get_name_from_function()
    return _trade_retriever(name)


def trade_balance(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """
    Get net trade balance data by country/region.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Net trade balance value by region/country : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("trade_exports_destination_value")
    exports = pipeline.dataset.rename(columns={"Total exportaciones": "Total"})
    pipeline.get("trade_imports_origin_value")
    imports = pipeline.dataset.rename(columns={"Total importaciones": "Total"})
    net = exports - imports
    net.rename_axis(None, inplace=True)

    return net


def terms_of_trade(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """
    Get terms of trade.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Terms of trade (exports/imports) : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("trade_exports_destination_price")
    exports = pipeline.dataset.rename(columns={"Total exportaciones": "Total"})
    pipeline.get("trade_imports_origin_price")
    imports = pipeline.dataset.rename(columns={"Total importaciones": "Total"})

    tot = exports / imports
    tot = tot.loc[:, ["Total"]]
    tot.rename(columns={"Total": "Términos de intercambio"}, inplace=True)
    tot = transform.rebase(tot, start_date="2005-01-01", end_date="2005-12-31")
    tot.rename_axis(None, inplace=True)
    metadata._set(tot, ts_type="-")

    return tot


@retry(
    retry_on_exceptions=(HTTPError, ConnectionError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=90,
)
def _commodity_weights(
    location: Union[str, PathLike, Engine, Connection, None] = None, download: bool = True
) -> pd.DataFrame:
    """Get commodity export weights for Uruguay.

    Parameters
    ----------
    location : str, os.PathLike, SQLAlchemy Connection or Engine, or None, \
               default None
        Either Path or path-like string pointing to a directory where to find
        a CSV for updating, SQLAlchemy connection or engine object, or
        ``None``, don't update.
    download : bool, default True
        If False, don't download data, retrieve what is available from
        ``location``.

    Returns
    -------
    Commodity weights : pd.DataFrame
        Export-based weights for relevant commodities to Uruguay.

    """
    if download is False and location is not None:
        output = ops._io(
            operation="read", data_loc=location, name="commodity_weights", multiindex=None
        )
        if not output.equals(pd.DataFrame()):
            return output

    base_url = "https://comtradeapi.un.org/public/v1/preview/C/A/SS?period"
    prods = ",".join(
        [
            "0011",
            "0111",
            "01251",
            "01252",
            "0176",
            "022",
            "041",
            "042",
            "043",
            "2222",
            "24",
            "25",
            "268",
            "97",
        ]
    )
    start = 1992
    year_pairs = []
    while start < dt.datetime.now().year - 12:
        stop = start + 11
        year_pairs.append(range(start, stop))
        start = stop
    year_pairs.append(range(year_pairs[-1].stop, dt.datetime.now().year - 1))
    reqs = []
    for pair in year_pairs:
        years = ",".join(str(x) for x in pair)
        full_url = f"{base_url}={years}&reporterCode=&partnerCode=858&flowCode=m&cmdCode={prods}&customsCode=c00&motCode=0&partner2Code=0&aggregateBy=reportercode&breakdownMode=classic&includeDesc=True&countOnlyFalse"
        un_r = requests.get(full_url)
        reqs.append(pd.DataFrame(un_r.json()["data"]))
        time.sleep(3)
    raw = pd.concat(reqs, axis=0)

    table = raw.groupby(["period", "cmdDesc"]).sum().reset_index()
    table = table.pivot(index="period", columns="cmdDesc", values="primaryValue")
    table.fillna(0, inplace=True)

    # output = roll.resample("M").bfill()

    beef = [
        "Bovine animals, live",
        "Bovine meat, fresh, chilled or frozen",
        "Bovine meat,frsh,chilled",
        "Edible offal of bovine animals, frozen",
        "Edible offal of bovine animals, fresh or chilled",
        "Edible offal of bovine animals, fresh/chilled",
        "Meat & offal (other than liver), of bovine animals, prepared/preserved, n.e.s.",
        "Meat and offal (other than liver), of bovine animals, prepared or preserv",
        "Meat of bovine animals, fresh or chilled",
        "Meat of bovine animals,fresh,chilled or frozen",
    ]
    table["Beef"] = table[beef].sum(axis=1, min_count=len(beef))
    table.drop(beef, axis=1, inplace=True)
    table.columns = [
        "Cebada",
        "Madera",
        "Oro",
        "Leche",
        "Pulpa de celulosa",
        "Arroz",
        "Soja",
        "Trigo",
        "Lana",
        "Carne bovina",
    ]
    output = table.div(table.sum(axis=1), axis=0)
    output.index = pd.to_datetime(output.index, format="%Y") + YearEnd(1)
    output = output.rolling(window=3, min_periods=3).mean().bfill()

    if location is not None:
        previous_data = ops._io(
            operation="read", data_loc=location, name="commodity_weights", multiindex=None
        )
        output = ops._revise(new_data=output, prev_data=previous_data, revise_rows="nodup")

    if location is not None:
        ops._io(
            operation="save",
            data_loc=location,
            data=output,
            name="commodity_weights",
            multiindex=None,
        )

    return output


@retry(
    retry_on_exceptions=(error.HTTPError, error.URLError, HTTPError, ConnectionError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def commodity_prices() -> pd.DataFrame:
    """Get commodity prices for Uruguay.

    Returns
    -------
    Commodity prices : pd.DataFrame
        Prices and price indexes of relevant commodities for Uruguay.

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        raw_beef = pd.read_excel(sources["beef"], header=4, index_col=0).dropna(how="all")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files", "inac_certs.pem")
            r = requests.get(sources["beef"], verify=certificate)
            raw_beef = pd.read_excel(BytesIO(r.content), header=4, index_col=0).dropna(how="all")
        else:
            raise err

    raw_beef.columns = raw_beef.columns.str.strip()
    proc_beef = raw_beef["Ing. Prom./Ton."].to_frame()
    proc_beef.index = pd.date_range(start="2002-01-04", periods=len(proc_beef), freq="W-SAT")
    proc_beef["Ing. Prom./Ton."] = np.where(
        proc_beef > np.mean(proc_beef) + np.std(proc_beef) * 2,
        proc_beef / 1000,
        proc_beef,
    )
    beef = proc_beef.resample("M").mean()

    # soy_wheat = []
    # for link in [url["soybean"], url["wheat"]]:
    #    proc = pd.read_csv(link, index_col=0)
    #    proc.index = pd.to_datetime(proc.index, format="%Y-%m-%d")
    #    proc.sort_index(inplace=True)
    #    soy_wheat.append(proc.resample("M").mean())
    # soybean = soy_wheat[0]
    # wheat = soy_wheat[1]

    milk_r = requests.get(sources["milk1"])
    milk_soup = BeautifulSoup(milk_r.content, "html.parser")
    links = milk_soup.find_all(href=re.compile("Europa"))
    xls = links[0]["href"]
    raw_milk = pd.read_excel(
        requests.utils.quote(xls).replace("%3A", ":"),
        skiprows=13,
        nrows=dt.datetime.now().year - 2006,
    )
    raw_milk.dropna(how="all", axis=1, inplace=True)
    raw_milk.drop(["Promedio ", "Variación"], axis=1, inplace=True)
    raw_milk.columns = ["Año/Mes"] + list(range(1, 13))
    proc_milk = pd.melt(raw_milk, id_vars=["Año/Mes"])
    proc_milk.sort_values(by=["Año/Mes", "variable"], inplace=True)
    proc_milk.index = pd.date_range(start="2007-01-31", periods=len(proc_milk), freq="M")
    proc_milk = proc_milk.iloc[:, 2].to_frame().divide(10).dropna()

    prev_milk = pd.read_excel(
        sources["milk2"],
        sheet_name="Raw Milk Prices",
        index_col=0,
        skiprows=6,
        usecols="A:AB",
        na_values=["c", 0],
    )
    prev_milk = (
        prev_milk[prev_milk.index.notna()].dropna(axis=0, how="all").mean(axis=1).to_frame()
    )
    prev_milk = prev_milk.set_index(
        pd.date_range(start="1977-01-31", freq="M", periods=len(prev_milk))
    )
    eurusd_r = requests.get(
        "http://fx.sauder.ubc.ca/cgi/fxdata",
        params=f"b=USD&c=EUR&rd=&fd=1&fm=1&fy=2001&ld=31&lm=12&ly="
        f"{dt.datetime.now().year}&y=monthly&q=volume&f=html&o=&cu=on",
    )
    eurusd = pd.read_html(eurusd_r.content)[0].drop("MMM YYYY", axis=1)
    eurusd.index = pd.date_range(start="2001-01-31", periods=len(eurusd), freq="M")
    eurusd_milk = eurusd.reindex(prev_milk.index)
    prev_milk = prev_milk.divide(eurusd_milk.values).multiply(10)
    prev_milk = prev_milk.loc[prev_milk.index < min(proc_milk.index)]
    prev_milk.columns, proc_milk.columns = ["Price"], ["Price"]
    milk = pd.concat([prev_milk, proc_milk])

    raw_pulp_r = requests.get(sources["pulp"].format(year=dt.date.today().year))
    temp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(BytesIO(raw_pulp_r.content), "r") as f:
        f.extractall(path=temp_dir.name)
        path_temp = path.join(temp_dir.name, "monthly_values.csv")
        raw_pulp = pd.read_csv(path_temp, sep=";").dropna(how="any")
    proc_pulp = raw_pulp.copy().sort_index(ascending=False)
    proc_pulp.index = pd.date_range(start="1990-01-31", periods=len(proc_pulp), freq="M")
    proc_pulp = proc_pulp.drop(["Label", "Codes"], axis=1).astype(float)
    proc_pulp = proc_pulp.div(eurusd.reindex(proc_pulp.index).values)
    pulp = proc_pulp

    r_imf = requests.get(sources["imf"])
    imf = re.findall("external-data[A-z]+.ashx", r_imf.text)[0]
    imf = f"https://imf.org/-/media/Files/Research/CommodityPrices/Monthly/{imf}"
    raw_imf = pd.read_excel(imf).dropna(how="all", axis=1).dropna(how="all", axis=0)
    raw_imf.columns = raw_imf.iloc[0, :]
    proc_imf = raw_imf.iloc[3:, 1:]
    proc_imf.index = pd.date_range(start="1990-01-31", periods=len(proc_imf), freq="M")
    rice = proc_imf[proc_imf.columns[proc_imf.columns.str.contains("Rice")]]
    wood = proc_imf[proc_imf.columns[proc_imf.columns.str.contains("Sawnwood")]]
    wood = wood.mean(axis=1).to_frame()
    wool = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Wool")]]
    wool = wool.mean(axis=1).to_frame()
    barley = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Barley")]]
    gold = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Gold")]]
    soybean = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Soybeans, U.S.")]]
    wheat = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Wheat")]]

    complete = pd.concat(
        [beef, pulp, soybean, milk, rice, wood, wool, barley, gold, wheat], axis=1
    )
    complete = complete.reindex(beef.index).dropna(thresh=8)
    complete.columns = [
        "Carne bovina",
        "Pulpa de celulosa",
        "Soja",
        "Leche",
        "Arroz",
        "Madera",
        "Lana",
        "Cebada",
        "Oro",
        "Trigo",
    ]
    complete = complete.apply(pd.to_numeric, errors="coerce")
    complete.rename_axis(None, inplace=True)

    metadata._set(
        complete,
        area="Sector externo",
        currency="USD",
        inf_adj="No",
        unit="2002-01=100",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )
    metadata._modify_multiindex(
        complete,
        levels=[5],
        new_arrays=[
            [
                "USD por ton",
                "USD por ton",
                "USD por ton",
                "USD por ton",
                "USD por ton",
                "USD por m3",
                "US cent. por kg",
                "USD por ton",
                "USD por onza troy",
                "USD por ton",
            ]
        ],
    )

    return complete


def commodity_index(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """Get export-weighted commodity price index for Uruguay.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Monthly export-weighted commodity index : pd.DataFrame
        Export-weighted average of commodity prices relevant to Uruguay.

    """
    if pipeline is None:
        pipeline = Pipeline()
    pipeline.get("commodity_prices")
    prices = pipeline.dataset
    prices.columns = prices.columns.get_level_values(0)
    prices = prices.interpolate(method="linear", limit=1).dropna(how="any")
    prices = prices.pct_change(periods=1)
    weights = _commodity_weights(
        location=pipeline.location, download=pipeline._download_commodity_weights
    )
    weights = weights[prices.columns]
    weights = weights.reindex(prices.index, method="ffill")

    product = pd.DataFrame(
        prices.values * weights.values, columns=prices.columns, index=prices.index
    )
    product = product.sum(axis=1).add(1).to_frame().cumprod().multiply(100)
    product.columns = ["Índice de precios de productos primarios"]
    product.rename_axis(None, inplace=True)

    metadata._set(
        product,
        area="Sector externo",
        currency="USD",
        inf_adj="No",
        unit="2002-01=100",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return product


@retry(
    retry_on_exceptions=(error.HTTPError, error.URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def rxr() -> pd.DataFrame:
    """Get official (BCU) real exchange rates.

    Returns
    -------
    Monthly real exchange rates vs select countries/regions : pd.DataFrame
        Available: global, regional, extraregional, Argentina, Brazil, US.

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        raw = pd.read_excel(sources["main"], skiprows=8, usecols="B:N", index_col=0)
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = requests.get(sources["main"], verify=certs_path)
            raw = pd.read_excel(r.content, skiprows=8, usecols="B:N", index_col=0)
    proc = raw.dropna(how="any")
    proc.columns = [
        "Global",
        "Extrarregional",
        "Regional",
        "Argentina",
        "Brasil",
        "EE.UU.",
        "México",
        "Alemania",
        "España",
        "Reino Unido",
        "Italia",
        "China",
    ]
    proc.index = pd.to_datetime(proc.index) + MonthEnd(1)
    proc.rename_axis(None, inplace=True)

    metadata._set(
        proc,
        area="Sector externo",
        currency="UYU/Otro",
        inf_adj="No",
        unit="2017=100",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return proc


@retry(
    retry_on_exceptions=(error.HTTPError, error.URLError, JSONDecodeError),
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=90,
)
def rxr_custom(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """Get custom real exchange rates vis-à-vis the US, Argentina and Brazil.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Monthly real exchange rates vs select countries : pd.DataFrame
        Available: Argentina, Brazil, US.

    """
    if pipeline is None:
        pipeline = Pipeline()

    ifs = regional._ifs(pipeline=pipeline)
    pipeline.get("cpi")
    uy_cpi = pipeline.dataset
    pipeline.get("nxr_monthly")
    uy_e = pipeline.dataset.iloc[:, [1]]
    proc = pd.concat([ifs, uy_cpi, uy_e], axis=1)
    proc = proc.interpolate(method="linear", limit_area="inside")
    proc = proc.dropna(how="all")
    proc.columns = ["AR_E_O", "AR_E_U", "BR_E", "AR_P", "BR_P", "US_P", "UY_P", "UY_E"]

    output = pd.DataFrame()
    output["UY_E_P"] = proc["UY_E"] / proc["UY_P"]
    output["Uruguay-Argentina oficial"] = output["UY_E_P"] / proc["AR_E_O"] * proc["AR_P"]
    output["Uruguay-Argentina informal"] = output["UY_E_P"] / proc["AR_E_U"] * proc["AR_P"]
    output["Uruguay-Brasil"] = output["UY_E_P"] / proc["BR_E"] * proc["BR_P"]
    output["Uruguay-EE.UU."] = output["UY_E_P"] * proc["US_P"]
    output.drop("UY_E_P", axis=1, inplace=True)
    output = output.loc[output.index >= "1979-12-01"]
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Sector externo",
        currency="-",
        inf_adj="No",
        unit="-",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )
    output = transform.rebase(output, start_date="2010-01-01", end_date="2010-12-31", base=100)
    metadata._modify_multiindex(
        output, levels=[3], new_arrays=[["UYU/ARS", "UYU/ARS", "UYU/BRL", "UYU/USD"]]
    )

    return output


@retry(
    retry_on_exceptions=(error.HTTPError, error.URLError),
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=90,
)
def balance_of_payments() -> pd.DataFrame:
    """Get balance of payments.

    Returns
    -------
    Quarterly balance of payments : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        raw = (
            pd.read_excel(sources["main"], skiprows=7, index_col=0, sheet_name="Cuadro Nº 1")
            .dropna(how="all")
            .T
        )
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = requests.get(sources["main"], verify=certs_path)
            raw = (
                pd.read_excel(r.content, skiprows=7, index_col=0, sheet_name="Cuadro Nº 1")
                .dropna(how="all")
                .T
            )
    output = raw.iloc[:, 2:]
    output.index = pd.date_range(start="2012-03-31", freq="Q", periods=len(output))
    pattern = r"\(1\)|\(2\)|\(3\)|\(4\)|\(5\)"
    output.columns = [re.sub(pattern, "", x).strip() for x in output.columns]
    output = output.drop(
        [
            "Por Sector Institucional",
            "Por Categoría Funcional",
            "Por Instrumento y Sector Institucional",
        ],
        axis=1,
    )
    output.columns = [x[:58] + "..." if len(x) > 60 else x for x in BOP_COLUMNS]

    metadata._set(
        output,
        area="Sector externo",
        currency="USD",
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(error.HTTPError, error.URLError),
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=90,
)
def balance_of_payments_summary(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """Get a balance of payments summary and capital flows calculations.

    Returns
    -------
    Quarterly balance of payments summary : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get("balance_of_payments")
    bop = pipeline.dataset.copy()
    output = pd.DataFrame(index=bop.index)
    output["Cuenta corriente"] = bop["Cuenta Corriente"]
    output["Balance de bienes y servicios"] = bop["Bienes y Servicios"]
    output["Balance de bienes"] = bop["Bienes"]
    output["Exportaciones de bienes"] = bop["Bienes - Crédito"]
    output["Importaciones de bienes"] = bop["Bienes - Débito"]
    output["Balance de servicios"] = bop["Servicios"]
    output["Exportaciones de servicios"] = bop["Servicios - Crédito"]
    output["Importaciones de servicios"] = bop["Servicios - Débito"]
    output["Ingreso primario"] = bop["Ingreso Primario"]
    output["Ingreso secundario"] = bop["Ingreso Secundario"]
    output["Cuenta capital"] = bop["Cuenta Capital"]
    output["Crédito en cuenta capital"] = bop["Cuenta Capital - Crédito"]
    output["Débito en cuenta capital"] = bop["Cuenta Capital - Débito"]
    output["Cuenta financiera"] = bop["Cuenta Financiera"]
    output["Balance de inversión directa"] = bop["Inversión directa"]
    output["Inversión directa en el exterior"] = bop[
        "Inversión directa - Adquisición neta de activos financieros"
    ]
    output["Inversión directa en Uruguay"] = bop["Inversión directa - Pasivos netos incurridos"]
    output["Balance de inversión de cartera"] = bop["Inversión de cartera"]
    output["Inversión de cartera en el exterior"] = bop[
        "Inversión de cartera - Adquisición neta de activos fin"
    ]
    output["Inversión de cartera en Uruguay"] = bop[
        "Inversión de cartera - Pasivos netos incurridos"
    ]
    output["Saldo de derivados financieros"] = bop["Derivados financieros - distintos de reservas"]
    output["Balance de otra inversión"] = bop["Otra inversión"]
    output["Otra inversión en el exterior"] = bop[
        "Otra inversión - Adquisición neta de activos financieros"
    ]
    output["Otra inversión en Uruguay"] = bop["Otra inversión - Pasivos netos incurridos"]
    output["Variación de activos de reserva"] = bop["Activos de Reserva BCU"]
    output["Errores y omisiones"] = bop["Errores y Omisiones"]
    output["Flujos brutos de capital"] = (
        output["Inversión directa en Uruguay"]
        + output["Inversión de cartera en Uruguay"]
        + output["Otra inversión en Uruguay"]
        + output["Crédito en cuenta capital"]
    )
    output["Flujos netos de capital"] = (
        -output["Balance de inversión directa"]
        - output["Balance de inversión de cartera"]
        - output["Balance de otra inversión"]
        - output["Saldo de derivados financieros"]
        + output["Cuenta capital"]
    )

    metadata._set(
        output,
        area="Sector externo",
        currency="USD",
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def international_reserves() -> pd.DataFrame:
    """Get international reserves data.

    Returns
    -------
    Daily international reserves : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        raw = pd.read_excel(
            sources["main"], usecols="D:J", index_col=0, skiprows=5, na_values="n/d"
        )
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = requests.get(sources["main"], verify=certs_path)
            raw = pd.read_excel(r.content, usecols="D:J", index_col=0, skiprows=5, na_values="n/d")
    proc = raw.dropna(thresh=1)
    reserves = proc[proc.index.notnull()]
    reserves.columns = [
        "Activos de reserva",
        "Otros activos externos de corto plazo",
        "Obligaciones en ME con el sector público",
        "Obligaciones en ME con el sector financiero",
        "Activos de reserva sin sector público y financiero",
        "Posición en ME del BCU",
    ]
    reserves = reserves.apply(pd.to_numeric, errors="coerce")
    reserves = reserves.loc[~reserves.index.duplicated(keep="first")]
    reserves.index = pd.to_datetime(reserves.index)
    reserves.rename_axis(None, inplace=True)

    metadata._set(
        reserves,
        area="Sector externo",
        currency="USD",
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Stock",
        cumperiods=1,
    )

    return reserves


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=90,
)
def international_reserves_changes(
    pipeline: Optional[Pipeline] = None, previous_data: pd.DataFrame = pd.DataFrame()
) -> pd.DataFrame:
    """Get international reserves changes data.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.
    previous_data : pd.DataFrame
        A DataFrame representing this dataset used to extract last
        available dates.

    Returns
    -------
    Monthly international reserves changes : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    if pipeline is None:
        pipeline = Pipeline()
    if previous_data.empty:
        first_year = 2013
    else:
        first_year = previous_data.index[-1].year

    mapping = dict(
        zip(
            ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Set", "Oct", "Nov", "Dic"],
            [str(x).zfill(2) for x in range(1, 13)],
        )
    )
    inverse_mapping = {v: k for k, v in mapping.items()}
    mapping.update({"Sep": "09"})

    current_year = dt.date.today().year
    months = []
    for year in range(first_year, current_year + 1):
        if year < current_year:
            filename = f"dic{year}.xls"
        else:
            current_month = inverse_mapping[str(dt.date.today().month).zfill(2)]
            last_month = inverse_mapping[
                str((dt.date.today() + relativedelta(months=-1)).month).zfill(2)
            ]
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            filename = f"{current_month}{year}.xls"
            r = requests.get(f"{sources['main']}{filename}", verify=certs_path)
            if r.status_code == 404:
                filename = f"{last_month}{year}.xls"
        try:
            data = pd.read_excel(
                f"{sources['main']}{filename}",
                skiprows=2,
                sheet_name="ACTIVOS DE RESERVA",
            )
        except URLError as err:
            if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
                certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
                r = requests.get(f"{sources['main']}{filename}", verify=certs_path)
                data = pd.read_excel(
                    r.content,
                    skiprows=2,
                    sheet_name="ACTIVOS DE RESERVA",
                )
        data = data.dropna(how="all").dropna(how="all", axis=1).set_index("CONCEPTOS")
        if data.columns[0] == "Mes":
            data.columns = data.iloc[0, :]
        data = (
            data.iloc[1:]
            .T.reset_index(names="date")
            .loc[
                lambda x: ~x["date"]
                .astype(str)
                .str.contains("Unnamed|Trimestre|Año|I", regex=True, case=True),
                lambda x: x.columns.notna(),
            ]
        )
        data["date"] = data["date"].replace("Mes\n", "", regex=True).str.strip()
        data = data.loc[data["date"].notna()]

        index = pd.Series(data["date"]).str.split("-", expand=True).replace(mapping)
        index = pd.to_datetime(
            index.iloc[:, 0] + "-" + index.iloc[:, 1], format="%m-%Y", errors="coerce"
        ) + pd.tseries.offsets.MonthEnd(1)
        if year == 2019:
            index = index.fillna(dt.datetime(year, 1, 31))
        elif year == 2013:
            index = index.fillna(dt.datetime(year, 12, 31))
        data["date"] = index
        data.columns = ["date"] + RESERVES_COLUMNS
        months.append(data)
    reserves = (
        pd.concat(months, sort=False, ignore_index=True)
        .drop_duplicates(subset="date")
        .dropna(subset="date")
        .set_index("date")
        .sort_index()
        .rename_axis(None)
    )
    metadata._set(
        reserves,
        area="Sector externo",
        currency="USD",
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )
    reserves.columns = reserves.columns.set_levels(["-"], level=2)

    return reserves
