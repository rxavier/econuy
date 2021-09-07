import datetime as dt
import re
import tempfile
import urllib
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
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd, YearEnd
from sqlalchemy.engine.base import Engine, Connection

from econuy import transform
from econuy.retrieval import regional
from econuy.core import Pipeline
from econuy.utils import ops, metadata, get_project_root
from econuy.utils.sources import urls
from econuy.utils.extras import trade_metadata, reserves_cols


def _trade_retriever(name: str) -> pd.DataFrame:
    """Helper function. See any of the `trade_...()` functions."""
    short_name = name[6:]
    meta = trade_metadata[short_name]
    xls = pd.ExcelFile(urls[name]["dl"]["main"])
    sheets = []
    for sheet in xls.sheet_names:
        raw = (
            pd.read_excel(xls, sheet_name=sheet, usecols=meta["cols"], index_col=0, skiprows=7)
            .dropna(thresh=5)
            .T
        )
        raw.index = pd.to_datetime(raw.index, errors="coerce") + MonthEnd(0)
        proc = raw[raw.index.notnull()].dropna(thresh=5, axis=1)
        if name != "trade_m_sect_val":
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
def trade_x_prod_val() -> pd.DataFrame:
    """Get export values by product.

    Returns
    -------
    Export values by product : pd.DataFrame

    """
    return _trade_retriever(name="trade_x_prod_val")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_x_prod_vol() -> pd.DataFrame:
    """Get export volumes by product.

    Returns
    -------
    Export volumes by product : pd.DataFrame

    """
    return _trade_retriever(name="trade_x_prod_vol")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_x_prod_pri() -> pd.DataFrame:
    """Get export prices by product.

    Returns
    -------
    Export prices by product : pd.DataFrame

    """
    return _trade_retriever(name="trade_x_prod_pri")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_x_dest_val() -> pd.DataFrame:
    """Get export values by destination.

    Returns
    -------
    Export values by destination : pd.DataFrame

    """
    return _trade_retriever(name="trade_x_dest_val")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_x_dest_vol() -> pd.DataFrame:
    """Get export volumes by destination.

    Returns
    -------
    Export volumes by destination : pd.DataFrame

    """
    return _trade_retriever(name="trade_x_dest_vol")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_x_dest_pri() -> pd.DataFrame:
    """Get export prices by destination.

    Returns
    -------
    Export prices by destination : pd.DataFrame

    """
    return _trade_retriever(name="trade_x_dest_pri")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_m_sect_val() -> pd.DataFrame:
    """Get import values by sector.

    Returns
    -------
    Import values by sector : pd.DataFrame

    """
    return _trade_retriever(name="trade_m_sect_val")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_m_sect_vol() -> pd.DataFrame:
    """Get import volumes by sector.

    Returns
    -------
    Import volumes by sector : pd.DataFrame

    """
    return _trade_retriever(name="trade_m_sect_vol")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_m_sect_pri() -> pd.DataFrame:
    """Get import prices by sector.

    Returns
    -------
    Import prices by sector : pd.DataFrame

    """
    return _trade_retriever(name="trade_m_sect_pri")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_m_orig_val() -> pd.DataFrame:
    """Get import values by origin.

    Returns
    -------
    Import values by origin : pd.DataFrame

    """
    return _trade_retriever(name="trade_m_orig_val")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_m_orig_vol() -> pd.DataFrame:
    """Get import volumes by origin.

    Returns
    -------
    Import volumes by origin : pd.DataFrame

    """
    return _trade_retriever(name="trade_m_orig_vol")


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def trade_m_orig_pri() -> pd.DataFrame:
    """Get import prices by origin.

    Returns
    -------
    Import prices by origin : pd.DataFrame

    """
    return _trade_retriever(name="trade_m_orig_pri")


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
    pipeline.get("trade_x_dest_val")
    exports = pipeline.dataset.rename(columns={"Total exportaciones": "Total"})
    pipeline.get("trade_m_orig_val")
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
    pipeline.get("trade_x_dest_pri")
    exports = pipeline.dataset.rename(columns={"Total exportaciones": "Total"})
    pipeline.get("trade_m_orig_pri")
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

    base_url = "http://comtrade.un.org/api/get?max=10000&type=C&freq=A&px=S3&ps"
    prods = "%2C".join(
        [
            "0011",
            "011",
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
    while start < dt.datetime.now().year - 5:
        stop = start + 4
        year_pairs.append(range(start, stop))
        start = stop
    year_pairs.append(range(year_pairs[-1].stop, dt.datetime.now().year - 1))
    reqs = []
    for pair in year_pairs:
        years = "%2C".join(str(x) for x in pair)
        full_url = f"{base_url}={years}&r=all&p=858&rg=1&cc={prods}"
        un_r = requests.get(full_url)
        reqs.append(pd.DataFrame(un_r.json()["dataset"]))
    raw = pd.concat(reqs, axis=0)

    table = raw.groupby(["period", "cmdDescE"]).sum().reset_index()
    table = table.pivot(index="period", columns="cmdDescE", values="TradeValue")
    table.fillna(0, inplace=True)
    percentage = table.div(table.sum(axis=1), axis=0)
    percentage.index = pd.to_datetime(percentage.index, format="%Y") + YearEnd(1)
    roll = percentage.rolling(window=3, min_periods=3).mean()
    output = roll.resample("M").bfill()

    beef = [
        "BOVINE MEAT",
        "Edible offal of bovine animals, fresh or chilled",
        "Meat and offal (other than liver), of bovine animals, " "prepared or preserv",
        "Edible offal of bovine animals, frozen",
        "Bovine animals, live",
    ]
    output["Beef"] = output[beef].sum(axis=1, min_count=len(beef))
    output.drop(beef, axis=1, inplace=True)
    output.columns = [
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
    bushel_conv = 36.74 / 100

    url = urls["commodity_prices"]["dl"]
    try:
        raw_beef = pd.read_excel(url["beef"], header=4, index_col=0).dropna(how="all")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files", "inac_certs.pem")
            r = requests.get(url["beef"], verify=certificate)
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

    raw_pulp_r = requests.get(url["pulp"])
    temp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(BytesIO(raw_pulp_r.content), "r") as f:
        f.extractall(path=temp_dir.name)
        path_temp = path.join(temp_dir.name, "monthly_values.csv")
        raw_pulp = pd.read_csv(path_temp, sep=";").dropna(how="any")
    proc_pulp = raw_pulp.copy().sort_index(ascending=False)
    proc_pulp.index = pd.date_range(start="1990-01-31", periods=len(proc_pulp), freq="M")
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
    links = milk_soup.find_all(href=re.compile("Oceanía|Oceania"))
    xls = links[0]["href"]
    raw_milk = pd.read_excel(
        requests.utils.quote(xls).replace("%3A", ":"),
        skiprows=14,
        nrows=dt.datetime.now().year - 2006,
    )
    raw_milk.dropna(how="all", axis=1, inplace=True)
    raw_milk.drop(["Promedio ", "Variación"], axis=1, inplace=True)
    raw_milk.columns = ["Año/Mes"] + list(range(1, 13))
    proc_milk = pd.melt(raw_milk, id_vars=["Año/Mes"])
    proc_milk.sort_values(by=["Año/Mes", "variable"], inplace=True)
    proc_milk.index = pd.date_range(start="2007-01-31", periods=len(proc_milk), freq="M")
    proc_milk = proc_milk.iloc[:, 2].to_frame()

    prev_milk = pd.read_excel(
        url["milk2"], sheet_name="Dairy Products Prices", index_col=0, usecols="A,D", skiprows=5
    )
    prev_milk = prev_milk.resample("M").mean()
    eurusd_r = requests.get(
        "http://fx.sauder.ubc.ca/cgi/fxdata",
        params=f"b=USD&c=EUR&rd=&fd=1&fm=1&fy=2001&ld=31&lm=12&ly="
        f"{dt.datetime.now().year}&y=monthly&q=volume&f=html&o=&cu=on",
    )
    eurusd = pd.read_html(eurusd_r.content)[0].drop("MMM YYYY", axis=1)
    eurusd.index = pd.date_range(start="2001-01-31", periods=len(eurusd), freq="M")
    eurusd = eurusd.reindex(prev_milk.index)
    prev_milk = prev_milk.divide(eurusd.values).multiply(10)
    prev_milk = prev_milk.loc[prev_milk.index < min(proc_milk.index)]
    prev_milk.columns, proc_milk.columns = ["Price"], ["Price"]
    milk = prev_milk.append(proc_milk)

    r_imf = requests.get(url["imf"])
    imf = re.findall("external-data[A-z]+.ashx", r_imf.text)[0]
    imf = f"https://imf.org/-/media/Files/Research/CommodityPrices/Monthly/{imf}"
    raw_imf = pd.read_excel(imf).dropna(how="all", axis=1).dropna(how="all", axis=0)
    raw_imf.columns = raw_imf.iloc[0, :]
    proc_imf = raw_imf.iloc[3:, 1:]
    proc_imf.index = pd.date_range(start="1980-01-31", periods=len(proc_imf), freq="M")
    rice = proc_imf[proc_imf.columns[proc_imf.columns.str.contains("Rice")]]
    wood = proc_imf[proc_imf.columns[proc_imf.columns.str.contains("Sawnwood")]]
    wood = wood.mean(axis=1).to_frame()
    wool = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Wool")]]
    wool = wool.mean(axis=1).to_frame()
    barley = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Barley")]]
    gold = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Gold")]]

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
def rxr_official() -> pd.DataFrame:
    """Get official (BCU) real exchange rates.

    Returns
    -------
    Monthly real exchange rates vs select countries/regions : pd.DataFrame
        Available: global, regional, extraregional, Argentina, Brazil, US.

    """
    raw = pd.read_excel(urls["rxr_official"]["dl"]["main"], skiprows=8, usecols="B:N", index_col=0)
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
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def reserves() -> pd.DataFrame:
    """Get international reserves data.

    Returns
    -------
    Daily international reserves : pd.DataFrame

    """
    raw = pd.read_excel(
        urls["reserves"]["dl"]["main"], usecols="D:J", index_col=0, skiprows=5, na_values="n/d"
    )
    proc = raw.dropna(how="any", thresh=1)
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
    reserves.rename_axis(None, inplace=True)
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
def reserves_changes(
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
    Daily international reserves changes : pd.DataFrame

    """
    name = "reserves_changes"

    if pipeline is None:
        pipeline = Pipeline()
    if previous_data.empty:
        first_year = 2013
    else:
        first_year = previous_data.index[-1].year

    months = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "set", "oct", "nov", "dic"]
    years = list(range(first_year, dt.datetime.now().year + 1))
    files = [month + str(year) for year in years for month in months]
    first_dates = list(pd.date_range(start=f"{first_year}-01-01", periods=len(files), freq="MS"))
    url = urls["reserves_changes"]["dl"]["main"]
    links = [f"{url}{file}.xls" for file in files]
    wrong_may14 = f"{url}may2014.xls"
    fixed_may14 = f"{url}mayo2014.xls"
    links = [fixed_may14 if x == wrong_may14 else x for x in links]

    if not previous_data.empty:
        previous_data.columns = previous_data.columns.set_levels(["-"], level=2)
        previous_data.columns = reserves_cols[1:46]
        previous_data.index = pd.to_datetime(previous_data.index).normalize()

    reports = []
    for link, first_day in zip(links, first_dates):
        try:
            raw = pd.read_excel(link, sheet_name="ACTIVOS DE RESERVA", skiprows=3)
            last_day = first_day + relativedelta(months=1) - dt.timedelta(days=1)
            proc = raw.dropna(axis=0, thresh=20).dropna(axis=1, thresh=20)
            proc = proc.transpose()
            proc = proc.iloc[:, 1:46]
            proc.columns = reserves_cols[1:46]
            proc = proc.iloc[1:]
            proc.index = pd.to_datetime(proc.index, errors="coerce").normalize()
            proc = proc.loc[proc.index.dropna()]
            proc = proc.loc[first_day:last_day]
            reports.append(proc)

        except urllib.error.HTTPError:
            pass

    mar14 = pd.read_excel(urls[name]["dl"]["missing"], index_col=0)
    mar14.columns = reserves_cols[1:46]
    reserves = pd.concat(reports + [mar14], sort=False)
    reserves = previous_data.append(reserves, sort=False)
    reserves = reserves.loc[~reserves.index.duplicated(keep="last")].sort_index()

    reserves = reserves.apply(pd.to_numeric, errors="coerce")
    reserves.rename_axis(None, inplace=True)
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
