import datetime as dt
import re
import tempfile
import zipfile
from io import BytesIO
from os import path
from urllib.error import URLError

import pandas as pd
import httpx
from pandas.tseries.offsets import MonthEnd, YearEnd

from econuy import load_dataset
from econuy.base import Dataset, DatasetMetadata
from econuy.retrieval import regional
from econuy.utils.operations import get_download_sources, get_name_from_function
from econuy.utils.extras import TRADE_METADATA, BOP_COLUMNS
from econuy.utils.retrieval import get_with_ssl_context


def _get_trade(dataset_name: str) -> Dataset:
    """Helper function. See any of the `trade_...()` functions."""
    sources = get_download_sources(dataset_name)
    meta = TRADE_METADATA[dataset_name]
    try:
        xls = pd.ExcelFile(sources["main"])
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            r_bytes = get_with_ssl_context("bcu", sources["main"])
            xls = pd.ExcelFile(r_bytes)
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
        if dataset_name != "trade_imports_category_value":
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
    if meta["unit"] == "Millions":
        output = output.div(1000)
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{dataset_name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": meta["currency"],
        "inflation_adjustment": None,
        "unit": meta["unit"],
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        dataset_name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(dataset_name, output, metadata)

    return dataset


def trade_exports_sector_value() -> Dataset:
    """Get export values by product.

    Returns
    -------
    Export values by product : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_exports_sector_volume() -> Dataset:
    """Get export volumes by product.

    Returns
    -------
    Export volumes by product : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_exports_sector_price() -> Dataset:
    """Get export prices by product.

    Returns
    -------
    Export prices by product : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_exports_destination_value() -> Dataset:
    """Get export values by destination.

    Returns
    -------
    Export values by destination : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_exports_destination_volume() -> Dataset:
    """Get export volumes by destination.

    Returns
    -------
    Export volumes by destination : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_exports_destination_price() -> Dataset:
    """Get export prices by destination.

    Returns
    -------
    Export prices by destination : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_imports_category_value() -> Dataset:
    """Get import values by sector.

    Returns
    -------
    Import values by sector : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_imports_category_volume() -> Dataset:
    """Get import volumes by sector.

    Returns
    -------
    Import volumes by sector : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_imports_category_price() -> Dataset:
    """Get import prices by sector.

    Returns
    -------
    Import prices by sector : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_imports_origin_value() -> Dataset:
    """Get import values by origin.

    Returns
    -------
    Import values by origin : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_imports_origin_volume() -> Dataset:
    """Get import volumes by origin.

    Returns
    -------
    Import volumes by origin : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_imports_origin_price() -> Dataset:
    """Get import prices by origin.

    Returns
    -------
    Import prices by origin : Dataset

    """
    name = get_name_from_function()
    return _get_trade(name)


def trade_balance(*args, **kwargs) -> Dataset:
    """
    Get net trade balance data by country/region.

    Returns
    -------
    Net trade balance value by region/country : Dataset

    """
    name = get_name_from_function()
    exports = (
        load_dataset("trade_exports_destination_value", *args, **kwargs)
        .to_named()
        .rename(columns={"Total exportaciones": "Total"})
    )
    imports = (
        load_dataset("trade_imports_origin_value", *args, **kwargs)
        .to_named()
        .rename(columns={"Total importaciones": "Total"})
    )
    output = exports - imports
    output = output.rename_axis(None)

    spanish_names = exports.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def terms_of_trade(*args, **kwargs) -> Dataset:
    """
    Get terms of trade.

    Returns
    -------
    Terms of trade (exports/imports) : Dataset

    """
    name = get_name_from_function()
    exports = (
        load_dataset("trade_exports_destination_price", *args, **kwargs)
        .to_named()
        .rename(columns={"Total exportaciones": "Total"})
    )
    imports = (
        load_dataset("trade_imports_origin_price", *args, **kwargs)
        .to_named()
        .rename(columns={"Total importaciones": "Total"})
    )

    output = exports / imports
    output = output.loc[:, ["Total"]]
    output = output.rename(columns={"Total": "Términos de intercambio"})
    output = output.rename_axis(None)

    spanish_names = exports.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "2005=100",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)
    dataset = dataset.rebase("2005-01-01", "2005-12-31", 100)
    dataset.metadata.update_dataset_metadata({"unit": "2005=100"})
    dataset.transformed = False

    return dataset


def _commodity_weights() -> pd.DataFrame:
    raw = pd.read_csv(
        "https://raw.githubusercontent.com/rxavier/econuy-extras/main/econuy_extras/manual_data/comtrade.csv"
    )

    table = raw.groupby(["RefYear", "CmdDesc"]).sum().reset_index()
    table = table.pivot(index="RefYear", columns="CmdDesc", values="PrimaryValue")
    table.fillna(0, inplace=True)

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

    return output


def commodity_prices() -> Dataset:
    """Get commodity prices for Uruguay.

    Returns
    -------
    Commodity prices : Dataset
        Prices and price indexes of relevant commodities for Uruguay.

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw_beef = pd.read_excel(
        sources["beef"], header=4, index_col=0, thousands=".", usecols="A:D"
    ).dropna(how="all")

    raw_beef.columns = raw_beef.columns.str.strip()
    proc_beef = raw_beef["Ing. Prom./Ton."].to_frame()
    proc_beef.index = pd.date_range(
        start="2002-01-04", periods=len(proc_beef), freq="W-SAT"
    )
    beef = proc_beef.resample("ME").mean()

    milk_r = httpx.get(sources["milk1"])
    xls = re.findall(
        r"https://www.inale.org/wp-content/uploads/[0-9\/]+/Precios-exportacion-de-Europa.xls",
        milk_r.text,
        flags=re.IGNORECASE,
    )[0]
    raw_milk = pd.read_excel(
        xls,
        skiprows=13,
        nrows=dt.datetime.now().year - 2006,
    )
    raw_milk.dropna(how="all", axis=1, inplace=True)
    raw_milk.drop(["Promedio ", "Variación"], axis=1, inplace=True)
    raw_milk.columns = ["Año/Mes"] + list(range(1, 13))
    proc_milk = pd.melt(raw_milk, id_vars=["Año/Mes"])
    proc_milk["Año/Mes"] = pd.to_numeric(proc_milk["Año/Mes"], errors="coerce")
    proc_milk = proc_milk.dropna(subset=["Año/Mes"])
    proc_milk = proc_milk.sort_values(by=["Año/Mes", "variable"])
    proc_milk.index = pd.date_range(
        start="2007-01-31", periods=len(proc_milk), freq="ME"
    )
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
        prev_milk[prev_milk.index.notna()]
        .dropna(axis=0, how="all")
        .mean(axis=1)
        .to_frame()
    )
    prev_milk = prev_milk.set_index(
        pd.date_range(start="1977-01-31", freq="ME", periods=len(prev_milk))
    )
    eurusd_r = httpx.get(
        "https://fx.sauder.ubc.ca/cgi/fxdata",
        params=f"b=USD&c=EUR&rd=&fd=1&fm=1&fy=2001&ld=31&lm=12&ly="
        f"{dt.datetime.now().year}&y=monthly&q=volume&f=html&o=",
    )
    eurusd = pd.read_html(BytesIO(eurusd_r.content))[0].drop("MMM YYYY", axis=1)
    eurusd.index = pd.date_range(start="2001-01-31", periods=len(eurusd), freq="ME")
    eurusd_milk = eurusd.reindex(prev_milk.index)
    prev_milk = prev_milk.divide(eurusd_milk.values).multiply(10)
    prev_milk = prev_milk.loc[prev_milk.index < min(proc_milk.index)]
    prev_milk.columns, proc_milk.columns = ["Price"], ["Price"]
    milk = pd.concat([prev_milk, proc_milk])

    raw_pulp_r = httpx.get(sources["pulp"].format(year=dt.date.today().year))
    temp_dir = tempfile.TemporaryDirectory()
    with zipfile.ZipFile(BytesIO(raw_pulp_r.content), "r") as f:
        f.extractall(path=temp_dir.name)
        path_temp = path.join(temp_dir.name, "monthly_values.csv")
        raw_pulp = pd.read_csv(path_temp, sep=";").dropna(how="any")
    proc_pulp = raw_pulp.copy().sort_index(ascending=False)
    proc_pulp.index = pd.date_range(
        start="1990-01-31", periods=len(proc_pulp), freq="ME"
    )
    proc_pulp = proc_pulp.drop(["Label", "Codes"], axis=1).astype(float)
    proc_pulp = proc_pulp.div(eurusd.reindex(proc_pulp.index).values)
    pulp = proc_pulp

    r_imf = httpx.get(sources["imf"])
    imf = re.findall("external-data.+ashx", r_imf.text)[0]
    imf = f"https://imf.org/-/media/Files/Research/CommodityPrices/Monthly/{imf}"
    raw_imf = pd.read_excel(imf).dropna(how="all", axis=1).dropna(how="all", axis=0)
    raw_imf.columns = raw_imf.iloc[0, :]
    proc_imf = raw_imf.iloc[3:, 1:]
    proc_imf.index = pd.date_range(start="1990-01-31", periods=len(proc_imf), freq="ME")
    rice = proc_imf[proc_imf.columns[proc_imf.columns.str.contains("Rice")]]
    wood = proc_imf[proc_imf.columns[proc_imf.columns.str.contains("Sawnwood")]]
    wood = wood.mean(axis=1).to_frame()
    wool = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Wool")]]
    wool = wool.mean(axis=1).to_frame()
    barley = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Barley")]]
    gold = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Gold")]]
    soybean = proc_imf[
        proc_imf.columns[proc_imf.columns.str.startswith("Soybeans, U.S.")]
    ]
    wheat = proc_imf[proc_imf.columns[proc_imf.columns.str.startswith("Wheat")]]

    output = pd.concat(
        [beef, pulp, soybean, milk, rice, wood, wool, barley, gold, wheat], axis=1
    )
    output = output.reindex(beef.index).dropna(thresh=8)
    output.columns = [
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
    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "-",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    units = [
        "USD per ton",
        "USD per ton",
        "USD per ton",
        "USD per ton",
        "USD per ton",
        "USD per m3",
        "US cent. per kg",
        "USD per ton",
        "USD per onza troy",
        "USD per ton",
    ]
    for indicator, unit in zip(ids, units):
        metadata.update_indicator_metadata_value(indicator, "unit", unit)

    dataset = Dataset(name, output, metadata)

    return dataset


def commodity_index(*args, **kwargs) -> Dataset:
    """Get export-weighted commodity price index for Uruguay.

    Returns
    -------
    Monthly export-weighted commodity index : Dataset
        Export-weighted average of commodity prices relevant to Uruguay.

    """
    name = get_name_from_function()
    prices = load_dataset("commodity_prices", *args, **kwargs).to_named()
    prices = prices.interpolate(method="linear", limit=1).dropna(how="any")
    prices = prices.pct_change(periods=1)
    weights = _commodity_weights()
    weights = weights[prices.columns]
    weights = weights.reindex(prices.index, method="ffill")

    output = pd.DataFrame(
        prices.values * weights.values, columns=prices.columns, index=prices.index
    )
    output = output.sum(axis=1).add(1).to_frame().cumprod().multiply(100)
    output.columns = ["Índice de precios de productos primarios"]
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "2002-01=100",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def rxr() -> Dataset:
    """Get official (BCU) real exchange rates.

    Returns
    -------
    Monthly real exchange rates vs select countries/regions : Dataset
        Available: global, regional, extraregional, Argentina, Brazil, US.

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        raw = pd.read_excel(sources["main"], skiprows=8, usecols="B:N", index_col=0)
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            r_bytes = get_with_ssl_context("bcu", sources["main"])
            raw = pd.read_excel(r_bytes, skiprows=9, usecols="B:N", index_col=0)
    output = raw.dropna(how="any")
    output.columns = [
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
    output.index = pd.to_datetime(output.index) + MonthEnd(1)
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": "UYU/other",
        "inflation_adjustment": None,
        "unit": "2019=100",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def rxr_custom(*args, **kwargs) -> Dataset:
    """Get custom real exchange rates vis-à-vis the US, Argentina and Brazil.

    Returns
    -------
    Monthly real exchange rates vs select countries : Dataset
        Available: Argentina, Brazil, US.

    """
    name = get_name_from_function()

    ifs = regional._ifs(*args, **kwargs)
    uy_cpi = load_dataset("cpi", *args, **kwargs).to_named()
    uy_e = load_dataset("nxr_monthly", *args, **kwargs).to_named().iloc[:, [1]]
    proc = pd.concat([ifs, uy_cpi, uy_e], axis=1)
    proc = proc.interpolate(method="linear", limit_area="inside")
    proc = proc.dropna(how="all")
    proc.columns = ["AR_E_O", "AR_E_U", "BR_E", "AR_P", "BR_P", "US_P", "UY_P", "UY_E"]

    output = pd.DataFrame()
    output["UY_E_P"] = proc["UY_E"] / proc["UY_P"]
    output["Uruguay-Argentina oficial"] = (
        output["UY_E_P"] / proc["AR_E_O"] * proc["AR_P"]
    )
    output["Uruguay-Argentina informal"] = (
        output["UY_E_P"] / proc["AR_E_U"] * proc["AR_P"]
    )
    output["Uruguay-Brasil"] = output["UY_E_P"] / proc["BR_E"] * proc["BR_P"]
    output["Uruguay-EE.UU."] = output["UY_E_P"] * proc["US_P"]
    output = output.drop("UY_E_P", axis=1)
    output = output.loc[output.index >= "1979-12-01"]
    output = output.rename_axis(None)
    output = output.dropna(how="all")

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": "UYU/other",
        "inflation_adjustment": None,
        "unit": "2019=100",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, currency in zip(ids, ["UYU/ARS", "UYU/ARS", "UYU/BRL", "UYU/USD"]):
        metadata.update_indicator_metadata_value(indicator, "currency", currency)
    dataset = Dataset(name, output, metadata).rebase("2010-01-01", "2010-12-31", 100)
    dataset.metadata.update_dataset_metadata({"unit": "2010=100"})
    dataset.transformed = False

    return dataset


def balance_of_payments() -> Dataset:
    """Get balance of payments.

    Returns
    -------
    Quarterly balance of payments : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        raw = (
            pd.read_excel(
                sources["main"], skiprows=7, index_col=0, sheet_name="Cuadro Nº 1"
            )
            .dropna(how="all")
            .T
        )
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            r_bytes = get_with_ssl_context("bcu", sources["main"])
            raw = (
                pd.read_excel(
                    r_bytes,
                    skiprows=7,
                    index_col=0,
                    sheet_name="Cuadro Nº 1",
                )
                .dropna(how="all")
                .T
            )
    output = raw.iloc[:, 2:]
    output.index = pd.date_range(start="2012-03-31", freq="QE-DEC", periods=len(output))
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
    output.columns = BOP_COLUMNS
    output = output.apply(pd.to_numeric, errors="coerce")

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def balance_of_payments_summary(*args, **kwargs) -> Dataset:
    """Get a balance of payments summary and capital flows calculations.

    Returns
    -------
    Quarterly balance of payments summary : Dataset

    """
    name = get_name_from_function()
    bop = load_dataset("balance_of_payments", *args, **kwargs).to_named()

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
    output["Inversión directa en Uruguay"] = bop[
        "Inversión directa - Pasivos netos incurridos"
    ]
    output["Balance de inversión de cartera"] = bop["Inversión de cartera"]
    output["Inversión de cartera en el exterior"] = bop[
        "Inversión de cartera - Adquisición neta de activos fin"
    ]
    output["Inversión de cartera en Uruguay"] = bop[
        "Inversión de cartera - Pasivos netos incurridos"
    ]
    output["Saldo de derivados financieros"] = bop[
        "Derivados financieros - distintos de reservas"
    ]
    output["Balance de otra inversión"] = bop["Otra inversión"]
    output["Otra inversión en el exterior"] = bop[
        "Otra inversión - Adquisición neta de activos financieros"
    ]
    output["Otra inversión en Uruguay"] = bop[
        "Otra inversión - Pasivos netos incurridos"
    ]
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

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def international_reserves() -> Dataset:
    """Get international reserves data.

    Returns
    -------
    Daily international reserves : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    try:
        raw = pd.read_excel(
            sources["main"], usecols="D:J", index_col=0, skiprows=5, na_values="n/d"
        )
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            r_bytes = get_with_ssl_context("bcu", sources["main"])
            raw = pd.read_excel(
                r_bytes,
                usecols="D:J",
                index_col=0,
                skiprows=5,
                na_values="n/d",
            )
    proc = raw.dropna(thresh=1)
    output = proc[proc.index.notnull()]
    output.columns = [
        "Activos de reserva",
        "Otros activos externos de corto plazo",
        "Obligaciones en ME con el sector público",
        "Obligaciones en ME con el sector financiero",
        "Activos de reserva sin sector público y financiero",
        "Posición en ME del BCU",
    ]
    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.loc[~output.index.duplicated(keep="first")]
    output.index = pd.to_datetime(output.index)
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "External sector",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


# @retry(
#     retry_on_exceptions=(HTTPError, URLError),
#     max_calls_total=10,
#     retry_window_after_first_call_in_seconds=90,
# )
# def international_reserves_changes(
#     pipeline: Optional[Pipeline] = None, previous_data: pd.DataFrame = pd.DataFrame()
# ) -> pd.DataFrame:
#     """Get international reserves changes data.

#     Parameters
#     ----------
#     pipeline : econuy.core.Pipeline or None, default None
#         An instance of the econuy Pipeline class.
#     previous_data : pd.DataFrame
#         A DataFrame representing this dataset used to extract last
#         available dates.

#     Returns
#     -------
#     Monthly international reserves changes : pd.DataFrame

#     """
#     name = get_name_from_function()
#     sources = get_download_sources(name)

#     if pipeline is None:
#         pipeline = Pipeline()
#     if previous_data.empty:
#         first_year = 2013
#     else:
#         first_year = previous_data.index[-1].year

#     mapping = dict(
#         zip(
#             [
#                 "Ene",
#                 "Feb",
#                 "Mar",
#                 "Abr",
#                 "May",
#                 "Jun",
#                 "Jul",
#                 "Ago",
#                 "Set",
#                 "Oct",
#                 "Nov",
#                 "Dic",
#             ],
#             [str(x).zfill(2) for x in range(1, 13)],
#         )
#     )
#     inverse_mapping = {v: k for k, v in mapping.items()}
#     mapping.update({"Sep": "09"})

#     current_year = dt.date.today().year
#     months = []
#     for year in range(first_year, current_year + 1):
#         if year < current_year:
#             filename = f"dic{year}.xls"
#         else:
#             current_month = inverse_mapping[str(dt.date.today().month).zfill(2)]
#             last_month = inverse_mapping[
#                 str((dt.date.today() + relativedelta(months=-1)).month).zfill(2)
#             ]
#             certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
#             filename = f"{current_month}{year}.xls"
#             r = httpx.get(f"{sources['main']}{filename}", verify=certs_path)
#             if r.status_code == 404:
#                 filename = f"{last_month}{year}.xls"
#         try:
#             data = pd.read_excel(
#                 f"{sources['main']}{filename}",
#                 skiprows=2,
#                 sheet_name="ACTIVOS DE RESERVA",
#             )
#         except URLError as err:
#             if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
#                 certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
#                 r = httpx.get(f"{sources['main']}{filename}", verify=certs_path)
#                 data = pd.read_excel(
#                     r.content,
#                     skiprows=2,
#                     sheet_name="ACTIVOS DE RESERVA",
#                 )
#         data = data.dropna(how="all").dropna(how="all", axis=1).set_index("CONCEPTOS")
#         if data.columns[0] == "Mes":
#             data.columns = data.iloc[0, :]
#         data = (
#             data.iloc[1:]
#             .T.reset_index(names="date")
#             .loc[
#                 lambda x: ~x["date"]
#                 .astype(str)
#                 .str.contains("Unnamed|Trimestre|Año|I", regex=True, case=True),
#                 lambda x: x.columns.notna(),
#             ]
#         )
#         data["date"] = data["date"].replace("Mes\n", "", regex=True).str.strip()
#         data = data.loc[data["date"].notna()]

#         index = pd.Series(data["date"]).str.split("-", expand=True).replace(mapping)
#         index = pd.to_datetime(
#             index.iloc[:, 0] + "-" + index.iloc[:, 1], format="%m-%Y", errors="coerce"
#         ) + pd.tseries.offsets.MonthEnd(1)
#         if year == 2019:
#             index = index.fillna(dt.datetime(year, 1, 31))
#         elif year == 2013:
#             index = index.fillna(dt.datetime(year, 12, 31))
#         data["date"] = index
#         data.columns = ["date"] + RESERVES_COLUMNS
#         months.append(data)
#     reserves = (
#         pd.concat(months, sort=False, ignore_index=True)
#         .drop_duplicates(subset="date")
#         .dropna(subset="date")
#         .set_index("date")
#         .sort_index()
#         .rename_axis(None)
#     )
#     metadata._set(
#         reserves,
#         area="Sector externo",
#         currency="USD",
#         inf_adj="No",
#         unit="Millones",
#         seas_adj="NSA",
#         ts_type="Flujo",
#         cumperiods=1,
#     )
#     reserves.columns = reserves.columns.set_levels(["-"], level=2)

#     return reserves
