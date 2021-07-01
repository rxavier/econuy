import datetime as dt
import re
import tempfile
from pathlib import Path
from io import BytesIO
from os import listdir, path
from typing import List, Optional
from urllib.error import HTTPError, URLError

import pandas as pd
import patoolib
import requests
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd

from econuy import transform
from econuy.core import Pipeline
from econuy.utils import metadata, get_project_root
from econuy.utils.sources import urls


def _natacc_retriever(url: str, nrows: int, inf_adj: str,
                      unit: str, seas_adj: str,
                      colnames: List[str]) -> pd.DataFrame:
    """Helper function. See any of the `natacc_...()` functions."""

    raw = pd.read_excel(url,
                        skiprows=9, nrows=nrows, usecols="B:AAA",
                        index_col=0).dropna(how="all").T
    raw.index = raw.index.str.replace("*", "", regex=True)
    raw.index = raw.index.str.replace(r"\bI \b", "3-", regex=True)
    raw.index = raw.index.str.replace(r"\bII \b", "6-", regex=True)
    raw.index = raw.index.str.replace(r"\bIII \b", "9-", regex=True)
    raw.index = raw.index.str.replace(r"\bIV \b", "12-", regex=True)
    raw.index = pd.to_datetime(raw.index, format="%m-%Y") + MonthEnd(1)
    raw.columns = colnames
    output = raw.apply(pd.to_numeric, errors="coerce")
    if unit == "Millones":
        output = output.div(1000)
    output.rename_axis(None, inplace=True)

    metadata._set(output, area="Actividad económica", currency="UYU",
                  inf_adj=inf_adj, unit=unit, seas_adj=seas_adj,
                  ts_type="Flujo", cumperiods=1)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def natacc_ind_con_nsa() -> pd.DataFrame:
    """Get supply-side national accounts data in NSA constant prices, 2005-.

    Returns
    -------
    National accounts, supply side, constant prices, NSA : pd.DataFrame

    """
    colnames = ["Actividades primarias",
                "Act. prim.: Agricultura, ganadería, caza y silvicultura",
                "Industrias manufactureras",
                "Suministro de electricidad, gas y agua",
                "Construcción",
                "Comercio, reparaciones, restaurantes y hoteles",
                "Transporte, almacenamiento y comunicaciones",
                "Otras actividades",
                "Otras actividades: SIFMI",
                "Impuestos menos subvenciones",
                "Producto bruto interno"]
    return _natacc_retriever(url=urls["natacc_ind_con_nsa"]["dl"]["main"],
                             nrows=12, inf_adj="Const. 2005", unit="Millones",
                             seas_adj="NSA", colnames=colnames)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def natacc_gas_con_nsa() -> pd.DataFrame:
    """Get demand-side national accounts data in NSA constant prices, 2005-.

    Returns
    -------
    National accounts, demand side, constant prices, NSA : pd.DataFrame

    """
    colnames = ["Gasto: total", "Gasto: privado",
                "Gasto: público",
                "Formación bruta de capital",
                "Formación bruta de capital: fijo",
                "Formación bruta de capital: fijo - pública",
                "Formación bruta de capital: fijo - privada",
                "Exportaciones",
                "Importaciones", "Producto bruto interno"]
    return _natacc_retriever(url=urls["natacc_gas_con_nsa"]["dl"]["main"],
                             nrows=10, inf_adj="Const. 2005", unit="Millones",
                             seas_adj="NSA", colnames=colnames)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def natacc_ind_con_idx_sa() -> pd.DataFrame:
    """Get supply-side national accounts data in SA real index, 1997-.

    Returns
    -------
    National accounts, supply side, real index, SA : pd.DataFrame

    """
    colnames = ["Actividades primarias",
                "Act. prim.: Agricultura, ganadería, caza y silvicultura",
                "Industrias manufactureras",
                "Suministro de electricidad, gas y agua",
                "Construcción",
                "Comercio, reparaciones, restaurantes y hoteles",
                "Transporte, almacenamiento y comunicaciones",
                "Otras actividades",
                "Otras actividades: SIFMI",
                "Impuestos menos subvenciones",
                "Producto bruto interno"]
    return _natacc_retriever(url=urls["natacc_ind_con_idx_sa"]["dl"]["main"],
                             nrows=12, inf_adj="Const. 2005", unit="2005=100",
                             seas_adj="SA", colnames=colnames)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def natacc_ind_con_idx_nsa() -> pd.DataFrame:
    """Get supply-side national accounts data in NSA real index, 1997-.

    Returns
    -------
    National accounts, supply side, real index, NSA : pd.DataFrame

    """
    colnames = ["Actividades primarias",
                "Act. prim.: Agricultura, ganadería, caza y silvicultura",
                "Industrias manufactureras",
                "Suministro de electricidad, gas y agua",
                "Construcción",
                "Comercio, reparaciones, restaurantes y hoteles",
                "Transporte, almacenamiento y comunicaciones",
                "Otras actividades",
                "Otras actividades: SIFMI",
                "Impuestos menos subvenciones",
                "Producto bruto interno"]
    return _natacc_retriever(url=urls["natacc_ind_con_idx_nsa"]["dl"]["main"],
                             nrows=12, inf_adj="Const. 2005", unit="2005=100",
                             seas_adj="NSA", colnames=colnames)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def natacc_ind_cur_nsa() -> pd.DataFrame:
    """Get supply-side national accounts data in NSA current prices, 2005-.

    Returns
    -------
    National accounts, supply side, current prices, NSA : pd.DataFrame

    """
    colnames = ["Actividades primarias",
                "Act. prim.: Agricultura, ganadería, caza y silvicultura",
                "Industrias manufactureras",
                "Suministro de electricidad, gas y agua",
                "Construcción",
                "Comercio, reparaciones, restaurantes y hoteles",
                "Transporte, almacenamiento y comunicaciones",
                "Otras actividades",
                "Otras actividades: SIFMI",
                "Impuestos menos subvenciones",
                "Producto bruto interno"]
    return _natacc_retriever(url=urls["natacc_ind_cur_nsa"]["dl"]["main"],
                             nrows=12, inf_adj="No", unit="Millones",
                             seas_adj="NSA", colnames=colnames)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def natacc_gdp_cur_nsa() -> pd.DataFrame:
    """Get nominal GDP, 1997-.

    Returns
    -------
    Nominal GDP : pd.DataFrame

    """
    colnames = ["Producto bruto interno"]
    return _natacc_retriever(url=urls["natacc_gdp_cur_nsa"]["dl"]["main"],
                             nrows=2, inf_adj="No", unit="Millones",
                             seas_adj="NSA", colnames=colnames)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def _lin_gdp(pipeline: Optional[Pipeline] = None):
    """Get nominal GDP data in UYU and USD with forecasts.

    Update nominal GDP data for use in the `transform.convert_gdp()` function.
    Get IMF forecasts for year of last available data point and the next
    year (for example, if the last period available at the BCU website is
    september 2019, fetch forecasts for 2019 and 2020).

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    output : Pandas dataframe
        Quarterly GDP in UYU and USD with 1 year forecasts.

    """
    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get(name="natacc_gdp_cur_nsa")
    data_uyu = pipeline.dataset
    # TODO: use Pipeline methods for these
    data_uyu = transform.rolling(data_uyu, window=4, operation="sum")
    data_usd = transform.convert_usd(data_uyu, pipeline=pipeline)

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
    output.rename_axis(None, inplace=True)

    metadata._modify_multiindex(output, levels=[0],
                                new_arrays=[["PBI UYU", "PBI USD"]])

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def industrial_production() -> pd.DataFrame:
    """Get industrial production data.

    Returns
    -------
    Monthly industrial production index : pd.DataFrame

    """
    name = "industrial_production"

    try:
        raw = pd.read_excel(urls[name]["dl"]["main"],
                            skiprows=4, usecols="B:EM")
        weights = pd.read_excel(urls[name]["dl"]["weights"],
                                skiprows=3, usecols="B:E").dropna(how="all")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files",
                               "ine_certs.pem")
            r = requests.get(urls[name]["dl"]["main"],
                             verify=certificate)
            raw = pd.read_excel(BytesIO(r.content),
                                skiprows=4, usecols="B:EM")
            r = requests.get(urls[name]["dl"]["weights"],
                             verify=certificate)
            weights = pd.read_excel(BytesIO(r.content),
                                skiprows=3, usecols="B:E").dropna(how="all")
        else:
            raise err
    proc = raw.dropna(how="any", subset=["Mes"]).dropna(thresh=100, axis=1)
    output = proc[~proc["Mes"].str.contains("PROM|Prom",
                                            regex=True)].drop("Mes", axis=1)
    output.index = pd.date_range(start="2002-01-31", freq="M",
                                 periods=len(output))

    column_names = []
    for c in output.columns[2:]:
        match = weights.loc[weights["division"] == c, "Denominación"]
        prefix = "Div_"
        if isinstance(match, pd.Series) and match.empty:
            prefix = "Agr_"
            match = weights.loc[weights["agrupacion"] == c, "Denominación"]
            if isinstance(match, pd.Series) and match.empty:
                prefix = "Cls_"
                match = weights.loc[weights["clase"] == c, "Denominación"]
        try:
            match = match.iloc[0]
        except AttributeError:
            pass
        match = (prefix + match.capitalize().strip())[:-1]
        match = re.sub(r"[\(\)]", "-", match)
        if len(match) > 60:
            match = match[:58] + "..."
        column_names.append(match)
    output.columns = (["Industrias manufactureras",
                       "Industrias manufactureras sin refinería"]
                      + column_names)
    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="2006=100", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    return output


def core_industrial(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """
    Get total industrial production, industrial production excluding oil
    refinery and core industrial production.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Measures of industrial production : pd.DataFrame

    """
    name = "core_industrial"

    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get("industrial_production")
    data = pipeline.dataset

    try:
        weights = pd.read_excel(
            urls[name]["dl"]["weights"],
            skiprows=3).dropna(
            how="all")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files",
                               "ine_certs.pem")
            r = requests.get(urls[name]["dl"]["weights"],
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
    exclude = (data.loc[:, "Cls_Elaboración de productos alimenticios n.c.p"] * other_foods
                + data.loc[:, "Cls_Pulpa de madera, papel y cartón"] * pulp)
    core = data["Industrias manufactureras sin refinería"] - exclude
    core = pd.concat([core], keys=["Núcleo industrial"],
                     names=["Indicador"], axis=1)
    output = pd.concat([output, core], axis=1)
    output = transform.rebase(output, start_date="2006-01-01",
                              end_date="2006-12-31")
    output.rename_axis(None, inplace=True)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def cattle() -> pd.DataFrame:
    """Get weekly cattle slaughter data.

    Returns
    -------
    Weekly cattle slaughter : pd.DataFrame

    """
    name = "cattle"
    try:
        output = pd.read_excel(urls[name]["dl"]["main"],
                               skiprows=8, usecols="C:H")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files",
                               "inac_certs.pem")
            r = requests.get(urls[name]["dl"]["main"],
                             verify=certificate)
            output = pd.read_excel(BytesIO(r.content),
                                   skiprows=8, usecols="C:H")
        else:
            raise err

    output.index = pd.date_range(start="2005-01-02", freq="W",
                                 periods=len(output))
    output.rename_axis(None, inplace=True)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="Cabezas", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def milk() -> pd.DataFrame:
    """Get monthly milk production in farms data.

    Returns
    -------
    Monhtly milk production in farms : pd.DataFrame

    """
    name = "milk"

    r = requests.get(urls[name]["dl"]["main"])
    soup = BeautifulSoup(r.content, features="lxml")
    link = soup.find_all(href=re.compile(".xls"))[0]
    raw = pd.read_excel(link["href"], skiprows=11, skipfooter=4)
    output = raw.iloc[:, 2:].drop(0, axis=0)
    output = pd.melt(output, id_vars="Año/ Mes")[["value"]].dropna()
    output.index = pd.date_range(start="2002-01-31", freq="M",
                                 periods=len(output))
    output = output.apply(pd.to_numeric)
    output.columns = ["Remisión de leche a planta"]
    output.rename_axis(None, inplace=True)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="Miles de litros", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def cement() -> pd.DataFrame:
    """Get monthly cement sales data.

    Returns
    -------
    Monthly cement sales : pd.DataFrame

    """
    name = "cement"

    output = pd.read_excel(urls[name]["dl"]["main"], skiprows=2,
                           usecols="B:E", index_col=0, skipfooter=1)
    output.index = output.index + MonthEnd(0)
    output.columns = ["Exportaciones", "Mercado interno", "Total"]
    output.rename_axis(None, inplace=True)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="Toneladas", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)
    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def diesel() -> pd.DataFrame:
    """
    Get diesel sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly diesel dales : pd.DataFrame

    """
    name = "diesel"

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls[name]["dl"]["main"])
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
    output.rename_axis(None, inplace=True)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="m3", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def gasoline() -> pd.DataFrame:
    """
    Get gasoline sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly gasoline dales : pd.DataFrame

    """
    name = "gasoline"

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls[name]["dl"]["main"])
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
    output.rename_axis(None, inplace=True)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="m3", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def electricity() -> pd.DataFrame:
    """
    Get electricity sales by sector data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly electricity dales : pd.DataFrame

    """
    name = "electricity"

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls[name]["dl"]["main"])
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
    output.rename_axis(None, inplace=True)

    metadata._set(output, area="Actividad económica", currency="-",
                  inf_adj="No", unit="MWh", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    return output
