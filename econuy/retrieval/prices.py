import warnings
from urllib.error import URLError, HTTPError
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from scipy.stats import stats


from econuy.core import Pipeline
from econuy.utils import metadata, get_project_root
from econuy.utils.ops import get_name_from_function, get_download_sources


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def cpi() -> pd.DataFrame:
    """Get CPI data.

    Returns
    -------
    Monthly CPI : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], usecols="C").dropna(axis=0, how="any")
    output = raw.set_index(
        pd.date_range(start="1937-07-31", freq="M", periods=len(raw))
    ).rename_axis(None)
    output.columns = ["Índice de precios al consumo"]
    output = output.apply(pd.to_numeric, errors="coerce")

    metadata._set(
        output,
        area="Precios",
        currency="-",
        inf_adj="No",
        unit="2022-10=100",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def cpi_divisions() -> pd.DataFrame:
    """Get CPI data by division.

    Returns
    -------
    Monthly CPI by division : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    raw = (
        pd.read_excel(sources["main"], usecols="A:D")
        .dropna(axis=0, how="any")
        .assign(date=lambda x: x["Año"].astype(str) + x["Mes"].astype(str).str.pad(2, "left", "0"))
        .pivot(columns="División", values="Indice Total País", index="date")
    )
    output = raw.set_index(
        pd.date_range(start="2010-12-31", freq="M", periods=len(raw))
    ).rename_axis(None)
    colnames = [
        "ALIMENTOS Y BEBIDAS NO ALCOHÓLICAS",
        "BEBIDAS ALCOHÓLICAS, TABACO Y NARCÓTICOS",
        "ROPA Y CALZADO",
        "VIVIENDA, AGUA, ELECTRICIDAD, GAS Y OTROS COMBUSTIBLES",
        "MOBILIARIO, ENSERES DOMÉSTICOS y DEMÁS ARTÍCULOS REGULARES DE LOS HOGARES",
        "SALUD",
        "TRANSPORTE",
        "INFORMACIÓN Y COMUNICACIÓN",
        "RECREACIÓN, DEPORTE Y CULTURA",
        "SERVICIOS DE EDUCACIÓN",
        "RESTAURANTES Y SERVICIOS DE ALOJAMIENTO",
        "SEGUROS Y SERVICIOS FINANCIEROS",
        "CUIDADO PERSONAL, PROTECCIÓN SOCIAL Y BIENES DIVERSOS",
    ]
    output.columns = [x.capitalize() for x in colnames]
    output = output.apply(pd.to_numeric, errors="coerce")

    metadata._set(
        output,
        area="Precios",
        currency="-",
        inf_adj="No",
        unit="2022-10=100",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return output


# @retry(
#     retry_on_exceptions=(HTTPError, URLError),
#     max_calls_total=4,
#     retry_window_after_first_call_in_seconds=60,
# )
# def cpi_classes() -> pd.DataFrame:
#     """Get CPI data by division, group and class.

#     Returns
#     -------
#     Monthly CPI by division, group and class : pd.DataFrame

#     """
#     name = "cpi_classes"

#     raw = []
#     nmonths = 0
#     with pd.ExcelFile(urls[name]["dl"]["main"]) as excel:
#         for sheet in excel.sheet_names:
#             data = (
#                 pd.read_excel(excel, sheet_name=sheet, skiprows=8, nrows=147)
#                 .dropna(how="all")
#                 .dropna(how="all", axis=1)
#             )
#             data["Unnamed: 1"] = np.where(
#                 data["Unnamed: 0"].str.len() == 2,
#                 "Div_" + data["Unnamed: 1"],
#                 np.where(
#                     data["Unnamed: 0"].str.len() == 3,
#                     "Gru_" + data["Unnamed: 1"],
#                     "Cls_" + data["Unnamed: 1"],
#                 ),
#             )
#             data = data.iloc[:, 1:].T
#             data.columns = data.iloc[0]
#             data = data.iloc[1:]
#             data = data.loc[data.index.str.contains("Índice")].reset_index(drop=True).iloc[:, 1:]
#             data.rename(columns={"Cls_Índice General": "Índice General"}, inplace=True)
#             data.index = range(nmonths, nmonths + len(data))
#             data.rename(
#                 columns={
#                     "Div_Bebidas Alcoholicas, Tabaco y Estupefacientes": "Div_Bebidas Alcohólicas, Tabaco y Estupefacientes",
#                     "Div_Muebles, Artículos Para el Hogar y Para la Coservación Oridnaria del Hogar": "Div_Muebles, Artículos Para el Hogar y Para la Conservación Ordinaria del Hogar",
#                     "Gru_Enseñanza no atribuíble a ningún nivel": "Gru_Enseñanza no atribuible a ningún nivel",
#                     "Cls_Enseñanza no atribuíble a ningún nivel": "Cls_Enseñanza no atribuible a ningún nivel",
#                     "Cls_Otros aparatos, articulos y productos para la atención personal": "Cls_Otros aparatos, artículos y productos para la atención personal",
#                 },
#                 inplace=True,
#             )
#             raw.append(data)
#             nmonths = nmonths + len(data)
#     output = pd.concat(raw)
#     output.index = pd.date_range("2011-01-31", freq="M", periods=len(output))
#     output.rename_axis(None, inplace=True)
#     output = output.apply(pd.to_numeric, errors="coerce")

#     metadata._set(
#         output,
#         area="Precios",
#         currency="-",
#         inf_adj="No",
#         unit="2010-12=100",
#         seas_adj="NSA",
#         ts_type="-",
#         cumperiods=1,
#     )

#     return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def inflation_expectations() -> pd.DataFrame:
    """Get data for the BCU inflation expectations survey.

    Returns
    -------
    Monthly inflation expectations : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    try:
        raw = pd.read_excel(sources["main"], skiprows=9)
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = requests.get(sources["main"], verify=certs_path)
            raw = pd.read_excel(r.content, skiprows=9)
    raw = raw.dropna(how="all", axis=1).dropna(thresh=4)
    mask = raw.iloc[-12:].isna().all()
    output = raw.loc[:, ~mask]

    output.columns = (
        ["Fecha"]
        + ["Inflación mensual del mes corriente"] * 5
        + ["Inflación para los próximos 6 meses"] * 5
        + ["Inflación anual del año calendario corriente"] * 5
        + ["Inflación anual de los próximos 12 meses"] * 5
        + ["Inflación anual del año calendario siguiente"] * 5
        + ["Inflación anual de los próximos 24 meses"] * 5
        + ["Inflación anual a dos años calendario"] * 5
    )
    output.set_index("Fecha", inplace=True, drop=True)
    output.columns = output.columns + " - " + output.iloc[0]
    output = output.iloc[1:]
    output.rename_axis(None, inplace=True)
    output.index = pd.date_range(start="2004-01-31", freq="M", periods=len(output))
    output = output.apply(pd.to_numeric, errors="coerce")

    metadata._set(
        output,
        area="Precios",
        currency="-",
        inf_adj="No",
        unit="%",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return output


# @retry(
#     retry_on_exceptions=(HTTPError, URLError),
#     max_calls_total=4,
#     retry_window_after_first_call_in_seconds=60,
# )
# def utilities() -> pd.DataFrame:
#     """Get prices for government-owned utilities.

#     Returns
#     -------
#     Monthly utilities prices : pd.DataFrame

#     """
#     name = "utilities"

#     cpi_1997 = pd.read_excel(urls[name]["dl"]["1997"], skiprows=5, nrows=529).dropna()
#     products = [
#         "    Electricidad",
#         "        Supergas",
#         "        Agua corriente",
#         "      Combustibles Liquidos",
#         "      Servicio Telefonico",
#     ]
#     cpi_1997 = cpi_1997.loc[
#         cpi_1997["Rubros, Agrupaciones, Subrubros, Familias y Artículos"].isin(products)
#     ]
#     cpi_1997 = cpi_1997.T.iloc[1:]
#     cpi_1997.columns = [
#         "Electricidad",
#         "Supergás",
#         "Combustibles líquidos",
#         "Agua corriente",
#         "Servicio telefónico",
#     ]
#     cpi_1997.index = pd.date_range(start="1997-03-31", freq="M", periods=len(cpi_1997))
#     cpi_1997.rename_axis(None, inplace=True)

#     with pd.ExcelFile(urls[name]["dl"]["2019"]) as excel:
#         cpi_2010 = []
#         for sheet in excel.sheet_names:
#             data = pd.read_excel(excel, sheet_name=sheet, skiprows=8, nrows=640).dropna(how="all")
#             products = [
#                 "04510010",
#                 "04520020",
#                 "07221",
#                 "04410010",
#                 "08300010",
#                 "08300020",
#                 "08300030",
#             ]
#             data = data.loc[
#                 data["Unnamed: 0"].isin(products),
#                 data.columns.str.contains("Indice|Índice", regex=True),
#             ].T
#             data.columns = [
#                 "Agua corriente",
#                 "Electricidad",
#                 "Supergás",
#                 "Combustibles líquidos",
#                 "Servicio de telefonía fija",
#                 "Servicio de telefonía móvil",
#                 "Servicio de Internet",
#             ]
#             cpi_2010.append(data)
#         cpi_2010 = pd.concat(cpi_2010)
#         cpi_2010.index = pd.date_range(start="2010-12-31", freq="M", periods=len(cpi_2010))

#     output = pd.concat([cpi_1997 / cpi_1997.iloc[-1] * 100, cpi_2010])
#     output = output.loc[~output.index.duplicated(keep="last")]
#     output.at[pd.Timestamp(2010, 12, 31), "Servicio telefónico"] = 100

#     metadata._set(
#         output,
#         area="Precios",
#         currency="-",
#         inf_adj="No",
#         unit="2010-12=100",
#         seas_adj="NSA",
#         ts_type="-",
#         cumperiods=1,
#     )

#     return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=30,
)
def ppi() -> pd.DataFrame:
    """Get PPI data.

    Returns
    -------
    Monthly PPI : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=7, index_col=0).dropna()

    raw.columns = [
        "Índice general",
        "Ganadería, agricultura y silvicultura",
        "Pesca",
        "Explotación de minas y canteras",
        "Industrias manufactureras",
    ]
    raw.rename_axis(None, inplace=True)
    raw.index = raw.index + MonthEnd(1)
    output = raw.apply(pd.to_numeric, errors="coerce")

    metadata._set(
        output,
        area="Precios",
        currency="-",
        inf_adj="No",
        unit="2010-03=100",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=30,
)
def nxr_monthly(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """Get monthly nominal exchange rate data.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Monthly nominal exchange rates : pd.DataFrame
        Sell rate, monthly average and end of period.

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get("nxr_daily")
    nxr_daily = pipeline.dataset
    output = pd.DataFrame(
        {
            "Tipo de cambio venta, fin de período": nxr_daily.iloc[:, 0].resample("M").last(),
            "Tipo de cambio venta, promedio": nxr_daily.iloc[:, 0].resample("M").mean(),
        }
    )

    historical = pd.read_excel(
        sources["historical"], skiprows=4, index_col=0, usecols="A,C,F"
    ).dropna(how="any", axis=0)
    historical.columns = ["Tipo de cambio venta, fin de período", "Tipo de cambio venta, promedio"]
    historical.index = pd.to_datetime(historical.index) + MonthEnd(1)
    historical = historical.apply(pd.to_numeric, errors="coerce")
    historical = historical.loc[:"2001-09-30", :]

    output = pd.concat([historical, output])
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Precios",
        currency="UYU/USD",
        inf_adj="No",
        unit="-",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=10,
    retry_window_after_first_call_in_seconds=60,
)
def nxr_daily() -> pd.DataFrame:
    """Get daily nominal exchange rate data.

    Returns
    -------
    Daily nominal exchange rates : pd.DataFrame
        Sell rate.

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=7, usecols="A:E").dropna(thresh=2)
    raw["Unnamed: 1"] = (
        raw["Unnamed: 1"]
        .str.slice(stop=3)
        .fillna(method="ffill")
        .replace({"Dic": "Dec", "Ene": "Jan", "Abr": "Apr", "Ago": "Aug", "Set": "Sep"})
    )
    raw["Unnamed: 2"] = raw["Unnamed: 2"].fillna(method="ffill").astype(int)
    raw.index = pd.to_datetime(
        raw[["Unnamed: 2", "Unnamed: 1", "Unnamed: 0"]].astype(str).agg("-".join, axis=1),
        format="%Y-%b-%d",
    )
    output = raw.loc["2001-10-01":, ["Unnamed: 4"]].rename(
        columns={"Unnamed: 4": "Tipo de cambio venta"}
    )
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Precios",
        currency="UYU/USD",
        inf_adj="No",
        unit="-",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return output


# The `_contains_nan` function needs to be monkey-patched to avoid an error
# when checking whether a Series is True
def _new_contains_nan(a, nan_policy="propagate"):
    policies = ["propagate", "raise", "omit"]
    if nan_policy not in policies:
        raise ValueError(
            "nan_policy must be one of {%s}" % ", ".join("'%s'" % s for s in policies)
        )
    try:
        with np.errstate(invalid="ignore"):
            # This [0] gets the value instead of the array, fixing the error
            contains_nan = np.isnan(np.sum(a))[0]
    except TypeError:
        try:
            contains_nan = np.nan in set(a.ravel())
        except TypeError:
            contains_nan = False
            nan_policy = "omit"
            warnings.warn(
                "The input array could not be properly checked for "
                "nan values. nan values will be ignored.",
                RuntimeWarning,
            )

    if contains_nan and nan_policy == "raise":
        raise ValueError("The input contains nan values")

    return contains_nan, nan_policy


stats._contains_nan = _new_contains_nan


# @retry(
#     retry_on_exceptions=(HTTPError, URLError),
#     max_calls_total=4,
#     retry_window_after_first_call_in_seconds=60,
# )
# def cpi_measures(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
#     """
#     Get core CPI, Winsorized CPI, tradabe CPI, non-tradable CPI and residual
#     CPI.

#     Parameters
#     ----------
#     pipeline : econuy.core.Pipeline or None, default None
#         An instance of the econuy Pipeline class.

#     Returns
#     -------
#     Monthly CPI measures : pd.DataFrame

#     """
#     if pipeline is None:
#         pipeline = Pipeline()

#     name = "cpi_measures"

#     xls_10 = pd.ExcelFile(urls[name]["dl"]["2010-"])
#     prod_97 = (
#         pd.read_excel(urls[name]["dl"]["1997"], skiprows=5)
#         .dropna(how="any")
#         .set_index("Rubros, Agrupaciones, Subrubros, Familias y Artículos")
#         .T
#     )

#     weights_97 = pd.read_excel(urls[name]["dl"]["1997_weights"], index_col=0).drop_duplicates(
#         subset="Descripción", keep="first"
#     )
#     weights = pd.read_excel(xls_10, usecols="A:C", skiprows=13, index_col=0).dropna(how="any")
#     weights.columns = ["Item", "Weight"]
#     weights_8 = weights.loc[weights.index.str.len() == 8]

#     sheets = []
#     for sheet in xls_10.sheet_names:
#         raw = pd.read_excel(xls_10, sheet_name=sheet, usecols="D:IN", skiprows=8).dropna(how="all")
#         proc = raw.loc[:, raw.columns.str.contains("Indice|Índice")].dropna(how="all")
#         sheets.append(proc.T)
#     complete_10 = pd.concat(sheets)
#     complete_10 = complete_10.iloc[:, 1:]
#     complete_10.columns = [weights["Item"], weights.index]
#     complete_10.index = pd.date_range(start="2010-12-31", periods=len(complete_10), freq="M")
#     diff_8 = complete_10.loc[
#         :, complete_10.columns.get_level_values(level=1).str.len() == 8
#     ].pct_change()
#     win = pd.DataFrame(winsorize(diff_8, limits=(0.05, 0.05), axis=1))
#     win.index = diff_8.index
#     win.columns = diff_8.columns.get_level_values(level=1)
#     cpi_win = win.mul(weights_8.loc[:, "Weight"].T)
#     cpi_win = cpi_win.sum(axis=1).add(1).cumprod().mul(100)

#     weights_97["Weight"] = (
#         weights_97["Rubro"]
#         .fillna(weights_97["Agrupación, subrubro, familia"])
#         .fillna(weights_97["Artículo"])
#         .drop(columns=["Rubro", "Agrupación, subrubro, familia", "Artículo"])
#     )
#     prod_97 = prod_97.loc[:, list(cpi_details["1997_base"].keys())]
#     prod_97.index = pd.date_range(start="1997-03-31", periods=len(prod_97), freq="M")
#     weights_97 = (
#         weights_97[weights_97["Descripción"].isin(cpi_details["1997_weights"])]
#         .set_index("Descripción")
#         .drop(columns=["Rubro", "Agrupación, subrubro, " "familia", "Artículo"])
#     ).div(100)
#     weights_97.index = prod_97.columns
#     prod_10 = complete_10.loc[:, list(cpi_details["2010_base"].keys())]
#     prod_10 = prod_10.loc[:, ~prod_10.columns.get_level_values(level=0).duplicated()]
#     prod_10.columns = prod_10.columns.get_level_values(level=0)
#     weights_10 = (
#         weights.loc[weights["Item"].isin(list(cpi_details["2010_base"].keys()))].drop_duplicates(
#             subset="Item", keep="first"
#         )
#     ).set_index("Item")
#     items = []
#     weights = []
#     for item, weight, details in zip(
#         [prod_10, prod_97], [weights_10, weights_97], ["2010_base", "1997_base"]
#     ):
#         for tradable in [True, False]:
#             items.append(
#                 item.loc[
#                     :, [k for k, v in cpi_details[details].items() if v["Tradable"] is tradable]
#                 ]
#             )
#             aux = weight.loc[
#                 [k for k, v in cpi_details[details].items() if v["Tradable"] is tradable]
#             ]
#             weights.append(aux.div(aux.sum()))
#         for core in [True, False]:
#             items.append(
#                 item.loc[:, [k for k, v in cpi_details[details].items() if v["Core"] is core]]
#             )
#             aux = weight.loc[[k for k, v in cpi_details[details].items() if v["Core"] is core]]
#             weights.append(aux.div(aux.sum()))

#     intermediate = []
#     for item, weight in zip(items, weights):
#         intermediate.append(item.mul(weight.squeeze()).sum(1))

#     output = []
#     for x, y in zip(intermediate[:4], intermediate[4:]):
#         aux = pd.concat(
#             [
#                 y.pct_change().loc[y.index < "2011-01-01"],
#                 x.pct_change().loc[x.index > "2011-01-01"],
#             ]
#         )
#         output.append(aux.fillna(0).add(1).cumprod().mul(100))

#     pipeline.get("cpi")
#     cpi_re = pipeline.dataset
#     cpi_re = cpi_re.loc[cpi_re.index >= "1997-03-31"]
#     output = pd.concat([cpi_re] + output + [cpi_win], axis=1)
#     output.columns = [
#         "Índice de precios al consumo: total",
#         "Índice de precios al consumo: transables",
#         "Índice de precios al consumo: no transables",
#         "Índice de precios al consumo: subyacente",
#         "Índice de precios al consumo: residual",
#         "Índice de precios al consumo: Winsorized 0.05",
#     ]
#     output = output.apply(pd.to_numeric, errors="coerce")
#     metadata._set(
#         output,
#         area="Precios",
#         currency="-",
#         inf_adj="No",
#         unit="2010-12=100",
#         seas_adj="NSA",
#         ts_type="-",
#         cumperiods=1,
#     )
#     output = transform.rebase(output, start_date="2010-12-01", end_date="2010-12-31")
#     output.rename_axis(None, inplace=True)

#     return output
