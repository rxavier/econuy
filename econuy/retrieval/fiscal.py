import datetime as dt
import re
from pathlib import Path
from typing import Dict, Optional
from urllib.error import HTTPError, URLError

import pandas as pd
import requests
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from requests.exceptions import ConnectionError

from econuy import transform
from econuy.core import Pipeline
from econuy.utils import metadata, get_project_root
from econuy.utils.extras import FISCAL_SHEETS, taxes_columns
from econuy.utils.operations import get_name_from_function, get_download_sources


@retry(
    retry_on_exceptions=(HTTPError, ConnectionError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def _balance_retriever() -> Dict[str, pd.DataFrame]:
    """Helper function. See any of the `balance_...()` functions."""
    sources = get_download_sources("fiscal_balance_global_public_sector")
    response = requests.get(sources["main"])
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all(href=re.compile("\\.xlsx$"))
    link = links[0]["href"]
    xls = pd.ExcelFile(link)
    output = {}
    for dataset, meta in FISCAL_SHEETS.items():
        data = (
            pd.read_excel(xls, sheet_name=meta["sheet"])
            .dropna(axis=0, thresh=4)
            .dropna(axis=1, thresh=4)
            .transpose()
            .set_index(2, drop=True)
        )
        data.columns = data.iloc[0]
        data = data[data.index.notnull()].rename_axis(None)
        data.index = data.index + MonthEnd(1)
        data.columns = meta["colnames"]
        data = data.apply(pd.to_numeric, errors="coerce")
        data.rename_axis(None, inplace=True)
        metadata._set(
            data,
            area="Sector público",
            currency="UYU",
            inf_adj="No",
            unit="Millones",
            seas_adj="NSA",
            ts_type="Flujo",
            cumperiods=1,
        )

        output.update({dataset: data})

    return output


def fiscal_balance_global_public_sector() -> pd.DataFrame:
    """Get fiscal balance data for the consolidated public sector.

    Returns
    -------
    Monthly fiscal balance for the consolidated public sector : pd.DataFrame

    """
    return _balance_retriever()["fiscal_balance_global_public_sector"]


def fiscal_balance_nonfinancial_public_sector() -> pd.DataFrame:
    """Get fiscal balance data for the non-financial public sector.

    Returns
    -------
    Monthly fiscal balance for the non-financial public sector : pd.DataFrame

    """
    return _balance_retriever()["fiscal_balance_nonfinancial_public_sector"]


def fiscal_balance_central_government() -> pd.DataFrame:
    """Get fiscal balance data for the central government + BPS.

    Returns
    -------
    Monthly fiscal balance for the central government + BPS : pd.DataFrame

    """
    return _balance_retriever()["fiscal_balance_central_government"]


def fiscal_balance_soe() -> pd.DataFrame:
    """Get fiscal balance data for public enterprises.

    Returns
    -------
    Monthly fiscal balance for public enterprises : pd.DataFrame

    """
    return _balance_retriever()["fiscal_balance_soe"]


def fiscal_balance_ancap() -> pd.DataFrame:
    """Get fiscal balance data for ANCAP.

    Returns
    -------
    Monthly fiscal balance for ANCAP : pd.DataFrame

    """
    return _balance_retriever()["fiscal_balance_ancap"]


def fiscal_balance_ute() -> pd.DataFrame:
    """Get fiscal balance data for UTE.

    Returns
    -------
    Monthly fiscal balance for UTE : pd.DataFrame

    """
    return _balance_retriever()["fiscal_balance_ute"]


def fiscal_balance_antel() -> pd.DataFrame:
    """Get fiscal balance data for ANTEL.

    Returns
    -------
    Monthly fiscal balance for ANTEL : pd.DataFrame

    """
    return _balance_retriever()["fiscal_balance_antel"]


def fiscal_balance_ose() -> pd.DataFrame:
    """Get fiscal balance data for OSE.

    Returns
    -------
    Monthly fiscal balance for OSE : pd.DataFrame

    """
    return _balance_retriever()["fiscal_balance_ose"]


@retry(
    retry_on_exceptions=(HTTPError, ConnectionError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def tax_revenue() -> pd.DataFrame:
    """
    Get tax revenues data.

    This retrieval function requires that Ghostscript and Tkinter be found in
    your system.

    Returns
    -------
    Monthly tax revenues : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    r = requests.get(sources["main"])
    url = re.findall("https://[A-z0-9-/\.]+Recaudaci%C3%B3n%20por%20impuesto%20-%20Series%20mensuales.xlsx", r.text)[0]
    raw = (
        pd.read_excel(url)
        .iloc[:, 2:]
        .drop(index=[0, 1])
        .drop(columns=["CONTROLES"])
        .set_index("SELECCIONE IMPUESTO")
        .rename_axis(None)
    )
    raw.index = pd.to_datetime(raw.index, format="%Y-%m-%d HH:MM:SS", errors="coerce") + MonthEnd(
        0
    )
    output = raw.loc[~pd.isna(raw.index), ~raw.columns.str.contains("Unnamed")]
    output.columns = taxes_columns
    output = output.div(1000000)
    latest = pd.read_csv(sources["pdfs"], index_col=0, parse_dates=True)
    latest = latest.loc[[x not in output.index for x in latest.index]]
    for col in latest.columns:
        for date in latest.index:
            prev_year = date + MonthEnd(-12)
            output.loc[date, col] = output.loc[prev_year, col] * (1 + latest.loc[date, col] / 100)
    output = pd.concat([output, latest], sort=False)
    output = output.loc[~output.index.duplicated(keep="first")]

    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)
    metadata._set(
        output,
        area="Sector público",
        currency="UYU",
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
    retry_window_after_first_call_in_seconds=30,
)
def _public_debt_retriever() -> Dict[str, pd.DataFrame]:
    """Helper function. See any of the `public_debt_...()` functions."""
    sources = get_download_sources("public_debt_global_public_sector")
    colnames = [
        "Total deuda",
        "Plazo contractual: hasta 1 año",
        "Plazo contractual: entre 1 y 5 años",
        "Plazo contractual: más de 5 años",
        "Plazo residual: hasta 1 año",
        "Plazo residual: entre 1 y 5 años",
        "Plazo residual: más de 5 años",
        "Moneda: pesos",
        "Moneda: dólares",
        "Moneda: euros",
        "Moneda: yenes",
        "Moneda: DEG",
        "Moneda: otras",
        "Residencia: no residentes",
        "Residencia: residentes",
    ]
    try:
        xls = pd.ExcelFile(sources["main"])
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certs_path = Path(get_project_root(), "utils", "files", "bcu_certs.pem")
            r = requests.get(sources["main"], verify=certs_path)
            xls = pd.ExcelFile(r.content)
    gps_raw = pd.read_excel(
        xls,
        sheet_name="SPG2",
        usecols="B:Q",
        index_col=0,
        skiprows=10,
        nrows=(dt.datetime.now().year - 1999) * 4,
    )
    gps = gps_raw.dropna(thresh=2)
    gps.index = pd.date_range(start="1999-12-31", periods=len(gps), freq="Q-DEC")
    gps.columns = colnames

    nfps_raw = pd.read_excel(xls, sheet_name="SPNM bruta", usecols="B:O", index_col=0)
    loc = nfps_raw.index.get_loc(
        "9. Deuda Bruta del Sector Público no " "monetario por plazo y  moneda."
    )
    nfps = nfps_raw.iloc[loc + 5 :, :].dropna(how="any")
    nfps.index = pd.date_range(start="1999-12-31", periods=len(nfps), freq="Q-DEC")
    nfps_extra_raw = pd.read_excel(
        xls,
        sheet_name="SPNM bruta",
        usecols="O:P",
        skiprows=11,
        nrows=(dt.datetime.now().year - 1999) * 4,
    )
    nfps_extra = nfps_extra_raw.dropna(how="all")
    nfps_extra.index = nfps.index
    nfps = pd.concat([nfps, nfps_extra], axis=1)
    nfps.columns = colnames

    cb_raw = pd.read_excel(
        xls,
        sheet_name="BCU bruta",
        usecols="B:O",
        index_col=0,
        skiprows=(dt.datetime.now().year - 1999) * 8 + 20,
    )
    cb = cb_raw.dropna(how="any")
    cb.index = pd.date_range(start="1999-12-31", periods=len(cb), freq="Q-DEC")
    cb_extra_raw = pd.read_excel(
        xls,
        sheet_name="BCU bruta",
        usecols="O:P",
        skiprows=11,
        nrows=(dt.datetime.now().year - 1999) * 4,
    )
    bcu_extra = cb_extra_raw.dropna(how="all")
    bcu_extra.index = cb.index
    cb = pd.concat([cb, bcu_extra], axis=1)
    cb.columns = colnames

    assets_raw = pd.read_excel(
        xls,
        sheet_name="Activos Neta",
        usecols="B,C,D,K",
        index_col=0,
        skiprows=13,
        nrows=(dt.datetime.now().year - 1999) * 4,
    )
    assets = assets_raw.dropna(how="any")
    assets.index = pd.date_range(start="1999-12-31", periods=len(assets), freq="Q-DEC")
    assets.columns = ["Total activos", "Sector público no monetario", "BCU"]

    output = {
        "public_debt_global_public_sector": gps,
        "public_debt_nonfinancial_public_sector": nfps,
        "public_debt_central_bank": cb,
        "public_assets": assets,
    }

    for meta, data in output.items():
        data.rename_axis(None, inplace=True)
        metadata._set(
            data,
            area="Sector público",
            currency="USD",
            inf_adj="No",
            unit="Millones",
            seas_adj="NSA",
            ts_type="Stock",
            cumperiods=1,
        )

        output.update({meta: data})

    return output


def public_debt_global_public_sector() -> pd.DataFrame:
    """Get public debt data for the consolidated public sector.

    Returns
    -------
    Quarterly public debt data for the consolidated public sector: pd.DataFrame

    """
    return _public_debt_retriever()["public_debt_global_public_sector"]


def public_debt_nonfinancial_public_sector() -> pd.DataFrame:
    """Get public debt data for the non-financial public sector.

    Returns
    -------
    Quarterly public debt data for the non-financial public sector: pd.DataFrame

    """
    return _public_debt_retriever()["public_debt_nonfinancial_public_sector"]


def public_debt_central_bank() -> pd.DataFrame:
    """Get public debt data for the central bank

    Returns
    -------
    Quarterly public debt data for the central bank : pd.DataFrame

    """
    return _public_debt_retriever()["public_debt_central_bank"]


def public_assets() -> pd.DataFrame:
    """Get public sector assets data.

    Returns
    -------
    Quarterly public sector assets: pd.DataFrame

    """
    return _public_debt_retriever()["public_assets"]


def net_public_debt_global_public_sector(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """
    Get net public debt excluding deposits at the central bank.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Net public debt excl. deposits at the central bank : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get("public_debt_global_public_sector")
    gross_debt = pipeline.dataset.loc[:, ["Total deuda"]]
    pipeline.get("public_assets")
    assets = pipeline.dataset.loc[:, ["Total activos"]]
    gross_debt.columns = ["Deuda neta del sector" " público global excl. encajes"]
    assets.columns = gross_debt.columns
    pipeline.get("international_reserves")
    deposits = pipeline.dataset.loc[:, ["Obligaciones en ME con el sector financiero"]]
    deposits = (
        transform.resample(deposits, rule="Q-DEC", operation="last")
        .reindex(gross_debt.index)
        .squeeze()
    )
    output = gross_debt.add(assets).add(deposits, axis=0).dropna()
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Sector público",
        currency="USD",
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Stock",
        cumperiods=1,
    )

    return output


def fiscal_balance_summary(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """
    Get the summary fiscal balance table found in the `Budget Law
    <https://www.gub.uy/contaduria-general-nacion/sites/
    contaduria-general-nacion/files/2020-09/
    Mensaje%20y%20Exposici%C3%B3n%20de%20motivos.pdf>`_. Includes adjustments
    for the `Social Security Fund <https://www.impo.com.uy/bases/decretos/
    71-2018/25>`_.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Summary fiscal balance table : pd.DataFrame

    """
    if pipeline is None or pipeline.download is True:
        data = _balance_retriever()
        gps = data["fiscal_balance_global_public_sector"]
        nfps = data["fiscal_balance_nonfinancial_public_sector"]
        gc = data["fiscal_balance_central_government"]
        pe = data["fiscal_balance_soe"]
    else:
        pipeline.get("fiscal_balance_global_public_sector")
        gps = pipeline.dataset
        pipeline.get("fiscal_balance_nonfinancial_public_sector")
        nfps = pipeline.dataset
        pipeline.get("fiscal_balance_central_government")
        gc = pipeline.dataset
        pipeline.get("fiscal_balance_soe")
        pe = pipeline.dataset

    proc = pd.DataFrame(index=gps.index)

    proc["Ingresos: GC-BPS"] = gc["Ingresos: GC-BPS"]
    proc["Ingresos: GC-BPS ex. FSS"] = gc["Ingresos: GC-BPS"] - gc["Ingresos: FSS - Cincuentones"]
    proc["Ingresos: GC"] = gc["Ingresos: GC"]
    proc["Ingresos: DGI"] = gc["Ingresos: DGI"]
    proc["Ingresos: Comercio ext."] = gc["Ingresos: Comercio ext."]
    proc["Ingresos: Otros"] = (
        gc["Ingresos: GC"] - gc["Ingresos: DGI"] - gc["Ingresos: Comercio ext."]
    )
    proc["Ingresos: BPS"] = gc["Ingresos: BPS neto"]
    proc["Ingresos: FSS - Cincuentones"] = gc["Ingresos: FSS - Cincuentones"]
    proc["Ingresos: BPS ex FSS"] = gc["Ingresos: BPS neto"] - gc["Ingresos: FSS - Cincuentones"]
    proc["Egresos: Primarios GC-BPS"] = gc["Egresos: GC-BPS"] - gc["Intereses: Total"]
    proc["Egresos: Primarios corrientes GC-BPS"] = (
        proc["Egresos: Primarios GC-BPS"] - gc["Egresos: Inversión"].squeeze()
    )
    proc["Egresos: Remuneraciones"] = gc["Egresos: Remuneraciones"]
    proc["Egresos: No personales"] = gc["Egresos: No personales"]
    proc["Egresos: Pasividades"] = gc["Egresos: Pasividades"]
    proc["Egresos: Transferencias"] = gc["Egresos: Transferencias"]
    proc["Egresos: Inversión"] = gc["Egresos: Inversión"]
    proc["Resultado: Primario GC-BPS"] = (
        proc["Ingresos: GC-BPS"] - proc["Egresos: Primarios GC-BPS"]
    )
    proc["Resultado: Primario GC-BPS ex FSS"] = (
        proc["Ingresos: GC-BPS ex. FSS"] - proc["Egresos: Primarios GC-BPS"]
    )
    proc["Intereses: GC-BPS"] = gc["Intereses: Total"]
    proc["Intereses: FSS - Cincuentones"] = gc["Intereses: FSS - Cincuentones"]
    proc["Intereses: GC-BPS ex FSS"] = (
        proc["Intereses: GC-BPS"] - proc["Intereses: FSS - Cincuentones"]
    )
    proc["Resultado: Global GC-BPS"] = (
        proc["Resultado: Primario GC-BPS"] - proc["Intereses: GC-BPS"]
    )
    proc["Resultado: Global GC-BPS ex FSS"] = (
        proc["Resultado: Primario GC-BPS ex FSS"] - proc["Intereses: GC-BPS ex FSS"]
    )

    proc["Resultado: Primario corriente EEPP"] = nfps["Ingresos: Res. primario corriente EEPP"]
    proc["Egresos: Inversiones EEPP"] = pe["Egresos: Inversiones"]
    proc["Resultado: Primario EEPP"] = (
        proc["Resultado: Primario corriente EEPP"] - proc["Egresos: Inversiones EEPP"]
    )
    proc["Intereses: EEPP"] = pe["Intereses"]
    proc["Resultado: Global EEPP"] = proc["Resultado: Primario EEPP"] - proc["Intereses: EEPP"]

    proc["Resultado: Primario intendencias"] = nfps["Resultado: Primario intendencias"]
    proc["Intereses: Intendencias"] = nfps["Intereses: Intendencias"]
    proc["Resultado: Global intendencias"] = (
        proc["Resultado: Primario intendencias"] - proc["Intereses: Intendencias"]
    )

    proc["Resultado: Primario BSE"] = nfps["Resultado: Primario BSE"]
    proc["Intereses: BSE"] = nfps["Intereses: BSE"]
    proc["Resultado: Global BSE"] = proc["Resultado: Primario BSE"] - proc["Intereses: BSE"]

    proc["Resultado: Primario resto SPNF"] = (
        proc["Resultado: Primario EEPP"]
        + proc["Resultado: Primario intendencias"]
        + proc["Resultado: Primario BSE"]
    )
    proc["Intereses: Resto SPNF"] = (
        proc["Intereses: EEPP"] + proc["Intereses: Intendencias"] + proc["Intereses: BSE"]
    )
    proc["Resultado: Global resto SPNF"] = (
        proc["Resultado: Global EEPP"]
        + proc["Resultado: Global intendencias"]
        + proc["Resultado: Global BSE"]
    )
    proc["Resultado: Primario SPNF"] = nfps["Resultado: Primario SPNF"]
    proc["Resultado: Primario SPNF ex FSS"] = (
        proc["Resultado: Primario SPNF"] - proc["Ingresos: FSS - Cincuentones"]
    )
    proc["Intereses: SPNF"] = nfps["Intereses: Totales"]
    proc["Intereses: SPNF ex FSS"] = (
        proc["Intereses: SPNF"] - proc["Intereses: FSS - Cincuentones"]
    )
    proc["Resultado: Global SPNF"] = nfps["Resultado: Global SPNF"]
    proc["Resultado: Global SPNF ex FSS"] = (
        proc["Resultado: Primario SPNF ex FSS"] - proc["Intereses: SPNF ex FSS"]
    )

    proc["Resultado: Primario BCU"] = gps["Resultado: Primario BCU"]
    proc["Intereses: BCU"] = gps["Intereses: BCU"]
    proc["Resultado: Global BCU"] = gps["Resultado: Global BCU"]

    proc["Resultado: Primario SPC"] = gps["Resultado: Primario SPC"]
    proc["Resultado: Primario SPC ex FSS"] = (
        proc["Resultado: Primario SPNF ex FSS"] + proc["Resultado: Primario BCU"]
    )
    proc["Intereses: SPC"] = proc["Intereses: SPNF"] + proc["Intereses: BCU"]
    proc["Intereses: SPC ex FSS"] = proc["Intereses: SPNF ex FSS"] + proc["Intereses: BCU"]
    proc["Resultado: Global SPC"] = proc["Resultado: Global SPNF"] + proc["Resultado: Global BCU"]
    proc["Resultado: Global SPC ex FSS"] = (
        proc["Resultado: Global SPNF ex FSS"] + proc["Resultado: Global BCU"]
    )
    output = proc
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Sector público",
        currency="UYU",
        inf_adj="No",
        unit="Millones",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output
