import sys
import datetime as dt
import re
import warnings
from os import PathLike
from tempfile import NamedTemporaryFile
from typing import Union, Dict
from urllib.error import HTTPError, URLError

import camelot
import pandas as pd
import requests
from PyPDF2 import pdf as pdf2
from PyPDF2.utils import PdfReadWarning
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from requests.exceptions import ConnectionError
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.remote.webdriver import WebDriver
from sqlalchemy.engine.base import Connection, Engine

import econuy.retrieval.external_sector
from econuy import transform
from econuy.utils import ops, metadata
from econuy.utils.chromedriver import _build
from econuy.utils.lstrings import urls, fiscal_sheets, taxes_columns, \
    fiscal_metadata


@retry(
    retry_on_exceptions=(HTTPError, ConnectionError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def balance(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
            revise_rows: Union[str, int] = "nodup",
            save_loc: Union[str, PathLike, Engine, Connection, None] = None,
            name: str = "balance",
            index_label: str = "index",
            only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get fiscal balance data.

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
    name : str, default 'balance'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal accounts different aggregations : Dict[str, pd.DataFrame]
        Available aggregations: non-financial public sector, consolidated
        public sector, central government, aggregated public enterprises
        and individual public enterprises.

    """
    if only_get is True and update_loc is not None:
        output = {}
        for meta in fiscal_sheets.values():
            data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{meta['Name']}", index_label=index_label
            )
            output.update({meta["Name"]: data})
        if all(not value.equals(pd.DataFrame()) for value in output.values()):
            return output

    response = requests.get(urls["balance"]["dl"]["main"])
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all(href=re.compile("\\.xlsx$"))
    link = links[0]["href"]
    xls = pd.ExcelFile(link, engine="openpyxl")
    output = {}
    for sheet, meta in fiscal_sheets.items():
        data = (pd.read_excel(xls, sheet_name=sheet, engine="openpyxl").
                dropna(axis=0, thresh=4).dropna(axis=1, thresh=4).
                transpose().set_index(2, drop=True))
        data.columns = data.iloc[0]
        data = data[data.index.notnull()].rename_axis(None)
        data.index = data.index + MonthEnd(1)
        data.columns = meta["Colnames"]

        if update_loc is not None:
            previous_data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{meta['Name']}", index_label=index_label
            )
            data = ops._revise(new_data=data,
                               prev_data=previous_data,
                               revise_rows=revise_rows)
        data = data.apply(pd.to_numeric, errors="coerce")
        metadata._set(
            data, area="Sector público", currency="UYU",
            inf_adj="No", unit="Millones", seas_adj="NSA",
            ts_type="Flujo", cumperiods=1
        )

        if save_loc is not None:
            ops._io(
                operation="save", data_loc=save_loc, data=data,
                name=f"{name}_{meta['Name']}", index_label=index_label
            )

        output.update({meta["Name"]: data})

    return output


@retry(
    retry_on_exceptions=(HTTPError, ConnectionError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def tax_revenue(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "taxes",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """
    Get tax revenues data.

    This retrieval function requires that Ghostscript and Tkinter be found in
    your system.

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
    name : str, default 'taxes'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly tax revenues : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    raw = pd.read_excel(urls["taxes"]["dl"]["main"],
                        usecols="C:AO", index_col=0)
    raw.index = pd.to_datetime(raw.index, errors="coerce")
    output = raw.loc[~pd.isna(raw.index)]
    output.index = output.index + MonthEnd(0)
    output.columns = taxes_columns
    output = output.div(1000000)
    latest = _get_taxes_from_pdf(output)
    output = pd.concat([output, latest], sort=False)
    output = output.loc[~output.index.duplicated(keep="first")]

    if update_loc is not None:
        previous_data = ops._io(operation="update",
                                data_loc=update_loc,
                                name=name,
                                index_label=index_label)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    output = output.apply(pd.to_numeric, errors="coerce")
    metadata._set(output, area="Sector público", currency="UYU",
                  inf_adj="No", unit="Millones", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


def _get_taxes_from_pdf(excel_data: pd.DataFrame) -> pd.DataFrame:
    extra_url = ",O,es,0,"
    last_month = excel_data.index[-1].month
    last_year = excel_data.index[-1].year
    if last_month == 12:
        reports_year = [last_year + 1]
    else:
        reports_year = [last_year, last_year + 1]
    data = []
    for year in reports_year:
        url = f"{urls['taxes']['dl']['report']}{year}{extra_url}"
        r = requests.get(url)
        pdf_urls = re.findall("afiledownload\?2,4,1851,O,S,0,[0-9]+"
                              "%[0-9A-z]{3}%[0-9A-z]{3}%3B108,", r.text)
        pdf_urls = list(dict.fromkeys(pdf_urls))
        if len(pdf_urls) == 0:
            continue
        dates = pd.date_range(start=dt.datetime(year, 1, 1), freq="M",
                              periods=len(pdf_urls))
        if sys.platform == "win32":
            delete = False
        else:
            delete = True
        for pdf, date in zip(pdf_urls, dates):
            with NamedTemporaryFile(suffix=".pdf", delete=delete) as f:
                r = requests.get(f"https://www.dgi.gub.uy/wdgi/{pdf}")
                f.write(r.content)
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=PdfReadWarning)
                    pages = pdf2.PdfFileReader(f.name).getNumPages()
                tables = camelot.read_pdf(f.name, flavor="stream",
                                          pages=str(pages), strip_text=".")
                table = tables[0].df.iloc[2:, 0:2]
                table.columns = ["Impuesto", date]
                table.set_index("Impuesto", inplace=True)
                table = (table.apply(pd.to_numeric, errors="coerce")
                         .dropna(how="any").T)
                table = table.loc[:,
                                  ["IVA",
                                   "IMESI",
                                   "IMEBA",
                                   "IRAE",
                                   "Categoría I",
                                   "Categoría II",
                                   "IASS",
                                   "IRNR",
                                   "Impuesto de Primaria",
                                   "6) Total Bruto (suma de (1) a (5))"]]
                table.columns = [
                    'IVA - Valor Agregado',
                    'IMESI - Específico Interno',
                    'IMEBA - Enajenación de Bienes Agropecuarios',
                    'IRAE - Rentas de Actividades Económicas',
                    'IRPF Cat I - Renta de las Personas Físicas',
                    'IRPF Cat II - Rentas de las Personas Físicas',
                    'IASS - Asistencia a la Seguridad Social',
                    'IRNR - Rentas de No Residentes',
                    'Impuesto de Educación Primaria',
                    'Recaudación Total de la DGI']
                data.append(table)
    output = pd.concat(data)

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=30,
)
def public_debt(update_loc: Union[str, PathLike,
                                  Engine, Connection, None] = None,
                revise_rows: Union[str, int] = "nodup",
                save_loc: Union[str, PathLike,
                                Engine, Connection, None] = None,
                name: str = "public_debt",
                index_label: str = "index",
                only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get public debt data.

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
    name : str, default 'public_debt'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Quarterly public debt data : pd.DataFrame
        Global public sector, non-monetary public sector and BCU debts.

    """
    if only_get is True and update_loc is not None:
        output = {}
        for meta in ["gps", "nfps", "cb", "assets"]:
            data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{meta}", index_label=index_label
            )
            output.update({meta: data})
        if all(not value.equals(pd.DataFrame()) for value in output.values()):
            return output

    colnames = ["Total deuda", "Plazo contractual: hasta 1 año",
                "Plazo contractual: entre 1 y 5 años",
                "Plazo contractual: más de 5 años",
                "Plazo residual: hasta 1 año",
                "Plazo residual: entre 1 y 5 años",
                "Plazo residual: más de 5 años",
                "Moneda: pesos", "Moneda: dólares", "Moneda: euros",
                "Moneda: yenes", "Moneda: DEG", "Moneda: otras",
                "Residencia: no residentes", "Residencia: residentes"]

    xls = pd.ExcelFile(urls["public_debt"]["dl"]["main"])
    gps_raw = pd.read_excel(xls, sheet_name="SPG2",
                            usecols="B:Q", index_col=0, skiprows=10,
                            nrows=(dt.datetime.now().year - 1999) * 4)
    gps = gps_raw.dropna(how="any", thresh=2)
    gps.index = pd.date_range(start="1999-12-31", periods=len(gps),
                              freq="Q-DEC")
    gps.columns = colnames

    nfps_raw = pd.read_excel(xls, sheet_name="SPNM bruta",
                             usecols="B:O", index_col=0)
    loc = nfps_raw.index.get_loc("9. Deuda Bruta del Sector Público no "
                                 "monetario por plazo y  moneda.")
    nfps = nfps_raw.iloc[loc + 5:, :].dropna(how="any")
    nfps.index = pd.date_range(start="1999-12-31", periods=len(nfps),
                               freq="Q-DEC")
    nfps_extra_raw = pd.read_excel(xls, sheet_name="SPNM bruta",
                                   usecols="O:P", skiprows=11,
                                   nrows=(dt.datetime.now().year - 1999) * 4)
    nfps_extra = nfps_extra_raw.dropna(how="all")
    nfps_extra.index = nfps.index
    nfps = pd.concat([nfps, nfps_extra], axis=1)
    nfps.columns = colnames

    cb_raw = pd.read_excel(xls, sheet_name="BCU bruta",
                           usecols="B:O", index_col=0,
                           skiprows=(dt.datetime.now().year - 1999) * 8 + 20)
    cb = cb_raw.dropna(how="any")
    cb.index = pd.date_range(start="1999-12-31", periods=len(cb),
                             freq="Q-DEC")
    cb_extra_raw = pd.read_excel(xls, sheet_name="BCU bruta",
                                 usecols="O:P", skiprows=11,
                                 nrows=(dt.datetime.now().year - 1999) * 4)
    bcu_extra = cb_extra_raw.dropna(how="all")
    bcu_extra.index = cb.index
    cb = pd.concat([cb, bcu_extra], axis=1)
    cb.columns = colnames

    assets_raw = pd.read_excel(xls, sheet_name="Activos Neta",
                               usecols="B,C,D,K", index_col=0, skiprows=13,
                               nrows=(dt.datetime.now().year - 1999) * 4)
    assets = assets_raw.dropna(how="any")
    assets.index = pd.date_range(start="1999-12-31", periods=len(assets),
                                 freq="Q-DEC")
    assets.columns = ["Total activos", "Sector público no monetario",
                      "BCU"]

    output = {"gps": gps, "nfps": nfps, "cb": cb, "assets": assets}

    for meta, data in output.items():
        if update_loc is not None:
            previous_data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{meta}", index_label=index_label
            )
            data = ops._revise(new_data=data,
                               prev_data=previous_data,
                               revise_rows=revise_rows)
        metadata._set(data, area="Sector público", currency="USD",
                      inf_adj="No", unit="Millones", seas_adj="NSA",
                      ts_type="Stock", cumperiods=1)

        if save_loc is not None:
            ops._io(operation="save", data_loc=save_loc,
                    data=data, name=f"{name}_{meta}", index_label=index_label)

        output.update({meta: data})

    return output


def net_public_debt(update_loc: Union[str, PathLike, Engine,
                                      Connection, None] = None,
                    save_loc: Union[str, PathLike, Engine,
                                    Connection, None] = None,
                    only_get: bool = True,
                    name: str = "net_public_debt",
                    index_label: str = "index") -> pd.DataFrame:
    """
    Get net public debt excluding deposits at the central bank.

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
    name : str, default 'net_public_debt'
        Either CSV filename for updating and/or saving, or table name if
        using SQL. Options will be appended to the base name.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Net public debt excl. deposits at the central bank : pd.DataFrame

    """
    data = public_debt(update_loc=update_loc,
                       save_loc=save_loc, only_get=only_get)
    gross_debt = data["gps"].loc[:, ["Total deuda"]]
    assets = data["assets"].loc[:, ["Total activos"]]
    gross_debt.columns = ["Deuda neta del sector"
                          " público global excl. encajes"]
    assets.columns = gross_debt.columns
    deposits = econuy.retrieval.external_sector.reserves(
        update_loc=update_loc, save_loc=save_loc,
        only_get=only_get).loc[:,
                               ["Obligaciones en ME con el sector financiero"]]
    deposits = (transform.resample(deposits, target="Q-DEC", operation="end")
                .reindex(gross_debt.index).squeeze())
    output = gross_debt.add(assets).add(deposits, axis=0).dropna()

    metadata._set(output, area="Sector público",
                  currency="USD", inf_adj="No", unit="Millones",
                  seas_adj="NSA", ts_type="Stock", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output


def balance_fss(update_loc: Union[str, PathLike, Engine,
                                  Connection, None] = None,
                save_loc: Union[str, PathLike, Engine,
                                Connection, None] = None,
                only_get: bool = True,
                name: str = "balance_fss",
                index_label: str = "index") -> Dict[str, pd.DataFrame]:
    """
    Get fiscal balance data for the consolidated publci sector, non-financial
    public sector and central government, both adjusted and non-adjusted for
    the `Social Security Fund <https://www.impo.com.uy/bases/decretos/
    71-2018/25>`_.

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
    name : str, default 'balance_fss'
        Either CSV filename for updating and/or saving, or table name if
        using SQL. Options will be appended to the base name.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Fiscal balances : Dict[str, pd.DataFrame]

    """
    data = balance(update_loc=update_loc,
                   save_loc=save_loc, only_get=only_get)
    gps = data["gps"]
    nfps = data["nfps"]
    gc = data["gc-bps"]

    proc = pd.DataFrame(index=gps.index)
    proc["Ingresos: SPNF-SPC"] = nfps["Ingresos: SPNF"]
    proc["Ingresos: GC-BPS"] = gc["Ingresos: GC-BPS"]
    proc["Egresos: Primarios SPNF-SPC"] = nfps["Egresos: Primarios SPNF"]
    proc["Egresos: Totales GC-BPS"] = gc["Egresos: GC-BPS"]
    proc["Egresos: Inversiones SPNF-SPC"] = nfps["Egresos: Inversiones"]
    proc["Egresos: Inversiones GC-BPS"] = gc["Egresos: Inversión"]
    proc["Intereses: SPNF"] = nfps["Intereses: Totales"]
    proc["Intereses: BCU"] = gps["Intereses: BCU"]
    proc["Intereses: SPC"] = proc["Intereses: SPNF"] + proc["Intereses: BCU"]
    proc["Intereses: GC-BPS"] = gc["Intereses: Total"]
    proc["Egresos: Totales SPNF"] = (proc["Egresos: Primarios SPNF-SPC"]
                                     + proc["Intereses: SPNF"])
    proc["Egresos: Totales SPC"] = (proc["Egresos: Totales SPNF"]
                                    + proc["Intereses: BCU"])
    proc["Egresos: Primarios GC-BPS"] = (proc["Egresos: Totales GC-BPS"]
                                         - proc["Intereses: GC-BPS"])
    proc["Resultado: Primario intendencias"] = nfps[
        "Resultado: Primario intendencias"
    ]
    proc["Resultado: Primario BSE"] = nfps["Resultado: Primario BSE"]
    proc["Resultado: Primario BCU"] = gps["Resultado: Primario BCU"]
    proc["Resultado: Primario SPNF"] = nfps["Resultado: Primario SPNF"]
    proc["Resultado: Global SPNF"] = nfps["Resultado: Global SPNF"]
    proc["Resultado: Primario SPC"] = gps["Resultado: Primario SPC"]
    proc["Resultado: Global SPC"] = gps["Resultado: Global SPC"]
    proc["Resultado: Primario GC-BPS"] = (proc["Ingresos: GC-BPS"]
                                          - proc["Egresos: Primarios GC-BPS"])
    proc["Resultado: Global GC-BPS"] = gc["Resultado: Global GC-BPS"]

    proc["Ingresos: FSS"] = gc["Ingresos: FSS"]
    proc["Intereses: FSS"] = gc["Intereses: BPS-FSS"]
    proc["Ingresos: SPNF-SPC aj. FSS"] = (proc["Ingresos: SPNF-SPC"]
                                          - proc["Ingresos: FSS"])
    proc["Ingresos: GC-BPS aj. FSS"] = (proc["Ingresos: GC-BPS"]
                                        - proc["Ingresos: FSS"])
    proc["Intereses: SPNF aj. FSS"] = (proc["Intereses: SPNF"]
                                       - proc["Intereses: FSS"])
    proc["Intereses: SPC aj. FSS"] = (proc["Intereses: SPC"]
                                      - proc["Intereses: FSS"])
    proc["Intereses: GC-BPS aj. FSS"] = (proc["Intereses: GC-BPS"]
                                         - proc["Intereses: FSS"])
    proc["Egresos: Totales SPNF aj. FSS"] = (proc["Egresos: Totales SPNF"]
                                             - proc["Intereses: FSS"])
    proc["Egresos: Totales SPC aj. FSS"] = (proc["Egresos: Totales SPC"]
                                            - proc["Intereses: FSS"])
    proc["Egresos: Totales GC-BPS aj. FSS"] = (proc["Egresos: Totales GC-BPS"]
                                               - proc["Intereses: FSS"])
    proc["Resultado: Primario SPNF aj. FSS"] = (
        proc["Resultado: Primario SPNF"]
        - proc["Ingresos: FSS"])
    proc["Resultado: Global SPNF aj. FSS"] = (proc["Resultado: Global SPNF"]
                                              - proc["Ingresos: FSS"]
                                              + proc["Intereses: FSS"])
    proc["Resultado: Primario SPC aj. FSS"] = (proc["Resultado: Primario SPC"]
                                               - proc["Ingresos: FSS"])
    proc["Resultado: Global SPC aj. FSS"] = (proc["Resultado: Global SPC"]
                                             - proc["Ingresos: FSS"]
                                             + proc["Intereses: FSS"])
    proc["Resultado: Primario GC-BPS aj. FSS"] = (
        proc["Resultado: Primario GC-BPS"]
        - proc["Ingresos: FSS"])
    proc["Resultado: Global GC-BPS aj. FSS"] = (
        proc["Resultado: Global GC-BPS"]
        - proc["Ingresos: FSS"]
        + proc["Intereses: FSS"])

    output = {}
    for agg, fss in zip(["gps", "nfps", "gc", "gps", "nfps", "gc"],
                        [True, True, True, False, False, False]):
        name_aux = f"{name}_{agg}"
        if fss:
            name_aux += "_fssadj"
        aux = proc.loc[:, fiscal_metadata[agg][fss]]
        metadata._set(aux, area="Sector público",
                      currency="UYU", inf_adj="No", unit="Millones",
                      seas_adj="NSA", ts_type="Flujo", cumperiods=1)
        output.update({name_aux: aux})

        if save_loc is not None:
            ops._io(operation="save", data_loc=save_loc,
                    data=aux, name=name_aux, index_label=index_label)

    return output
