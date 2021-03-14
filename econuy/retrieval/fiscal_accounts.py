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
from sqlalchemy.engine.base import Connection, Engine

import econuy.retrieval.external_sector
from econuy import transform
from econuy.utils import ops, metadata
from econuy.utils.sources import urls
from econuy.utils.extras import fiscal_sheets, taxes_columns


@retry(
    retry_on_exceptions=(HTTPError, ConnectionError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def _balance_retriever(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
                       revise_rows: Union[str, int] = "nodup",
                       save_loc: Union[str, PathLike, Engine, Connection, None] = None,
                       only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Helper function. See any of the `balance_...()` functions."""
    if only_get is True and update_loc is not None:
        output = {}
        for dataset in fiscal_sheets.keys():
            data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"balance_{dataset}")
            output.update({dataset: data})
        if all(not value.equals(pd.DataFrame()) for value in output.values()):
            return output

    response = requests.get(urls["balance_gps"]["dl"]["main"])
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all(href=re.compile("\\.xlsx$"))
    link = links[0]["href"]
    xls = pd.ExcelFile(link)
    output = {}
    for dataset, meta in fiscal_sheets.items():
        data = (pd.read_excel(xls, sheet_name=meta["sheet"]).
                dropna(axis=0, thresh=4).dropna(axis=1, thresh=4).
                transpose().set_index(2, drop=True))
        data.columns = data.iloc[0]
        data = data[data.index.notnull()].rename_axis(None)
        data.index = data.index + MonthEnd(1)
        data.columns = meta["colnames"]
        data = data.apply(pd.to_numeric, errors="coerce")
        metadata._set(
            data, area="Sector público", currency="UYU",
            inf_adj="No", unit="Millones", seas_adj="NSA",
            ts_type="Flujo", cumperiods=1
        )

        if update_loc is not None:
            previous_data = ops._io(operation="update", data_loc=update_loc,
                                    name=f"balance_{dataset}")
            data = ops._revise(new_data=data,
                               prev_data=previous_data,
                               revise_rows=revise_rows)

        if save_loc is not None:
            ops._io(operation="save", data_loc=save_loc, data=data,
                    name=f"balance_{dataset}")

        output.update({dataset: data})

    return output


def balance_gps(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
                revise_rows: Union[str, int] = "nodup",
                save_loc: Union[str, PathLike, Engine, Connection, None] = None,
                only_get: bool = False) -> pd.DataFrame:
    """Get fiscal balance data for the consolidated public sector.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal balance for the consolidated public sector : pd.DataFrame

    """
    return _balance_retriever(update_loc=update_loc, revise_rows=revise_rows,
                              save_loc=save_loc, only_get=only_get)["gps"]


def balance_nfps(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
                 revise_rows: Union[str, int] = "nodup",
                 save_loc: Union[str, PathLike, Engine, Connection, None] = None,
                 only_get: bool = False) -> pd.DataFrame:
    """Get fiscal balance data for the non-financial public sector.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal balance for the non-financial public sector : pd.DataFrame

    """
    return _balance_retriever(update_loc=update_loc, revise_rows=revise_rows,
                              save_loc=save_loc, only_get=only_get)["nfps"]


def balance_cg_bps(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
                   revise_rows: Union[str, int] = "nodup",
                   save_loc: Union[str, PathLike, Engine, Connection, None] = None,
                   only_get: bool = False) -> pd.DataFrame:
    """Get fiscal balance data for the central government + BPS.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal balance for the central government + BPS : pd.DataFrame

    """
    return _balance_retriever(update_loc=update_loc, revise_rows=revise_rows,
                              save_loc=save_loc, only_get=only_get)["cg-bps"]


def balance_pe(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
               revise_rows: Union[str, int] = "nodup",
               save_loc: Union[str, PathLike, Engine, Connection, None] = None,
               only_get: bool = False) -> pd.DataFrame:
    """Get fiscal balance data for public enterprises.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal balance for public enterprises : pd.DataFrame

    """
    return _balance_retriever(update_loc=update_loc, revise_rows=revise_rows,
                              save_loc=save_loc, only_get=only_get)["pe"]


def balance_ancap(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
                  revise_rows: Union[str, int] = "nodup",
                  save_loc: Union[str, PathLike, Engine, Connection, None] = None,
                  only_get: bool = False) -> pd.DataFrame:
    """Get fiscal balance data for ANCAP.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal balance for ANCAP : pd.DataFrame

    """
    return _balance_retriever(update_loc=update_loc, revise_rows=revise_rows,
                              save_loc=save_loc, only_get=only_get)["ancap"]


def balance_ute(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
                revise_rows: Union[str, int] = "nodup",
                save_loc: Union[str, PathLike, Engine, Connection, None] = None,
                only_get: bool = False) -> pd.DataFrame:
    """Get fiscal balance data for UTE.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal balance for UTE : pd.DataFrame

    """
    return _balance_retriever(update_loc=update_loc, revise_rows=revise_rows,
                              save_loc=save_loc, only_get=only_get)["ute"]


def balance_antel(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
                  revise_rows: Union[str, int] = "nodup",
                  save_loc: Union[str, PathLike, Engine, Connection, None] = None,
                  only_get: bool = False) -> pd.DataFrame:
    """Get fiscal balance data for ANTEL.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal balance for ANTEL : pd.DataFrame

    """
    return _balance_retriever(update_loc=update_loc, revise_rows=revise_rows,
                              save_loc=save_loc, only_get=only_get)["antel"]


def balance_ose(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
                revise_rows: Union[str, int] = "nodup",
                save_loc: Union[str, PathLike, Engine, Connection, None] = None,
                only_get: bool = False) -> pd.DataFrame:
    """Get fiscal balance data for OSE.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly fiscal balance for OSE : pd.DataFrame

    """
    return _balance_retriever(update_loc=update_loc, revise_rows=revise_rows,
                              save_loc=save_loc, only_get=only_get)["ose"]


@retry(
    retry_on_exceptions=(HTTPError, ConnectionError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def tax_revenue(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Monthly tax revenues : pd.DataFrame

    """
    name = "taxes"

    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name)
        if not output.equals(pd.DataFrame()):
            return output

    raw = pd.read_excel(urls[name]["dl"]["main"],
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
                                name=name)
        output = ops._revise(new_data=output, prev_data=previous_data,
                             revise_rows=revise_rows)

    output = output.apply(pd.to_numeric, errors="coerce")
    metadata._set(output, area="Sector público", currency="UYU",
                  inf_adj="No", unit="Millones", seas_adj="NSA",
                  ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name)

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
def _public_debt_retriever(update_loc: Union[str, PathLike,
                                             Engine, Connection, None] = None,
                           revise_rows: Union[str, int] = "nodup",
                           save_loc: Union[str, PathLike,
                                           Engine, Connection, None] = None,
                           only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Helper function. See any of the `public_debt_...()` functions."""
    if only_get is True and update_loc is not None:
        output = {}
        for meta in ["gps", "nfps", "cb", "assets"]:
            data = ops._io(operation="update", data_loc=update_loc,
                           name=f"public_debt_{meta}")
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

    xls = pd.ExcelFile(urls["public_debt_gps"]["dl"]["main"])
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
            previous_data = ops._io(operation="update", data_loc=update_loc,
                                    name=f"public_debt_{meta}")
            data = ops._revise(new_data=data,
                               prev_data=previous_data,
                               revise_rows=revise_rows)
        metadata._set(data, area="Sector público", currency="USD",
                      inf_adj="No", unit="Millones", seas_adj="NSA",
                      ts_type="Stock", cumperiods=1)

        if save_loc is not None:
            ops._io(operation="save", data_loc=save_loc,
                    data=data, name=f"public_debt_{meta}")

        output.update({meta: data})

    return output


def public_debt_gps(update_loc: Union[str, PathLike,
                                      Engine, Connection, None] = None,
                    revise_rows: Union[str, int] = "nodup",
                    save_loc: Union[str, PathLike,
                                    Engine, Connection, None] = None,
                    only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get public debt data for the consolidated public sector.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Quarterly public debt data for the consolidated public sector: pd.DataFrame

    """
    return _public_debt_retriever(update_loc=update_loc, revise_rows=revise_rows,
                                  save_loc=save_loc, only_get=only_get)["gps"]


def public_debt_nfps(update_loc: Union[str, PathLike,
                                       Engine, Connection, None] = None,
                     revise_rows: Union[str, int] = "nodup",
                     save_loc: Union[str, PathLike,
                                     Engine, Connection, None] = None,
                     only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get public debt data for the non-financial public sector.

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Quarterly public debt data for the non-financial public sector: pd.DataFrame

    """
    return _public_debt_retriever(update_loc=update_loc, revise_rows=revise_rows,
                                  save_loc=save_loc, only_get=only_get)["nfps"]


def public_debt_cb(update_loc: Union[str, PathLike,
                                     Engine, Connection, None] = None,
                   revise_rows: Union[str, int] = "nodup",
                   save_loc: Union[str, PathLike,
                                   Engine, Connection, None] = None,
                   only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get public debt data for the central bank

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
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Quarterly public debt data for the central bank : pd.DataFrame

    """
    return _public_debt_retriever(update_loc=update_loc, revise_rows=revise_rows,
                                  save_loc=save_loc, only_get=only_get)["cb"]


def net_public_debt(update_loc: Union[str, PathLike, Engine,
                                      Connection, None] = None,
                    save_loc: Union[str, PathLike, Engine,
                                    Connection, None] = None,
                    only_get: bool = True) -> pd.DataFrame:
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
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Net public debt excl. deposits at the central bank : pd.DataFrame

    """
    name = "net_public_debt"

    data = _public_debt_retriever(update_loc=update_loc,
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
    deposits = (transform.resample(deposits, rule="Q-DEC", operation="last")
                .reindex(gross_debt.index).squeeze())
    output = gross_debt.add(assets).add(deposits, axis=0).dropna()

    metadata._set(output, area="Sector público",
                  currency="USD", inf_adj="No", unit="Millones",
                  seas_adj="NSA", ts_type="Stock", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name)

    return output


def balance_summary(update_loc: Union[str, PathLike, Engine,
                                      Connection, None] = None,
                    save_loc: Union[str, PathLike, Engine,
                                    Connection, None] = None,
                    only_get: bool = True) -> pd.DataFrame:
    """
    Get the summary fiscal balance table found in the `Budget Law
    <https://www.gub.uy/contaduria-general-nacion/sites/
    contaduria-general-nacion/files/2020-09/
    Mensaje%20y%20Exposici%C3%B3n%20de%20motivos.pdf>`_. Includes adjustments
    for the `Social Security Fund <https://www.impo.com.uy/bases/decretos/
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
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Summary fiscal balance table : pd.DataFrame

    """
    name = "balance_summary"

    data = _balance_retriever(update_loc=update_loc,
                              save_loc=save_loc, only_get=only_get)
    gps = data["gps"]
    nfps = data["nfps"]
    gc = data["cg-bps"]
    pe = data["pe"]

    proc = pd.DataFrame(index=gps.index)

    proc["Ingresos: GC-BPS"] = gc["Ingresos: GC-BPS"]
    proc["Ingresos: GC-BPS ex. FSS"] = (gc["Ingresos: GC-BPS"]
                                        - gc["Ingresos: FSS - Cincuentones"])
    proc["Ingresos: GC"] = gc["Ingresos: GC"]
    proc["Ingresos: DGI"] = gc["Ingresos: DGI"]
    proc["Ingresos: Comercio ext."] = gc["Ingresos: Comercio ext."]
    proc["Ingresos: Otros"] = (gc["Ingresos: GC"]
                               - gc["Ingresos: DGI"]
                               - gc["Ingresos: Comercio ext."])
    proc["Ingresos: BPS"] = gc["Ingresos: BPS neto"]
    proc["Ingresos: FSS - Cincuentones"] = gc["Ingresos: FSS - Cincuentones"]
    proc["Ingresos: BPS ex FSS"] = (gc["Ingresos: BPS neto"]
                                    - gc["Ingresos: FSS - Cincuentones"])
    proc["Egresos: Primarios GC-BPS"] = (gc["Egresos: GC-BPS"]
                                         - gc["Intereses: Total"])
    proc["Egresos: Primarios corrientes GC-BPS"] = (proc["Egresos: Primarios GC-BPS"]
                                                    - gc["Egresos: Inversión"].squeeze())
    proc["Egresos: Remuneraciones"] = gc["Egresos: Remuneraciones"]
    proc["Egresos: No personales"] = gc["Egresos: No personales"]
    proc["Egresos: Pasividades"] = gc["Egresos: Pasividades"]
    proc["Egresos: Transferencias"] = gc["Egresos: Transferencias"]
    proc["Egresos: Inversión"] = gc["Egresos: Inversión"]
    proc["Resultado: Primario GC-BPS"] = (proc["Ingresos: GC-BPS"]
                                          - proc["Egresos: Primarios GC-BPS"])
    proc["Resultado: Primario GC-BPS ex FSS"] = (proc["Ingresos: GC-BPS ex. FSS"]
                                                 - proc["Egresos: Primarios GC-BPS"])
    proc["Intereses: GC-BPS"] = gc["Intereses: Total"]
    proc["Intereses: FSS - Cincuentones"] = gc["Intereses: FSS - Cincuentones"]
    proc["Intereses: GC-BPS ex FSS"] = (proc["Intereses: GC-BPS"]
                                        - proc["Intereses: FSS - Cincuentones"])
    proc["Resultado: Global GC-BPS"] = (proc["Resultado: Primario GC-BPS"]
                                        - proc["Intereses: GC-BPS"])
    proc["Resultado: Global GC-BPS ex FSS"] = (proc["Resultado: Primario GC-BPS ex FSS"]
                                               - proc["Intereses: GC-BPS ex FSS"])

    proc["Resultado: Primario corriente EEPP"] = nfps["Ingresos: Res. primario corriente EEPP"]
    proc["Egresos: Inversiones EEPP"] = pe["Egresos: Inversiones"]
    proc["Resultado: Primario EEPP"] = (proc["Resultado: Primario corriente EEPP"]
                                        - proc["Egresos: Inversiones EEPP"])
    proc["Intereses: EEPP"] = pe["Intereses"]
    proc["Resultado: Global EEPP"] = (proc["Resultado: Primario EEPP"]
                                      - proc["Intereses: EEPP"])

    proc["Resultado: Primario intendencias"] = nfps["Resultado: Primario intendencias"]
    proc["Intereses: Intendencias"] = nfps["Intereses: Intendencias"]
    proc["Resultado: Global intendencias"] = (proc["Resultado: Primario intendencias"]
                                              - proc["Intereses: Intendencias"])

    proc["Resultado: Primario BSE"] = nfps["Resultado: Primario BSE"]
    proc["Intereses: BSE"] = nfps["Intereses: BSE"]
    proc["Resultado: Global BSE"] = (proc["Resultado: Primario BSE"]
                                     - proc["Intereses: BSE"])

    proc["Resultado: Primario resto SPNF"] = (proc["Resultado: Primario EEPP"]
                                              + proc["Resultado: Primario intendencias"]
                                              + proc["Resultado: Primario BSE"])
    proc["Intereses: Resto SPNF"] = (proc["Intereses: EEPP"]
                                     + proc["Intereses: Intendencias"]
                                     + proc["Intereses: BSE"])
    proc["Resultado: Global resto SPNF"] = (proc["Resultado: Global EEPP"]
                                            + proc["Resultado: Global intendencias"]
                                            + proc["Resultado: Global BSE"])
    proc["Resultado: Primario SPNF"] = nfps["Resultado: Primario SPNF"]
    proc["Resultado: Primario SPNF ex FSS"] = (proc["Resultado: Primario SPNF"]
                                               - proc["Ingresos: FSS - Cincuentones"])
    proc["Intereses: SPNF"] = nfps["Intereses: Totales"]
    proc["Intereses: SPNF ex FSS"] = (proc["Intereses: SPNF"]
                                      - proc["Intereses: FSS - Cincuentones"])
    proc["Resultado: Global SPNF"] = nfps["Resultado: Global SPNF"]
    proc["Resultado: Global SPNF ex FSS"] = (proc["Resultado: Primario SPNF ex FSS"]
                                             - proc["Intereses: SPNF ex FSS"])

    proc["Resultado: Primario BCU"] = gps["Resultado: Primario BCU"]
    proc["Intereses: BCU"] = gps["Intereses: BCU"]
    proc["Resultado: Global BCU"] = gps["Resultado: Global BCU"]

    proc["Resultado: Primario SPC"] = gps["Resultado: Primario SPC"]
    proc["Resultado: Primario SPC ex FSS"] = (proc["Resultado: Primario SPNF ex FSS"]
                                              + proc["Resultado: Primario BCU"])
    proc["Intereses: SPC"] = proc["Intereses: SPNF"] + proc["Intereses: BCU"]
    proc["Intereses: SPC ex FSS"] = (proc["Intereses: SPNF ex FSS"]
                                     + proc["Intereses: BCU"])
    proc["Resultado: Global SPC"] = (proc["Resultado: Global SPNF"]
                                     + proc["Resultado: Global BCU"])
    proc["Resultado: Global SPC ex FSS"] = (proc["Resultado: Global SPNF ex FSS"]
                                            + proc["Resultado: Global BCU"])
    output = proc

    metadata._set(output, area="Sector público",
                  currency="UYU", inf_adj="No", unit="Millones",
                  seas_adj="NSA", ts_type="Flujo", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name)

    return output
