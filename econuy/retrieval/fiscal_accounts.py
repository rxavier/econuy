import re
import datetime as dt
from tempfile import NamedTemporaryFile
from os import PathLike
from typing import Union, Dict

import pandas as pd
import requests
import camelot
from PyPDF2 import pdf as pdf2
from bs4 import BeautifulSoup
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from requests.exceptions import ConnectionError, HTTPError
from sqlalchemy.engine.base import Connection, Engine
from selenium.webdriver.remote.webdriver import WebDriver

from econuy.utils import ops, metadata
from econuy.utils.lstrings import urls, fiscal_sheets, taxes_columns
from econuy.utils.chromedriver import _build


@retry(
    retry_on_exceptions=(HTTPError, ConnectionError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "fiscal",
        index_label: str = "index",
        only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get fiscal data.

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
    name : str, default 'fiscal'
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

    response = requests.get(urls["fiscal"]["dl"]["main"])
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all(href=re.compile("\\.xlsx$"))
    link = links[0]["href"]
    xls = pd.ExcelFile(link)
    output = {}
    for sheet, meta in fiscal_sheets.items():
        data = (pd.read_excel(xls, sheet_name=sheet).
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


def get_taxes(
        update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "taxes",
        index_label: str = "index",
        only_get: bool = False,
        **kwargs) -> pd.DataFrame:
    """Get tax revenues data.

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
    latest = _get_taxes_from_pdf(output, **kwargs)
    output = pd.concat([output, latest], sort=False)

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


def _get_taxes_from_pdf(excel_data: pd.DataFrame,
                        driver: WebDriver = None) -> pd.DataFrame:
    last_month = excel_data.index[-1].month
    month_match = {1: "Enero",
                   2: "Febrero",
                   3: "Marzo",
                   4: "Abril",
                   5: "Mayo",
                   6: "Junio",
                   7: "Julio",
                   8: "Agosto",
                   9: "Setiembre",
                   10: "Octubre",
                   11: "Noviembre",
                   12: "Diciembre"}
    if driver is None:
        driver = _build()
    driver.get(urls["taxes"]["dl"]["report"])
    soup = BeautifulSoup(driver.page_source, "html.parser")
    pdfs = soup.find_all("a", class_=re.compile("TitleStyle"))
    pdfs_iterator = pdfs.copy()
    driver.close()
    for pdf in pdfs_iterator:
        if month_match[last_month + 1] not in pdf.text:
            pdfs.remove(pdf)
        else:
            break
    dates = pd.date_range(start=dt.datetime(dt.datetime.now().year,
                                            last_month + 1, 1), freq="M",
                          periods=len(pdfs))
    data = []
    for pdf, date in zip(pdfs, dates):
        with NamedTemporaryFile(suffix=".pdf") as f:
            r = requests.get(f"https://www.dgi.gub.uy/wdgi/{pdf['href']}")
            f.write(r.content)
            pages = pdf2.PdfFileReader(f.name).getNumPages()
            tables = camelot.read_pdf(f.name, flavor="stream",
                                      pages=str(pages), strip_text=".")
            table = tables[0].df.iloc[2:, 0:2]
            table.columns = ["Impuesto", date]
            table.set_index("Impuesto", inplace=True)
            table = (table.apply(pd.to_numeric, errors="coerce")
                     .dropna(how="any").T)
            table = table.loc[:,
                    ["IVA", "IMESI", "IMEBA", "IRAE", "Categoría I",
                     "Categoría II", "IASS", "IRNR",
                     "Impuesto de Primaria",
                     "6) Total Bruto (suma de (1) a (5))"]]
            table.columns = ['IVA - Valor Agregado',
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
