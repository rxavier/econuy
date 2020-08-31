import os
import datetime as dt
from os import PathLike
from typing import Union
from urllib.error import URLError, HTTPError

import pandas as pd
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from opnieuw import retry
from sqlalchemy.engine.base import Connection, Engine
from dotenv import load_dotenv

from econuy.utils import ops, metadata
from econuy.utils.lstrings import urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "call",
        index_label: str = "index",
        only_get: bool = False) -> pd.DataFrame:
    """Get 1-day call interest rate data.

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
    name : str, default 'cpi'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'call'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Daily call rate : pd.DataFrame

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    driver = _build_chrome_driver()
    driver.get(urls["call"]["dl"]["main"])
    start = driver.find_element(by="name",
                                value="ctl00$ContentPlaceHolder1$dateDesde$dateInput")
    start.clear()
    start.send_keys("01/01/2002")
    end = driver.find_element(by="name",
                              value="ctl00$ContentPlaceHolder1$dateHasta$dateInput")
    end.clear()
    end.send_keys(dt.datetime.now().strftime("%d/%m/%Y"))
    submit = driver.find_element(by="id",
                                 value="ContentPlaceHolder1_LinkFiltrar")
    submit.click()
    tables = pd.read_html(driver.page_source, decimal=",", thousands=".")
    raw = tables[8].iloc[:, :-2]
    call = raw.set_index("FECHA")
    call.index = pd.to_datetime(call.index, format="%d/%m/%Y")
    call.sort_index(inplace=True)
    call.columns = ["Tasa call a 1 día: Promedio",
                    "Tasa call a 1 día: Máximo",
                    "Tasa call a 1 día: Mínimo"]

    if update_loc is not None:
        previous_data = ops._io(
            operation="update", data_loc=update_loc,
            name=name, index_label=index_label
        )
        call = ops._revise(new_data=call, prev_data=previous_data,
                           revise_rows=revise_rows)

    call = call.apply(pd.to_numeric, errors="coerce")
    metadata._set(call, area="Mercado financiero", currency="UYU",
                  inf_adj="No", unit="Tasa", seas_adj="NSA",
                  ts_type="-", cumperiods=1)

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=call, name=name, index_label=index_label)

    return call


def _build_chrome_driver():
    """Utility function for building a Selenium Chrome driver"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    try:
        load_dotenv("econuy/.env")
        service = Service(executable_path=os.environ.get("CHROMEDRIVER_PATH"))
        chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_PATH")
        return webdriver.Chrome(options=chrome_options, service=service)
    except (WebDriverException, TypeError):
        chromedriver_autoinstaller.install()
        return webdriver.Chrome(options=chrome_options)
