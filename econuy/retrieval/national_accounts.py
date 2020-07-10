import datetime as dt
from os import PathLike
from typing import Union, Dict
from urllib.error import URLError, HTTPError

import pandas as pd
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd
from sqlalchemy.engine.base import Connection, Engine

from econuy import transform
from econuy.utils import ops, metadata
from econuy.utils.lstrings import na_metadata


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def get(update_loc: Union[str, PathLike, Engine, Connection, None] = None,
        revise_rows: Union[str, int] = "nodup",
        save_loc: Union[str, PathLike, Engine, Connection, None] = None,
        name: str = "naccounts",
        index_label: str = "index",
        only_get: bool = False) -> Dict[str, pd.DataFrame]:
    """Get national accounts data.

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
    name : str, default 'naccounts'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default False
        If True, don't download data, retrieve what is available from
        ``update_loc``.

    Returns
    -------
    Quarterly national accounts : Dict[str, pd.DataFrame]
        Each dataframe corresponds to a national accounts table.

    """
    if only_get is True and update_loc is not None:
        output = {}
        for filename, meta in na_metadata.items():
            data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{filename}", index_label=index_label
            )
            output.update({filename: data})
        if all(not value.equals(pd.DataFrame()) for value in output.values()):
            return output

    parsed_excels = {}
    for filename, meta in na_metadata.items():
        raw = pd.read_excel(meta["url"], skiprows=9, nrows=meta["Rows"])
        proc = (raw.drop(columns=["Unnamed: 0"]).
                dropna(axis=0, how="all").dropna(axis=1, how="all"))
        proc = proc.transpose()
        proc.columns = meta["Colnames"]
        proc.drop(["Unnamed: 1"], inplace=True)
        _fix_dates(proc)
        if meta["Unit"] == "Miles":
            proc = proc.divide(1000)
            unit_ = "Millones"
        else:
            unit_ = meta["Unit"]

        if update_loc is not None:
            previous_data = ops._io(
                operation="update", data_loc=update_loc,
                name=f"{name}_{filename}", index_label=index_label
            )
            proc = ops._revise(new_data=proc, prev_data=previous_data,
                               revise_rows=revise_rows)
        proc = proc.apply(pd.to_numeric, errors="coerce")

        metadata._set(proc, area="Actividad económica", currency="UYU",
                      inf_adj=meta["Inf. Adj."],
                      unit=unit_,
                      seas_adj=meta["Seas"], ts_type="Flujo",
                      cumperiods=1)

        if save_loc is not None:
            ops._io(
                operation="save", data_loc=save_loc, data=proc,
                name=f"{name}_{filename}", index_label=index_label
            )

        parsed_excels.update({filename: proc})

    return parsed_excels


def _fix_dates(df):
    """Cleanup dates inplace in BCU national accounts files."""
    df.index = df.index.str.replace("*", "")
    df.index = df.index.str.replace(r"\bI \b", "3-", regex=True)
    df.index = df.index.str.replace(r"\bII \b", "6-", regex=True)
    df.index = df.index.str.replace(r"\bIII \b", "9-", regex=True)
    df.index = df.index.str.replace(r"\bIV \b", "12-", regex=True)
    df.index = pd.to_datetime(df.index, format="%m-%Y") + MonthEnd(1)


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def _lin_gdp(update_loc: Union[str, PathLike, Engine,
                               Connection, None] = None,
             save_loc: Union[str, PathLike, Engine,
                             Connection, None] = None,
             name: str = "lin_gdp",
             index_label: str = "index",
             only_get: bool = True,
             only_get_na: bool = True):
    """Get nominal GDP data in UYU and USD with forecasts.

    Update nominal GDP data for use in the `convert.convert_gdp()` function.
    Get IMF forecasts for year of last available data point and the next
    year (for example, if the last period available at the BCU website is
    september 2019, fetch forecasts for 2019 and 2020).

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
    name : str, default 'lin_gdp'
        Either CSV filename for updating and/or saving, or table name if
        using SQL.
    index_label : str, default 'index'
        Label for SQL indexes.
    only_get : bool, default True
        If True, don't download data, retrieve what is available from
        ``update_loc``.
    only_get_na : bool, default True
        If True, don't download national accounts data,
        retrieve what is available from ``update_loc``.

    Returns
    -------
    output : Pandas dataframe
        Quarterly GDP in UYU and USD with 1 year forecasts.

    """
    if only_get is True and update_loc is not None:
        output = ops._io(operation="update", data_loc=update_loc,
                         name=name, index_label=index_label)
        if not output.equals(pd.DataFrame()):
            return output

    data_uyu = get(update_loc=update_loc, only_get=only_get_na)["gdp_cur_nsa"]
    data_uyu = transform.rolling(data_uyu, periods=4, operation="sum")
    data_usd = transform.convert_usd(data_uyu,
                                     update_loc=update_loc,
                                     only_get=only_get)

    data = [data_uyu, data_usd]
    last_year = data_uyu.index.max().year
    if data_uyu.index.max().month == 12:
        last_year += 1

    results = []
    for table, gdp in zip(["NGDP", "NGDPD"], data):
        table_url = (f"https://www.imf.org/external/pubs/ft/weo/2019/02/weodat"
                     f"a/weorept.aspx?sy={last_year - 1}&ey={last_year + 1}"
                     f"&scsm=1&ssd=1&sort=country&ds=.&br=1&pr1.x=27&pr1.y=9&c"
                     f"=298&s={table}&grp=0&a=")
        imf_data = pd.to_numeric(pd.read_html(table_url)[4].iloc[2, [5, 6, 7]])
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
    arrays = []
    for level in range(0, 9):
        arrays.append(list(output.columns.get_level_values(level)))
    arrays[0] = ["PBI UYU", "PBI USD"]
    tuples = list(zip(*arrays))
    output.columns = pd.MultiIndex.from_tuples(tuples,
                                               names=["Indicador", "Área",
                                                      "Frecuencia", "Moneda",
                                                      "Inf. adj.", "Unidad",
                                                      "Seas. Adj.", "Tipo",
                                                      "Acum. períodos"])

    if save_loc is not None:
        ops._io(operation="save", data_loc=save_loc,
                data=output, name=name, index_label=index_label)

    return output
