import datetime as dt
import urllib
from os import PathLike, path, mkdir
from pathlib import Path
from typing import Union, Optional

import pandas as pd
from dateutil.relativedelta import relativedelta

from econuy.utils import metadata
from econuy.utils.lstrings import (reserves_url, reserves_cols,
                                   missing_reserves_url, ff_url)


def get(update_path: Union[str, PathLike, None] = None,
        save_path: Union[str, PathLike, None] = None,
        name: Optional[str] = None) -> pd.DataFrame:
    """Get spot, future and forwards FX operations by the Central Bank.

    Parameters
    ----------
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Daily spot, future and forwards foreign exchange operations : pd.DataFrame

    """
    if name is None:
        name = "fx_spot_ff"
    changes = _reserves_changes(update_path=update_path, save_path=save_path)
    ff = _futures_forwards(update_path=update_path, save_path=save_path)
    spot = changes.iloc[:, [0]]
    fx_ops = pd.merge(spot, ff, how="outer", left_index=True, right_index=True)
    fx_ops = fx_ops.loc[(fx_ops.index >= ff.index.min()) &
                        (fx_ops.index <= spot.index.max())]
    fx_ops = fx_ops.apply(pd.to_numeric, errors="coerce")
    fx_ops = fx_ops.fillna(0)
    fx_ops.columns = ["Spot", "Futuros", "Forwards"]

    metadata._set(fx_ops, area="Reservas internacionales",
                  currency="USD", inf_adj="No", unit="Millones",
                  seas_adj="NSA", ts_type="Flujo", cumperiods=1)
    fx_ops.columns.set_levels(["-"], level=2, inplace=True)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        fx_ops.to_csv(full_save_path)

    return fx_ops


def _reserves_changes(update_path: Union[str, PathLike, None] = None,
                      save_path: Union[str, PathLike, None] = None,
                      name: Optional[str] = None) -> pd.DataFrame:
    """Get international reserves change data.

    Parameters
    ----------
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Daily international reserves changes : pd.DataFrame

    """
    if name is None:
        name = "reserves_chg"
    months = ["ene", "feb", "mar", "abr", "may", "jun",
              "jul", "ago", "set", "oct", "nov", "dic"]
    years = list(range(2013, dt.datetime.now().year + 1))
    files = [month + str(year) for year in years for month in months]

    urls = [f"{reserves_url}{file}.xls" for file in files]
    wrong_may14 = f"{reserves_url}may2014.xls"
    fixed_may14 = f"{reserves_url}mayo2014.xls"
    urls = [fixed_may14 if x == wrong_may14 else x for x in urls]

    if update_path is not None:
        full_update_path = (Path(update_path) / name).with_suffix(".csv")
        try:
            previous_data = pd.read_csv(full_update_path, index_col=0,
                                        header=list(range(9)),
                                        float_precision="high")
            metadata._set(previous_data)
            previous_data.columns.set_levels(["-"], level=2, inplace=True)
            previous_data.columns = reserves_cols[1:46]
            previous_data.index = pd.to_datetime(previous_data.index)
            urls = urls[-18:]
        except FileNotFoundError:
            previous_data = pd.DataFrame()
            pass

    reports = []
    for url in urls:

        try:
            with pd.ExcelFile(url) as xls:
                month_of_report = pd.read_excel(xls, sheet_name="INDICE")
                raw = pd.read_excel(xls, sheet_name="ACTIVOS DE RESERVA",
                                    skiprows=3)
            first_day = month_of_report.iloc[7, 4]
            last_day = (first_day
                        + relativedelta(months=1)
                        - dt.timedelta(days=1))
            proc = raw.dropna(axis=0, thresh=20).dropna(axis=1, thresh=20)
            proc = proc.transpose()
            proc.index.name = "Date"
            proc = proc.iloc[:, 1:46]
            proc.columns = reserves_cols[1:46]
            proc = proc.iloc[1:]
            proc.index = pd.to_datetime(proc.index, errors="coerce")
            proc = proc.loc[proc.index.dropna()]
            proc = proc.loc[first_day:last_day]
            reports.append(proc)

        except urllib.error.HTTPError:
            print(f"{url} could not be reached.")
            pass

    mar14 = pd.read_excel(missing_reserves_url, index_col=0)
    mar14.columns = reserves_cols[1:46]
    reserves = pd.concat(reports + [mar14], sort=False).sort_index()

    if update_path is not None:
        reserves = previous_data.append(reserves, sort=False)
        reserves = reserves.loc[~reserves.index.duplicated(keep="last")]

    reserves = reserves.apply(pd.to_numeric, errors="coerce")
    metadata._set(reserves, area="Reservas internacionales",
                  currency="USD", inf_adj="No", unit="Millones",
                  seas_adj="NSA", ts_type="Flujo", cumperiods=1)
    reserves.columns.set_levels(["-"], level=2, inplace=True)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        reserves.to_csv(full_save_path)

    return reserves


def _futures_forwards(update_path: Union[str, PathLike, None] = None,
                      save_path: Union[str, PathLike, None] = None,
                      name: Optional[str] = None) -> pd.DataFrame:
    """Get future and forwards FX operations by the Central Bank.

    Parameters
    ----------
    update_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or ``None``, don't update.
    save_path : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or ``None``, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Daily future and forwards foreign exchange operations : pd.DataFrame

    """
    if name is None:
        name = "fx_ff"
    dates = pd.bdate_range("2013-11-01",
                           dt.datetime.today()).strftime("%y%m%d").tolist()
    if update_path is not None:
        full_update_path = (Path(update_path) / name).with_suffix(".csv")
        try:
            prev_data = pd.read_csv(full_update_path, index_col=0,
                                    header=list(range(9)),
                                    float_precision="high")
            metadata._set(prev_data)
            prev_data.columns.set_levels(["-"], level=2, inplace=True)
            prev_data.index = pd.to_datetime(prev_data.index)
            last_date = prev_data.index[len(prev_data)-1]
            dates = pd.bdate_range(
                last_date + dt.timedelta(days=1), dt.datetime.today()
            ).strftime("%y%m%d").tolist()
        except FileNotFoundError:
            prev_data = pd.DataFrame()
            pass

    reports = []
    for date in dates:

        try:
            raw_report = pd.read_excel(f"{ff_url}{date}.xls")

            if (dt.datetime.strptime(date, "%y%m%d") >=
                    dt.datetime(2014, 5, 21)):
                future1 = raw_report.iloc[19, ].apply(pd.to_numeric,
                                                      errors="coerce").sum()
                future2 = raw_report.iloc[21, ].apply(pd.to_numeric,
                                                      errors="coerce").sum()
                future = future1 + future2
                forward = raw_report.iloc[24, ].apply(pd.to_numeric,
                                                      errors="coerce").sum()

            else:
                future = raw_report.iloc[19, ].apply(pd.to_numeric,
                                                     errors="coerce").sum()
                forward = raw_report.iloc[22, ].apply(pd.to_numeric,
                                                      errors="coerce").sum()

            reports.append([dt.datetime.strptime(date, "%y%m%d"),
                            future, forward])

        except urllib.error.HTTPError:
            print(f"Report for {date} could not be reached.")
            pass

    try:
        operations = pd.DataFrame(reports)
        operations.columns = ["Date", "Futuros", "Forwards"]
        operations.set_index("Date", inplace=True)
        metadata._set(
            operations, area="Reservas internacionales",  currency="USD",
            inf_adj="No", unit="Millones", seas_adj="NSA",
            ts_type="Flujo", cumperiods=1
        )
        operations.columns.set_levels(["-"], level=2, inplace=True)
        operations = operations.divide(1000)
    except ValueError:
        return prev_data

    if update_path is not None:
        operations = prev_data.append(operations, sort=False)

    if save_path is not None:
        full_save_path = (Path(save_path) / name).with_suffix(".csv")
        if not path.exists(path.dirname(full_save_path)):
            mkdir(path.dirname(full_save_path))
        operations.to_csv(full_save_path)

    return operations
