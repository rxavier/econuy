import os
import datetime as dt
from pathlib import Path
from typing import Union

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from config import ROOT_DIR
from processing import columns, updates, convert, freqs

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 80
FILES = {"https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_101t.xls":
             {"Rows": 12, "Inf. Adj.": "Const. 2005", "Index": "No", "Seas": "NSA", "Name": "na_ind_con_nsa",
              "Colnames": ["PBI: Actividades primarias", "PBI: Agricultura, ganadería, caza y silvucultura",
                           "PBI: Industrias manufactureras", "PBI: Suministro de electricidad, gas y agua",
                           "PBI: Construcción", "PBI: Comercio, reparaciones, restaurantes y hoteles",
                           "PBI: Transporte, almacenamiento y comunicaciones", "PBI: Otras actividades",
                           "PBI: SIFMI", "PBI: Impuestos menos subvenciones", "PBI"]},
         "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_100t.xls":
             {"Rows": 12, "Inf. Adj.": "Corriente", "Index": "No", "Seas": "NSA", "Name": "na_ind_cur_nsa",
              "Colnames": ["PBI: Actividades primarias", "PBI: Agricultura, ganadería, caza y silvucultura",
                           "PBI: Industrias manufactureras", "PBI: Suministro de electricidad, gas y agua",
                           "PBI: Construcción", "PBI: Comercio, reparaciones, restaurantes y hoteles",
                           "PBI: Transporte, almacenamiento y comunicaciones", "PBI: Otras actividades",
                           "PBI: SIFMI", "PBI: Impuestos menos subvenciones", "PBI"]},
         "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_104t.xls":
             {"Rows": 10, "Inf. Adj.": "Const. 2005", "Index": "No", "Seas": "NSA", "Name": "na_gas_con_nsa",
              "Colnames": ["PBI: Gasto total", "PBI: Gasto privado", "PBI: Gasto público",
                           "PBI: Formación bruta de capital", "PBI: Formación bruta de capital fijo",
                           "PBI: Formación bruta de capital fijo pública",
                           "PBI: Formación bruta de capital fijo privada", "PBI: Exportaciones",
                           "PBI: Importaciones", "PBI"]},
         "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_132t.xls":
             {"Rows": 12, "Inf. Adj.": "Const. 2005", "Index": "2005=100", "Seas": "NSA", "Name": "na_ind_con_idx_nsa",
              "Colnames": ["PBI: Actividades primarias", "PBI: Agricultura, ganadería, caza y silvucultura",
                           "PBI: Industrias manufactureras", "PBI: Suministro de electricidad, gas y agua",
                           "PBI: Construcción", "PBI: Comercio, reparaciones, restaurantes y hoteles",
                           "PBI: Transporte, almacenamiento y comunicaciones", "PBI: Otras actividades",
                           "PBI: SIFMI", "PBI: Impuestos menos subvenciones", "PBI"]},
         "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_133t.xls":
             {"Rows": 12, "Inf. Adj.": "Const. 2005", "Index": "2005=100", "Seas": "SA", "Name": "na_ind_con_idx_sa",
              "Colnames": ["PBI: Actividades primarias", "PBI: Agricultura, ganadería, caza y silvucultura",
                           "PBI: Industrias manufactureras", "PBI: Suministro de electricidad, gas y agua",
                           "PBI: Construcción", "PBI: Comercio, reparaciones, restaurantes y hoteles",
                           "PBI: Transporte, almacenamiento y comunicaciones", "PBI: Otras actividades",
                           "PBI: SIFMI", "PBI: Impuestos menos subvenciones", "PBI"]},
         "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_130t.xls":
             {"Rows": 2, "Inf. Adj.": "Corriente", "Index": "No", "Seas": "NSA", "Name": "na_gdp_cur_nsa",
              "Colnames": ["PBI"]}}

def get(update: bool = False, revise_rows: int = 0,
        save: bool = False, force_update: bool = False):
    """Get national accounts data.

    Parameters
    ----------
    update : bool (default is False)
        If true, try to update existing data on disk.
    revise_rows : int (default is 0)
        How many rows of old data to replace with new data.
    save : bool (default is False)
        If true, save output dataframe in CSV format.
    force_update : bool (default is False)
        If True, fetch data and update existing data even if it was modified
        within its update window (for national accounts data, 80 days)

    Returns
    -------
    dataframe : Pandas dataframe

    """
    parsed_excels = {}
    for file, metadata in FILES.items():

        if update is True:
            update_path = os.path.join(DATA_PATH, metadata['Name'] + ".csv")
            delta, previous_data = updates.check_modified(update_path)

            if delta < update_threshold and force_update is False:
                print(f"{metadata['Name']}.csv was modified within"
                      f" {update_threshold} day(s). Skipping download...")
                parsed_excels.update({metadata["Name"]: previous_data})
                continue

        raw = pd.read_excel(file, skiprows=9, nrows=metadata["Rows"])
        proc = (raw.drop(columns=["Unnamed: 0"]).
                       dropna(axis=0, how="all").dropna(axis=1, how="all"))
        proc = proc.transpose()
        proc.columns = metadata["Colnames"]
        proc.drop(["Unnamed: 1"], inplace=True)

        fix_na_dates(proc)

        if metadata["Index"] == "No":
            proc = proc.divide(1000)
        if update is True:
            proc = updates.revise(new_data=proc, prev_data=previous_data,
                                  revise_rows=revise_rows)
        proc = proc.apply(pd.to_numeric, errors="coerce")

        columns.set_metadata(proc, area="Actividad económica", currency="UYU",
                             inf_adj=metadata["Inf. Adj."],
                             index=metadata["Index"], seas_adj=metadata["Seas"],
                             ts_type="Flujo", cumperiods=1)

        if save is True:
            save_path = os.path.join(DATA_PATH, metadata['Name'] + ".csv")
            proc.to_csv(save_path, sep=" ")

        parsed_excels.update({metadata["Name"]: proc})

    return parsed_excels


def fix_na_dates(df):
    """Cleanup dates inplace in BCU national accounts files.

    Parameters
    ----------
    df : Pandas dataframe

    Returns
    -------
    None

    """
    df.index = df.index.str.replace("*", "")
    df.index = df.index.str.replace(r"\bI \b", "3-", regex=True)
    df.index = df.index.str.replace(r"\bII \b", "6-", regex=True)
    df.index = df.index.str.replace(r"\bIII \b", "9-", regex=True)
    df.index = df.index.str.replace(r"\bIV \b", "12-", regex=True)
    df.index = pd.to_datetime(df.index, format="%m-%Y") + MonthEnd(1)


def lin_gdp(update: Union[str, Path, None] = None,
            save: Union[str, Path, None] = None,
            force_update: bool = False):
    """Get nominal GDP data in UYU and USD with forecasts.

    Update nominal GDP data for use in the `convert.pcgdp()` function.
    Get IMF forecasts for year of last available data point and the next
    year (for example, if the last period available at the BCU website is
    september 2019, fetch forecasts for 2019 and 2020).

    Parameters
    ----------
    update : str, Path or None (default is None)
        Path or path-like string pointing to a CSV file for updating.
    save : str, Path or None (default is None)
        Path or path-like string where to save the output dataframe in CSV
        format.
    force_update : bool (default is False)
        If True, fetch data and update existing data even if it was modified
        within its update window (for national accounts data, 80 days)

    Returns
    -------
    dataframe : Pandas dataframe

    """
    if update is not None:
        update_path = os.path.join(DATA_PATH, update)
        delta, previous_data = updates.check_modified(update_path)

        if delta < update_threshold and force_update is False:
            print(f"{update} was modified within {update_threshold} day(s). "
                  f"Skipping download...")
            return previous_data

    data_uyu = get(update=True, revise_rows=4, save=True,
                   force_update=False)["na_gdp_cur_nsa"]
    data_uyu = freqs.rolling(data_uyu, periods=4, operation="sum")
    data_usd = convert.usd(data_uyu)

    data = [data_uyu, data_usd]
    last_year = data_uyu.index.max().year
    if data_uyu.index.max().month == 12:
        last_year += 1

    results = []
    for table, gdp in zip(["NGDP", "NGDPD"], data):

        table_url = (f"https://www.imf.org/external/pubs/ft/weo/2019/02/weodat"
                     f"a/weorept.aspx?sy={last_year-1}&ey={last_year+1}&scsm=1"
                     f"&ssd=1&sort=country&ds=.&br=1&pr1.x=27&pr1.y=9&c=298&s"
                     f"={table}&grp=0&a=")
        imf_data = pd.to_numeric(pd.read_html(table_url)[4].iloc[2, [5, 6, 7]])
        imf_data = imf_data.reset_index(drop=True)
        fcast = (gdp.loc[[dt.datetime(last_year-1, 12, 31)]].
                 multiply(imf_data.iloc[1]).divide(imf_data.iloc[0]))
        fcast = fcast.rename(index={dt.datetime(last_year-1, 12, 31):
                                    dt.datetime(last_year, 12, 31)})
        next_fcast = (gdp.loc[[dt.datetime(last_year-1, 12, 31)]].
                      multiply(imf_data.iloc[2]).divide(imf_data.iloc[0]))
        next_fcast = next_fcast.rename(index={dt.datetime(last_year-1, 12, 31):
                                              dt.datetime(last_year+1, 12, 31)})
        fcast = fcast.append(next_fcast)
        gdp = gdp.append(fcast)
        results.append(gdp)

    output = pd.concat(results, axis=1)
    output = output.resample("Q-DEC").interpolate("linear")

    if save is not None:
        save_path = os.path.join(DATA_PATH, save)
        output.to_csv(save_path, sep=" ")

    return output
