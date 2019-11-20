import os

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from config import ROOT_DIR
from processing import colnames, update_revise

DATA_PATH = os.path.join(ROOT_DIR, "data")


def get(update=False, revise=0, save=False):

    files = {"https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_101t.xls":
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

    parsed_excels = {}
    for file, metadata in files.items():

        base = pd.read_excel(file, skiprows=9, nrows=metadata["Rows"])
        base_pruned = base.drop(columns=["Unnamed: 0"]).dropna(axis=0, how="all").dropna(axis=1, how="all")
        base_transpose = base_pruned.transpose()
        base_transpose.columns = metadata["Colnames"]
        base_transpose.drop(["Unnamed: 1"], inplace=True)

        fix_na_dates(base_transpose)

        if update is True:
            update_path = os.path.join(DATA_PATH, metadata['Name'] + ".csv")
            base_transpose = update_revise.upd_rev(base_transpose, prev_data=update_path, revise=revise)

        base_transpose = base_transpose.apply(pd.to_numeric, errors="coerce")
        colnames.set_colnames(base_transpose, area="Actividad económica", currency="UYU", inf_adj=metadata["Inf. Adj."],
                              index=metadata["Index"], seas_adj=metadata["Seas"], ts_type="Flujo", cumperiods=1)

        if save is True:
            save_path = os.path.join(DATA_PATH, metadata['Name'] + ".csv")
            base_transpose.to_csv(save_path, sep=" ")

        parsed_excels.update({metadata["Name"]: base_transpose})

    return parsed_excels


def fix_na_dates(df):

    df.index = df.index.str.replace("*", "")
    df.index = df.index.str.replace(r"\bI \b", "3-", regex=True)
    df.index = df.index.str.replace(r"\bII \b", "6-", regex=True)
    df.index = df.index.str.replace(r"\bIII \b", "9-", regex=True)
    df.index = df.index.str.replace(r"\bIV \b", "12-", regex=True)
    df.index = pd.to_datetime(df.index, format="%m-%Y") + MonthEnd(1)


if __name__ == "__main__":
    national_accounts = get(update=True, revise=4, save=True)
