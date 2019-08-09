import pandas as pd
from pandas.tseries.offsets import MonthEnd

from processing import colnames

FILES = {"https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_101t.xls":
         {"Rows": 12, "Inf. Adj.": "Const. 2005", "Index": "No", "Seas": "NSA"},
         "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_100t.xls":
         {"Rows": 12, "Inf. Adj.": "Current", "Index": "No", "Seas": "NSA"},
         "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_104t.xls":
         {"Rows": 10, "Inf. Adj.": "Const. 2005", "Index": "No", "Seas": "NSA"},
         "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_132t.xls":
         {"Rows": 12, "Inf. Adj.": "Const. 2005", "Index": "2005=100", "Seas": "NSA"},
         "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_133t.xls":
         {"Rows": 12, "Inf. Adj.": "Const. 2005", "Index": "2005=100", "Seas": "SA"}}


def parse_excel(dictionary):

    parsed_excels = {}
    for file, metadata in dictionary.items():

        base = pd.read_excel(file, skiprows=9, nrows=metadata["Rows"])
        base_pruned = base.drop(columns=["Unnamed: 0"]).dropna(axis=0, how="all").dropna(axis=1, how="all")
        base_transpose = base_pruned.transpose()
        base_transpose.columns = base_transpose.iloc[0]
        base_transpose.drop(["Unnamed: 1"], inplace=True)
        base_transpose = base_transpose.apply(pd.to_numeric, errors="coerce")

        fix_na_dates(base_transpose)
        colnames.set_colnames(base_transpose, area="National accounts", currency="UYU", inf_adj=metadata["Inf. Adj."],
                              index=metadata["Index"], seas_adj=metadata["Seas"], ts_type="Flow", cumperiods=1)

        name = file.split("/")[-1].replace(".xls", "")

        parsed_excels.update({name: base_transpose})

    return parsed_excels


def fix_na_dates(df):

    df.index = df.index.str.replace("*", "")
    df.index = df.index.str.replace(r"\bI \b", "3-", regex=True)
    df.index = df.index.str.replace(r"\bII \b", "6-", regex=True)
    df.index = df.index.str.replace(r"\bIII \b", "9-", regex=True)
    df.index = df.index.str.replace(r"\bIV \b", "12-", regex=True)
    df.index = pd.to_datetime(df.index, format="%m-%Y") + MonthEnd(1)


if __name__ == "__main__":
    national_accounts = parse_excel(FILES)
