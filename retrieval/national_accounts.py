import pandas as pd
from pandas.tseries.offsets import MonthEnd

from processing import colnames


def get(update=None, revise=0, save=None):

    files = {"https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_101t.xls":
             {"Rows": 12, "Inf. Adj.": "Const. 2005", "Index": "No", "Seas": "NSA", "Name": "na_ind_con_nsa"},
             "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_100t.xls":
             {"Rows": 12, "Inf. Adj.": "Current", "Index": "No", "Seas": "NSA", "Name": "na_ind_cur_nsa"},
             "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_104t.xls":
             {"Rows": 10, "Inf. Adj.": "Const. 2005", "Index": "No", "Seas": "NSA", "Name": "na_gas_con_nsa"},
             "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_132t.xls":
             {"Rows": 12, "Inf. Adj.": "Const. 2005", "Index": "2005=100", "Seas": "NSA", "Name": "na_ind_con_idx_nsa"},
             "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_133t.xls":
             {"Rows": 12, "Inf. Adj.": "Const. 2005", "Index": "2005=100", "Seas": "SA", "Name": "na_ind_con_idx_sa"}}

    parsed_excels = {}
    for file, metadata in files.items():

        base = pd.read_excel(file, skiprows=9, nrows=metadata["Rows"])
        base_pruned = base.drop(columns=["Unnamed: 0"]).dropna(axis=0, how="all").dropna(axis=1, how="all")
        base_transpose = base_pruned.transpose()
        base_transpose.columns = base_transpose.iloc[0]
        base_transpose.drop(["Unnamed: 1"], inplace=True)

        fix_na_dates(base_transpose)

        if update is not None:
            previous_data = pd.read_csv(f"../data/{metadata['Name']}.csv", sep=" ",
                                        index_col=0, header=[0, 1, 2, 3, 4, 5, 6, 7, 8])
            previous_data.index = pd.to_datetime(previous_data.index)
            non_revised = previous_data[:len(previous_data)-revise]
            revised = base_transpose[len(previous_data)-revise:]
            non_revised.columns = base_transpose.columns
            base_transpose = non_revised.append(revised, sort=False)

        base_transpose = base_transpose.apply(pd.to_numeric, errors="coerce")
        colnames.set_colnames(base_transpose, area="National accounts", currency="UYU", inf_adj=metadata["Inf. Adj."],
                              index=metadata["Index"], seas_adj=metadata["Seas"], ts_type="Flow", cumperiods=1)

        if save is not None:
            base_transpose.to_csv(f"../data/{metadata['Name']}.csv", sep=" ")

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
    national_accounts = get(update=True, revise=4, save=True)
