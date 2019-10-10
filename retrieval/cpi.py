import pandas as pd

from processing import colnames


def get(update=False, revise=0, save=False):

    file = "http://ine.gub.uy/c/document_library/get_file?uuid=2e92084a-94ec-4fec-b5ca-42b40d5d2826&groupId=10181"

    cpi_raw = pd.read_excel(file, skiprows=7).dropna(axis=0, thresh=2)
    cpi = (cpi_raw.drop(["Mensual", "Acum.año", "Acum.12 meses"], axis=1).
           dropna(axis=0, how="all").set_index("Mes y año").rename_axis(None))
    cpi.columns = ["CPI index"]
    cpi = cpi.apply(pd.to_numeric, errors="coerce")

    if update is not False:
        previous_data = pd.read_csv(update, sep=" ", index_col=0, header=[0, 1, 2, 3, 4, 5, 6, 7, 8])
        previous_data.index = pd.to_datetime(previous_data.index)
        non_revised = previous_data[:len(previous_data)-revise]
        revised = cpi[len(previous_data)-revise:]
        non_revised.columns = ["CPI index"]
        cpi = non_revised.append(revised, sort=True)

    colnames.set_colnames(cpi, area="Prices and wages", currency="-", inf_adj="No",
                          index="2010-10-31", seas_adj="NSA", ts_type="-", cumperiods=1)

    if save is not False:
        cpi.to_csv(save, sep=" ")

    return cpi


if __name__ == "__main__":
    prices = get(update="data/cpi.csv", revise=6, save="data/cpi.csv")
