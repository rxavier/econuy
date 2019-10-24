import pandas as pd

from processing import colnames, update_revise


def get(update=None, revise=0, save=None):

    file = "http://ine.gub.uy/c/document_library/get_file?uuid=2e92084a-94ec-4fec-b5ca-42b40d5d2826&groupId=10181"

    cpi_raw = pd.read_excel(file, skiprows=7).dropna(axis=0, thresh=2)
    cpi = (cpi_raw.drop(["Mensual", "Acum.año", "Acum.12 meses"], axis=1).
           dropna(axis=0, how="all").set_index("Mes y año").rename_axis(None))
    cpi.columns = ["CPI index"]

    if update is not None:
        cpi = update_revise.upd_rev(cpi, prev_data=update, revise=revise)

    cpi = cpi.apply(pd.to_numeric, errors="coerce")
    colnames.set_colnames(cpi, area="Prices and wages", currency="-", inf_adj="No",
                          index="2010-10-31", seas_adj="NSA", ts_type="-", cumperiods=1)

    if save is not None:
        cpi.to_csv(save, sep=" ")

    return cpi


if __name__ == "__main__":
    prices = get(update="../data/cpi.csv", revise=6, save="../data/cpi.csv")
