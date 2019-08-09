import pandas as pd

from processing import colnames


def get():

    file = "http://ine.gub.uy/c/document_library/get_file?uuid=3fbf4ffd-a829-420c-aca9-9f01ecd7919a&groupId=10181"

    nxr_raw = pd.read_excel(file, skiprows=4)
    nxr = nxr_raw.dropna(axis=0, thresh=4).set_index("Mes y año").dropna(axis=1, how="all")
    nxr.columns = ["Buy, average", "Sell, average", "Buy, EOP", "Sell, EOP"]
    nxr = nxr.apply(pd.to_numeric, errors="coerce")

    colnames.set_colnames(nxr, area="Prices and wages", currency="-", inf_adj="No",
                          index="No", seas_adj="NSA", ts_type="-", cumperiods=1)

    return nxr


if __name__ == "__main__":
    exchange_rate = get()
