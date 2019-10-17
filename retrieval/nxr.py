import pandas as pd

from processing import colnames


def get(update=None, revise=0, save=None):

    file = "http://ine.gub.uy/c/document_library/get_file?uuid=3fbf4ffd-a829-420c-aca9-9f01ecd7919a&groupId=10181"

    nxr_raw = pd.read_excel(file, skiprows=4)
    nxr = nxr_raw.dropna(axis=0, thresh=4).set_index("Mes y año").dropna(axis=1, how="all").rename_axis(None)
    nxr.columns = ["Buy, average", "Sell, average", "Buy, EOP", "Sell, EOP"]

    if update is not None:
        previous_data = pd.read_csv(update, sep=" ", index_col=0, header=[0, 1, 2, 3, 4, 5, 6, 7, 8])
        previous_data.index = pd.to_datetime(previous_data.index)
        non_revised = previous_data[:len(previous_data)-revise]
        revised = nxr[len(previous_data)-revise:]
        non_revised.columns = ["Buy, average", "Sell, average", "Buy, EOP", "Sell, EOP"]
        nxr = non_revised.append(revised, sort=False)

    nxr = nxr.apply(pd.to_numeric, errors="coerce")
    colnames.set_colnames(nxr, area="Prices and wages", currency="-", inf_adj="No",
                          index="No", seas_adj="NSA", ts_type="-", cumperiods=1)

    if save is not None:
        nxr.to_csv(save, sep=" ")

    return nxr


if __name__ == "__main__":
    exchange_rate = get(update="../data/nxr.csv", revise=6, save="../data/nxr.csv")
