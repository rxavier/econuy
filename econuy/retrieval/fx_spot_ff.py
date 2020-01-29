import os

import pandas as pd

from econuy.config import ROOT_DIR
from econuy.processing import columns
from econuy.retrieval import fx_ff, reserves_chg

data_path = os.path.join(ROOT_DIR, "data")
path_chg = os.path.join(data_path, "reserves_chg.csv")
path_ff = os.path.join(data_path, "fx_ff.csv")
path_save = os.path.join(data_path, "fx_spot_ff.csv")

changes = reserves_chg.get_reserves_chg(files=reserves_chg.files_,
                                        online_files=None, offline_files=None,
                                        update=path_chg, save=path_chg)
ff = fx_ff.get(update=path_ff, save=path_ff)

spot = changes.iloc[:, 0]

fx_ops = pd.merge(spot, ff, how="outer", left_index=True, right_index=True)
fx_ops = fx_ops.loc[(fx_ops.index >= ff.index.min()) &
                    (fx_ops.index <= spot.index.max())]
fx_ops = fx_ops.apply(pd.to_numeric, errors="coerce")
fx_ops = fx_ops.fillna(0)
fx_ops.columns = ["Spot", "Futuros", "Forwards"]

columns.set_metadata(fx_ops, area="Reservas internacionales", currency="USD",
                     inf_adj="No", index="No", seas_adj="NSA", ts_type="Flujo",
                     cumperiods=1)

fx_ops.to_csv(path_save, sep=" ")
