from os import PathLike
from typing import Union

import pandas as pd

from econuy.resources import columns, updates
from econuy.retrieval import fx_ff, reserves_chg


def get(save: Union[str, PathLike, bool] = False):
    """Get spot, future and forwards FX operations by the Central Bank."""
    changes = reserves_chg.get(update=True, save=True)
    ff = fx_ff.get(update=True, save=True)
    spot = changes.iloc[:, 0]
    fx_ops = pd.merge(spot, ff, how="outer", left_index=True, right_index=True)
    fx_ops = fx_ops.loc[(fx_ops.index >= ff.index.min()) &
                        (fx_ops.index <= spot.index.max())]
    fx_ops = fx_ops.apply(pd.to_numeric, errors="coerce")
    fx_ops = fx_ops.fillna(0)
    fx_ops.columns = ["Spot", "Futuros", "Forwards"]

    columns._setmeta(fx_ops, area="Reservas internacionales",
                     currency="USD", inf_adj="No", index="No",
                     seas_adj="NSA", ts_type="Flujo", cumperiods=1)

    if save is not False:
        save_path = updates._paths(save, multiple=False,
                                   name="fx_spot_ff.csv")
        fx_ops.to_csv(save_path)

    return fx_ops
