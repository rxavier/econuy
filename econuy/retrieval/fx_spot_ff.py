from os import PathLike
from pathlib import Path
from typing import Union

import pandas as pd

from econuy.resources import columns
from econuy.retrieval import fx_ff, reserves_chg


def get(update: Union[str, PathLike, None] = None,
        save: Union[str, PathLike, None] = None,
        name: Union[str, None] = None):
    """Get spot, future and forwards FX operations by the Central Bank."""
    if name is None:
        name = "fx_spot_ff"
    changes = reserves_chg.get(update=update, save=save)
    ff = fx_ff.get(update=update, save=save)
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

    if save is not None:
        save_path = (Path(save) / name).with_suffix(".csv")
        fx_ops.to_csv(save_path)

    return fx_ops
