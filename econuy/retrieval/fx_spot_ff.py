from os import PathLike
from pathlib import Path
from typing import Union, Optional

import pandas as pd

from econuy.resources import columns
from econuy.retrieval import fx_ff, reserves_chg


def get(update: Union[str, PathLike, None] = None,
        save: Union[str, PathLike, None] = None,
        name: Optional[str] = None) -> pd.DataFrame:
    """Get spot, future and forwards FX operations by the Central Bank.

    Parameters
    ----------
    update : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to find a CSV
        for updating, or None, don't update.
    save : str, os.PathLike or None, default None
        Path or path-like string pointing to a directory where to save the CSV,
        or None, don't save.
    name : str, default None
        CSV filename for updating and/or saving.

    Returns
    -------
    Daily spot, future and forwards foreign exchange operations : pd.DataFrame

    """
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
