from typing import Optional

import pandas as pd


def set_metadata(
        df: pd.DataFrame, area: Optional[str] = None,
        currency: Optional[str] = None, inf_adj: Optional[str] = None,
        index: Optional[str] = None, seas_adj: Optional[str] = None,
        ts_type: Optional[str] = None, cumperiods: Optional[int] = None
):
    """Add a multiindex to a dataframe's columns.

    Characterize a dataframe by adding metadata to its column names by
    use of multiindexes.

    Parameters
    ----------
    df : Pandas dataframe
    area : str or None (default is None)
        Topic to which the data relates to.
    currency : str or None (default is None)
        Currency denomination.
    inf_adj : str or None (default is None)
        Whether the data is in constant prices.
    index : str or None (default is None)
        Whether the data is some type of index.
    seas_adj : str or None (default is None)
        Whether the data is seasonally adjusted.
    ts_type : str or None (default is None)
        Time series type, generally 'stock' or 'flow'.
    cumperiods : int or None (default is None)
        Number of periods accumulated per observation.

    Returns
    -------
    None

    See also
    --------
    Modifies the dataframe's column names in place.

    """
    colnames = df.columns
    inferred_freq = pd.infer_freq(df.index)
    if inferred_freq is None:
        print("Frequency could not be inferred from the index.")
        inferred_freq = "-"

    if not isinstance(df.columns, pd.MultiIndex):
        df.columns = pd.MultiIndex.from_product(
            [
                colnames, [area], [inferred_freq], [currency], [inf_adj],
                [index], [seas_adj], [ts_type], [cumperiods]
            ],
            names=[
                "Indicador", "Área", "Frecuencia", "Unidad/Moneda",
                "Inf. adj.", "Índice", "Seas. Adj.", "Tipo", "Acum. períodos"
            ]
        )
    else:
        df.columns = df.columns.set_levels([inferred_freq], level=2)

        if area is not None:
            df.columns = df.columns.set_levels([area], level=1)

        if currency is not None:
            df.columns = df.columns.set_levels([currency], level=3)

        if inf_adj is not None:
            df.columns = df.columns.set_levels([inf_adj], level=4)

        if index is not None:
            df.columns = df.columns.set_levels([index], level=5)

        if seas_adj is not None:
            df.columns = df.columns.set_levels([seas_adj], level=6)

        if ts_type is not None:
            df.columns = df.columns.set_levels([ts_type], level=7)

        if cumperiods is not None:
            df.columns = df.columns.set_levels([cumperiods], level=8)
