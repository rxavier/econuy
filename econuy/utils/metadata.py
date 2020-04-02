from typing import Optional
import warnings

import pandas as pd


def _set(
        df: pd.DataFrame, area: Optional[str] = None,
        currency: Optional[str] = None, inf_adj: Optional[str] = None,
        unit: Optional[str] = None, seas_adj: Optional[str] = None,
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
    unit : str or None (default is None)
        Units in which data is defined.
    seas_adj : str or None (default is None)
        Whether the data is seasonally adjusted.
    ts_type : str or None (default is None)
        Time series type, generally 'Stock' or 'Flujo'.
    cumperiods : int or None (default is None)
        Number of periods accumulated per period.

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
        warnings.warn("Metadata: frequency could not be inferred "
                      "from the index. Setting to '-'.", UserWarning)
        inferred_freq = "-"
    names = [
        "Indicador", "Área", "Frecuencia", "Moneda",
        "Inf. adj.", "Unidad", "Seas. Adj.", "Tipo", "Acum. períodos"
    ]
    if not isinstance(df.columns, pd.MultiIndex):
        df.columns = pd.MultiIndex.from_product(
            [
                colnames, [area], [inferred_freq], [currency], [inf_adj],
                [unit], [seas_adj], [ts_type], [cumperiods]
            ],
            names=names
        )
    else:
        arrays = []
        for level in range(0, 9):
            arrays.append(list(df.columns.get_level_values(level)))

        arrays[2] = [inferred_freq] * len(df.columns)
        if area is not None:
            arrays[1] = [area] * len(df.columns)
        if currency is not None:
            arrays[3] = [currency] * len(df.columns)
        if inf_adj is not None:
            arrays[4] = [inf_adj] * len(df.columns)
        if unit is not None:
            arrays[5] = [unit] * len(df.columns)
        if seas_adj is not None:
            arrays[6] = [seas_adj] * len(df.columns)
        if ts_type is not None:
            arrays[7] = [ts_type] * len(df.columns)
        if cumperiods is not None:
            arrays[8] = [cumperiods] * len(df.columns)

        try:
            arrays[8] = list(map(int, arrays[8]))
        except ValueError:
            pass

        tuples = list(zip(*arrays))
        index = pd.MultiIndex.from_tuples(tuples, names=names)
        df.columns = index
