import pandas as pd


def set_colnames(df, area=None, currency=None, inf_adj=None, index=None, seas_adj=None, ts_type=None, cumperiods=None):

    colnames = df.columns
    inferred_freq = pd.infer_freq(df.index)

    if not isinstance(df.columns, pd.core.index.MultiIndex):
        df.columns = pd.MultiIndex.from_product([colnames, [area], [inferred_freq], [currency],
                                                 [inf_adj], [index], [seas_adj], [ts_type], [cumperiods]],
                                                names=["Indicador", "Área", "Frecuencia", "Unidad/Moneda", "Inf. adj.",
                                                       "Índice", "Seas. Adj.", "Tipo", "Acum. períodos"])
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
