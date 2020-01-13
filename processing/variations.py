import pandas as pd

from processing import colnames, freqs


def var_diff(df, operation="var", period_op="last"):

    inferred_freq = pd.infer_freq(df.index)

    type_change = {"last":
                   {"var": [lambda x: x.pct_change(periods=1), "% variación"],
                    "diff": [lambda x: x.diff(periods=1, min_periods=1), "Cambio"]},
                   "inter":
                   {"var": [lambda x: x.pct_change(periods=last_year), "% variación interanual"],
                    "diff": [lambda x: x.diff(periods=last_year), "Cambio interanual"]},
                   "annual":
                   {"var": [lambda x: x.pct_change(periods=last_year), "% variación anual"],
                    "diff": [lambda x: x.diff(periods=last_year), "Cambio anual"]}}

    if inferred_freq == "M":
        last_year = 12
    elif inferred_freq == "Q" or inferred_freq == "Q-DEC":
        last_year = 4
    elif inferred_freq == "A":
        last_year = 1
    else:
        raise ValueError("The dataframe needs to have a frequency of M (month end), Q (quarter end) or A (year end)")

    if period_op == "annual":

        if df.columns.get_level_values("Tipo")[0] == "Stock":
            output = df.apply(type_change[period_op][operation][0]).multiply(100)
        else:
            output = freqs.rolling(df, operation="sum")
            output = output.apply(type_change[period_op][operation][0]).multiply(100)

        colnames.set_colnames(output, index=type_change[period_op][operation][1])

    else:
        output = df.apply(type_change[period_op][operation][0]).multiply(100)
        colnames.set_colnames(output, index=type_change[period_op][operation][1])

    return output
