import pandas as pd

from retrieval import nxr, cpi
from processing import freqs, colnames, rolling


def usd(df):

    inferred_freq = pd.infer_freq(df.index)

    nxr_data = nxr.get()

    if df.columns.get_level_values("Type")[0] == "Flow":

        colnames.set_colnames(nxr_data, ts_type="Flow")
        nxr_matching_freq = freqs.freq_resample(nxr_data, target=inferred_freq, operation="average").iloc[:, [1]]

        cum_periods = int(df.columns.get_level_values("Cumulative periods")[0])
        nxr_matching_freq = rolling.rolling(nxr_matching_freq, periods=cum_periods, operation="average")

    else:

        colnames.set_colnames(nxr_data, ts_type="Stock")
        nxr_matching_freq = freqs.freq_resample(nxr_data, target=inferred_freq, operation="average").iloc[:, [3]]

    nxr_to_use = nxr_matching_freq[nxr_matching_freq.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / nxr_to_use)

    colnames.set_colnames(converted_df, currency="USD")

    return converted_df


def real(df, start_date=None, end_date=None):

    inferred_freq = pd.infer_freq(df.index)

    cpi_data = cpi.get()
    colnames.set_colnames(cpi_data, ts_type="Flow")
    cpi_matching_freq = freqs.freq_resample(cpi_data, target=inferred_freq, operation="average").iloc[:, [0]]

    cum_periods = int(df.columns.get_level_values("Cumulative periods")[0])
    cpi_matching_freq = rolling.rolling(cpi_matching_freq, periods=cum_periods, operation="average")

    cpi_to_use = cpi_matching_freq[cpi_matching_freq.index.isin(df.index)].iloc[:, 0]

    if start_date is None:
        converted_df = df.apply(lambda x: x / cpi_to_use)
        col_text = "Const."
    elif end_date is None:
        converted_df = df.apply(lambda x: x / cpi_to_use * cpi_to_use[start_date])
        col_text = f"Const. {start_date}"
    else:
        converted_df = df.apply(lambda x: x / cpi_to_use * cpi_to_use[start_date:end_date].mean())
        col_text = f"Const. {start_date}_{end_date}"

    colnames.set_colnames(converted_df, inf_adj=col_text)

    return converted_df
