import pandas as pd

from retrieval import nxr, cpi, national_accounts
from processing import freqs, colnames


def usd(df):

    inferred_freq = pd.infer_freq(df.index)

    nxr_data = nxr.get(update="nxr.csv", revise_rows=6, save="nxr.csv", force_update=False)

    if df.columns.get_level_values("Tipo")[0] == "Flujo":

        colnames.set_colnames(nxr_data, ts_type="Flujo")
        nxr_matching_freq = freqs.freq_resample(nxr_data, target=inferred_freq, operation="average").iloc[:, [1]]

        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        nxr_matching_freq = freqs.rolling(nxr_matching_freq, periods=cum_periods, operation="average")

    else:

        colnames.set_colnames(nxr_data, ts_type="Stock")
        nxr_matching_freq = freqs.freq_resample(nxr_data, target=inferred_freq, operation="average").iloc[:, [3]]

    nxr_to_use = nxr_matching_freq[nxr_matching_freq.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / nxr_to_use)

    colnames.set_colnames(converted_df, currency="USD")

    return converted_df


def real(df, start_date=None, end_date=None):

    inferred_freq = pd.infer_freq(df.index)

    cpi_data = cpi.get(update="cpi.csv", revise_rows=6, save="cpi.csv", force_update=False)
    colnames.set_colnames(cpi_data, ts_type="Flujo")
    cpi_matching_freq = freqs.freq_resample(cpi_data, target=inferred_freq, operation="average").iloc[:, [0]]

    cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
    cpi_matching_freq = freqs.rolling(cpi_matching_freq, periods=cum_periods, operation="average")

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


def pcgdp(df, hifreq=True):

    inferred_freq = pd.infer_freq(df.index)

    gdp_base = national_accounts.lin_gdp(update="lin_gdp.csv", save="lin_gdp.csv", force_update=False)

    if hifreq is True:
        gdp_base = freqs.freq_resample(gdp_base, target=inferred_freq, operation="upsample", interpolation="linear")
    else:
        gdp_base = gdp_base.resample(inferred_freq, convention="end").asfreq()

    if df.columns.get_level_values("Unidad/Moneda")[0] == "USD":
        gdp_base = gdp_base.iloc[:, 1].to_frame()
    else:
        gdp_base = gdp_base.iloc[:, 0].to_frame()

    gdp_to_use = gdp_base[gdp_base.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / gdp_to_use)

    colnames.set_colnames(converted_df, currency="% PBI")

    return converted_df
