import datetime as dt

import pandas as pd
from pandas.tseries.offsets import YearEnd

from retrieval import nxr, cpi, national_accounts
from processing import freqs, colnames


def usd(df):

    inferred_freq = pd.infer_freq(df.index)

    nxr_data = nxr.get()

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

    cpi_data = cpi.get()
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

    gdp_base = national_accounts.get()["na_gdp_cur_nsa"]

    if hifreq is False:
        gdp_freq = freqs.freq_resample(gdp_base, target=inferred_freq, operation="sum")
    else:
        gdp_cum = freqs.rolling(gdp_base, periods=4)
        gdp_freq = freqs.freq_resample(gdp_cum, target=inferred_freq, operation="upsample")

    if df.columns.get_level_values("Unidad/Moneda")[0] == "USD":
        gdp = usd(gdp_freq)
        series_name = "NGDPD"
    else:
        gdp = gdp_freq
        series_name = "NGDP"

    last_year = gdp.index.max().year

    gdp_url = (f"https://www.imf.org/external/pubs/ft/weo/2019/02/weodata/weorept.aspx?sy={last_year-1}&ey={last_year}&"
               f"scsm=1&ssd=1&sort=country&ds=.&br=1&pr1.x=27&pr1.y=9&c=298&s={series_name}&grp=0&a=")
    imf_data = pd.to_numeric(pd.read_html(gdp_url)[4].iloc[2, [5, 6]].reset_index(drop=True))
    forecast = gdp.loc[[dt.datetime(last_year-1, 12, 31)]].multiply(imf_data.iloc[1]).divide(imf_data.iloc[0])
    forecast.index = forecast.index + YearEnd(1)
    gdp = gdp.append(forecast)
    gdp = gdp.resample(inferred_freq).interpolate("linear")

    gdp_to_use = gdp[gdp.index.isin(df.index)].iloc[:, 0]
    converted_df = df.apply(lambda x: x / gdp_to_use)

    colnames.set_colnames(converted_df, currency="% PBI")

    return converted_df
