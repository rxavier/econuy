# import datetime as dt
# import tempfile
# import zipfile
# import os
# from io import BytesIO
# from os import path

# import pandas as pd
# import httpx
# from pandas.tseries.offsets import MonthEnd, QuarterEnd
# from dotenv import load_dotenv

# from econuy.transform import decompose, rebase
# from econuy.utils import metadata
# from econuy.utils.operations import get_download_sources, get_name_from_function


# load_dotenv()
# FRED_API_KEY = os.environ.get("FRED_API_KEY")


# def global_gdp() -> pd.DataFrame:
#     """Get seasonally adjusted real quarterly GDP for select countries.

#     Countries/aggregates are US, EU-27, Japan and China.

#     Returns
#     -------
#     Quarterly real GDP in seasonally adjusted terms : pd.DataFrame

#     """
#     name = get_name_from_function()
#     sources = get_download_sources(name)

#     if FRED_API_KEY is None:
#         raise ValueError(
#             "FRED_API_KEY not found. Get one at https://fredaccount.stlouisfed.org/apikeys and set it as an environment variable."
#         )

#     chn_y = dt.datetime.now().year + 1
#     chn_r = httpx.get(sources["chn_oecd"].format(f"{chn_y}-Q4"))
#     chn_datasets = (
#         pd.read_csv(BytesIO(chn_r.content))[
#             ["TIME_PERIOD", "TRANSFORMATION", "OBS_VALUE"]
#         ]
#         .pivot(index="TIME_PERIOD", columns="TRANSFORMATION", values="OBS_VALUE")
#         .rename_axis(None)
#         .rename_axis(None, axis=1)
#     )
#     chn_datasets.index = pd.to_datetime(
#         chn_datasets.index, format="mixed"
#     ) + QuarterEnd(0)
#     chn_qoq = chn_datasets.iloc[:, [0]]
#     chn_yoy = chn_datasets.iloc[:, [1]]
#     chn_obs = pd.read_csv(
#         sources["chn_obs"], index_col=0, parse_dates=[0], usecols=[0, 1]
#     )
#     chn_obs = chn_obs.loc[
#         (chn_obs.index > "2011-01-01") & (chn_obs.index < "2016-01-01")
#     ]
#     chn_yoy["volume"] = chn_obs
#     for row in reversed(range(len(chn_yoy.loc[chn_yoy.index < "2011-01-01"]))):
#         if pd.isna(chn_yoy.iloc[row, 1]):
#             chn_yoy.iloc[row, 1] = chn_yoy.iloc[row + 4, 1] / (
#                 1 + chn_yoy.iloc[row + 4, 0] / 100
#             )
#     chn_yoy = chn_yoy[["volume"]].loc[chn_yoy.index < "2016-01-01"]
#     metadata._set(chn_yoy)
#     chn_sa = decompose(
#         chn_yoy[["volume"]].loc[chn_yoy.index < "2016-01-01"],
#         component="seas",
#         method="x13",
#     )
#     chn_sa = pd.concat([chn_sa, chn_qoq], axis=1)
#     for row in range(len(chn_sa)):
#         if not pd.isna(chn_sa.iloc[row, 1]):
#             chn_sa.iloc[row, 0] = chn_sa.iloc[row - 1, 0] * (
#                 1 + chn_sa.iloc[row, 1] / 100
#             )
#     chn = chn_sa.iloc[:, [0]].div(10)

#     gdps = []
#     for series in ["GDPC1", "CLVMNACSCAB1GQEU272020", "JPNRGDPEXP"]:
#         r = httpx.get(
#             f"{sources['fred']}{series}&api_key=" f"{FRED_API_KEY}&file_type=json"
#         )
#         aux = pd.DataFrame.from_records(r.json()["observations"])
#         aux = aux[["date", "value"]].set_index("date")
#         aux.index = pd.to_datetime(aux.index)
#         aux.index = aux.index.shift(3, freq="ME") + MonthEnd(0)
#         aux.columns = [series]
#         aux = aux.apply(pd.to_numeric, errors="coerce")
#         if series == "GDPC1":
#             aux = aux.div(4)
#         elif series == "CLVMNACSCAB1GQEU272020":
#             aux = aux.div(1000)
#         gdps.append(aux)
#     gdps = pd.concat(gdps, axis=1)

#     output = pd.concat([gdps, chn], axis=1)
#     output.columns = ["Estados Unidos", "Unión Europea", "Japón", "China"]
#     output.rename_axis(None, inplace=True)

#     metadata._set(
#         output,
#         area="Global",
#         currency="USD",
#         inf_adj="Const.",
#         unit="Miles de millones",
#         seas_adj="SA",
#         ts_type="Flujo",
#         cumperiods=1,
#     )
#     metadata._modify_multiindex(
#         output, levels=[3], new_arrays=[["USD", "EUR", "JPY", "CNY"]]
#     )

#     return output


# def global_stock_markets() -> pd.DataFrame:
#     """Get stock market index data.

#     Indexes selected are S&P 500, Euronext 100, Nikkei 225 and Shanghai
#     Composite.

#     Returns
#     -------
#     Daily stock market index in USD : pd.DataFrame

#     """
#     name = get_name_from_function()
#     sources = get_download_sources(name)

#     yahoo = []
#     for series in ["spy", "n100", "nikkei", "sse"]:
#         aux = pd.read_csv(
#             sources[series].format(timestamp=dt.datetime.now().timestamp().__round__()),
#             index_col=0,
#             usecols=[0, 4],
#             parse_dates=True,
#         )
#         aux.columns = [series]
#         yahoo.append(aux)
#     output = pd.concat(yahoo, axis=1).interpolate(method="linear", limit_area="inside")
#     output.columns = [
#         "S&P 500",
#         "Euronext 100",
#         "Nikkei 225",
#         "Shanghai Stock Exchange Composite",
#     ]
#     output.rename_axis(None, inplace=True)
#     metadata._set(
#         output,
#         area="Global",
#         currency="USD",
#         inf_adj="No",
#         seas_adj="NSA",
#         ts_type="-",
#         cumperiods=1,
#     )
#     metadata._modify_multiindex(
#         output, levels=[3], new_arrays=[["USD", "EUR", "JPY", "CNY"]]
#     )
#     output = rebase(output, start_date="2019-01-02")

#     return output


# def global_policy_rates() -> pd.DataFrame:
#     """Get central bank policy interest rates data.

#     Countries/aggregates selected are US, Euro Area, Japan and China.

#     Returns
#     -------
#     Daily policy interest rates : pd.DataFrame

#     """
#     name = get_name_from_function()
#     sources = get_download_sources(name)

#     r = httpx.get(sources["main"])
#     temp_dir = tempfile.TemporaryDirectory()
#     with zipfile.ZipFile(BytesIO(r.content), "r") as f:
#         f.extractall(path=temp_dir.name)
#         path_temp = path.join(temp_dir.name, "WS_CBPOL_csv_row.csv")
#         raw = pd.read_csv(path_temp, index_col=0)
#     output = raw.loc[:, lambda x: x.columns.str.contains("D:Daily")]
#     output.columns = output.iloc[0]
#     output = (
#         output[["JP:Japan", "CN:China", "US:United States", "XM:Euro area"]]
#         .iloc[8:]
#         .dropna(how="all")
#     )
#     output.columns = ["Japón", "China", "Estados Unidos", "Eurozona"]
#     output = output.apply(pd.to_numeric, errors="coerce")

#     output = output[["Estados Unidos", "Eurozona", "Japón", "China"]]
#     output.index = pd.to_datetime(output.index)
#     output = output.interpolate(method="linear", limit_direction="forward")
#     output.rename_axis(None, inplace=True)

#     metadata._set(
#         output,
#         area="Global",
#         currency="USD",
#         inf_adj="No",
#         seas_adj="NSA",
#         unit="Tasa",
#         ts_type="-",
#         cumperiods=1,
#     )
#     metadata._modify_multiindex(
#         output, levels=[3], new_arrays=[["USD", "EUR", "JPY", "CNY"]]
#     )

#     return output


# # @retry(
# #     retry_on_exceptions=(HTTPError, URLError),
# #     max_calls_total=12,
# #     retry_window_after_first_call_in_seconds=60,
# # )
# # def long_rates() -> pd.DataFrame:
# #     """Get 10-year government bonds interest rates.

# #     Countries/aggregates selected are US, Germany, France, Italy, Spain
# #     United Kingdom, Japan and China.

# #     Returns
# #     -------
# #     Daily 10-year government bonds interest rates : pd.DataFrame

# #     """
# #     name = "global_long_rates"

# #     bonds = []
# #     r = httpx.get(f"{urls[name]['dl']['fred']}DGS10&api_key=" f"{FRED_API_KEY}&file_type=json")
# #     us = pd.DataFrame.from_records(r.json()["observations"])
# #     us = us[["date", "value"]].set_index("date")
# #     us.index = pd.to_datetime(us.index)
# #     us.columns = ["United States"]
# #     bonds.append(us.apply(pd.to_numeric, errors="coerce").dropna())

# #     for country, sid in zip(
# #         ["Germany", "France", "Italy", "Spain", "United Kingdom", "Japan", "China"],
# #         ["23693", "23778", "23738", "23806", "23673", "23901", "29227"],
# #     ):
# #         end_date_dt = dt.datetime(2000, 1, 1)
# #         start_date_dt = dt.datetime(2000, 1, 1)
# #         aux = []
# #         while end_date_dt < dt.datetime.now():
# #             end_date_dt = start_date_dt + dt.timedelta(days=5000)
# #             params = {
# #                 "curr_id": sid,
# #                 "smlID": str(randint(1000000, 99999999)),
# #                 "header": f"{country} 10-Year Bond Yield Historical Data",
# #                 "st_date": start_date_dt.strftime("%m/%d/%Y"),
# #                 "end_date": end_date_dt.strftime("%m/%d/%Y"),
# #                 "interval_sec": "Daily",
# #                 "sort_col": "date",
# #                 "sort_ord": "DESC",
# #                 "action": "historical_data",
# #             }
# #             r = httpx.post(
# #                 urls["global_long_rates"]["dl"]["main"], headers=investing_headers, data=params
# #             )
# #             aux.append(pd.read_html(r.content, match="Price", index_col=0,
# #                                     parse_dates=True, flavor="html.parser")[0])
# #             start_date_dt = end_date_dt + dt.timedelta(days=1)
# #         aux = pd.concat(aux, axis=0)[["Price"]].sort_index()
# #         aux.columns = [country]
# #         bonds.append(aux)

# #     output = bonds[0].join(bonds[1:], how="left")
# #     output = output.interpolate(method="linear", limit_area="inside")
# #     output.columns = [
# #         "Estados Unidos",
# #         "Alemania",
# #         "Francia",
# #         "Italia",
# #         "España",
# #         "Reino Unido",
# #         "Japón",
# #         "China",
# #     ]
# #     output.rename_axis(None, inplace=True)

# #     metadata._set(
# #         output,
# #         area="Global",
# #         currency="USD",
# #         inf_adj="No",
# #         seas_adj="NSA",
# #         unit="Tasa",
# #         ts_type="-",
# #         cumperiods=1,
# #     )
# #     metadata._modify_multiindex(
# #         output, levels=[3], new_arrays=[["USD", "EUR", "EUR", "EUR", "EUR", "GBP", "JPY", "CNY"]]
# #     )

# #     return output


# def global_nxr() -> pd.DataFrame:
#     """Get currencies data.

#     Selected currencies are the US dollar index, USDEUR, USDJPY and USDCNY.

#     Returns
#     -------
#     Daily currencies : pd.DataFrame

#     """
#     name = get_name_from_function()
#     sources = get_download_sources(name)

#     output = []
#     for series in ["dollar", "eur", "jpy", "cny"]:
#         aux = pd.read_csv(
#             sources[series].format(timestamp=dt.datetime.now().timestamp().__round__()),
#             index_col=0,
#             usecols=[0, 4],
#             parse_dates=True,
#         )
#         aux.columns = [series]
#         if series == "dollar":
#             aux.dropna(inplace=True)
#         output.append(aux)
#     output = (
#         output[0].join(output[1:]).interpolate(method="linear", limit_area="inside")
#     )
#     output.columns = ["Índice Dólar", "Euro", "Yen", "Renminbi"]
#     output.rename_axis(None, inplace=True)

#     metadata._set(
#         output,
#         area="Global",
#         currency="USD",
#         inf_adj="No",
#         seas_adj="NSA",
#         ts_type="-",
#         cumperiods=1,
#     )
#     metadata._modify_multiindex(
#         output,
#         levels=[3, 5],
#         new_arrays=[
#             ["USD", "EUR", "JPY", "CNY"],
#             ["Canasta/USD", "EUR/USD", "JPY/USD", "CNY/USD"],
#         ],
#     )

#     return output
