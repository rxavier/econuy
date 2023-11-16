from typing import Optional, Union
from datetime import datetime

import pandas as pd

from econuy.transform import rolling, resample
from econuy.utils import metadata
from econuy.utils.transform import error_handler


def convert_usd(df: pd.DataFrame, pipeline=None, errors: str = "raise") -> pd.DataFrame:
    """Convert to other units.

    See Also
    --------
    :mod:`~econuy.core.Pipeline.convert`.

    """
    if errors not in ["raise", "coerce", "ignore"]:
        raise ValueError("'errors' must be one of 'raise', " "'coerce' or 'ignore'.")
    if "Moneda" not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the " "'Moneda' level.")

    if pipeline is None:
        from econuy.core import Pipeline

        pipeline = Pipeline()

    checks = [x == "UYU" for x in df.columns.get_level_values("Moneda")]
    if any(checks):
        if not all(checks) and errors == "raise":
            error_df = df.loc[:, [not check for check in checks]]
            msg = f"{error_df.columns[0][0]} does not have the " f"appropiate metadata."
            return error_handler(df=df, errors=errors, msg=msg)
        pipeline.get(name="nxr_monthly")
        nxr_data = pipeline.dataset
        all_metadata = df.columns.droplevel("Indicador")
        if all(x == all_metadata[0] for x in all_metadata):
            return _convert_usd(df=df, nxr=nxr_data)
        else:
            columns = []
            for column_name, check in zip(df.columns, checks):
                df_column = df[[column_name]]
                if check is False:
                    msg = f"{column_name[0]} does not have the " f"appropiate metadata."
                    columns.append(error_handler(df=df_column, errors=errors, msg=msg))
                else:
                    converted = _convert_usd(df=df_column, nxr=nxr_data)
                    columns.append(converted)
            return pd.concat(columns, axis=1)
    else:
        return error_handler(df=df, errors=errors)


def _convert_usd(df: pd.DataFrame, nxr: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if nxr is None:
        from econuy.core import Pipeline

        pipeline = Pipeline()
        pipeline.get("nxr_monthly")
        nxr = pipeline.dataset

    inferred_freq = pd.infer_freq(df.index)
    if inferred_freq in ["D", "B", "C", "W", "W-SUN", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").last()
        inferred_freq = pd.infer_freq(df.index)

    if df.columns.get_level_values("Tipo")[0] == "Stock":
        metadata._set(nxr, ts_type="Stock")
        nxr_freq = resample(nxr, rule=inferred_freq, operation="last").iloc[:, [1]]
    else:
        metadata._set(nxr, ts_type="Flujo")
        nxr_freq = resample(nxr, rule=inferred_freq, operation="mean").iloc[:, [0]]
        cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
        nxr_freq = rolling(nxr_freq, window=cum_periods, operation="mean")

    nxr_to_use = nxr_freq.reindex(df.index).iloc[:, 0]
    converted_df = df.div(nxr_to_use, axis=0)
    metadata._set(converted_df, currency="USD")

    return converted_df


def convert_real(
    df: pd.DataFrame,
    start_date: Union[str, datetime, None] = None,
    end_date: Union[str, datetime, None] = None,
    pipeline=None,
    errors: str = "raise",
) -> pd.DataFrame:
    """Convert to other units.

    See Also
    --------
    :mod:`~econuy.core.Pipeline.convert`.

    """
    if errors not in ["raise", "coerce", "ignore"]:
        raise ValueError("'errors' must be one of 'raise', " "'coerce' or 'ignore'.")
    if "Inf. adj." not in df.columns.names:
        raise ValueError("Input dataframe's multiindex requires the " "'Inf. adj.' level.")

    if pipeline is None:
        from econuy.core import Pipeline

        pipeline = Pipeline()

    checks = [
        x == "UYU" and "Const." not in y
        for x, y in zip(
            df.columns.get_level_values("Moneda"), df.columns.get_level_values("Inf. adj.")
        )
    ]
    if any(checks):
        if not all(checks) and errors == "raise":
            error_df = df.loc[:, [not check for check in checks]]
            msg = f"{error_df.columns[0][0]} does not have the " f"appropiate metadata."
            return error_handler(df=df, errors=errors, msg=msg)
        pipeline.get(name="cpi")
        cpi_data = pipeline.dataset
        all_metadata = df.columns.droplevel("Indicador")
        if all(x == all_metadata[0] for x in all_metadata):
            return _convert_real(df=df, start_date=start_date, end_date=end_date, cpi=cpi_data)
        else:
            columns = []
            for column_name, check in zip(df.columns, checks):
                df_column = df[[column_name]]
                if check is False:
                    msg = f"{column_name[0]} does not have the " f"appropiate metadata."
                    columns.append(error_handler(df=df_column, errors=errors, msg=msg))
                else:
                    converted = _convert_real(
                        df=df_column, start_date=start_date, end_date=end_date, cpi=cpi_data
                    )
                    columns.append(converted)
            return pd.concat(columns, axis=1)
    else:
        return error_handler(df=df, errors=errors)


def _convert_real(
    df: pd.DataFrame,
    start_date: Union[str, datetime, None] = None,
    end_date: Union[str, datetime, None] = None,
    cpi: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if cpi is None:
        from econuy.core import Pipeline

        pipeline = Pipeline()
        pipeline.get("cpi")
        cpi = pipeline.dataset

    inferred_freq = pd.infer_freq(df.index)
    if inferred_freq in ["D", "B", "C", "W", "W-SUN", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").mean()
        inferred_freq = pd.infer_freq(df.index)

    metadata._set(cpi, ts_type="Flujo")
    cpi_freq = resample(cpi, rule=inferred_freq, operation="mean").iloc[:, [0]]
    cum_periods = int(df.columns.get_level_values("Acum. períodos")[0])
    cpi_to_use = rolling(cpi_freq, window=cum_periods, operation="mean").squeeze()

    if start_date is None:
        converted_df = df.div(cpi_to_use, axis=0)
        col_text = "Const."
    elif end_date is None:
        start_date = pd.to_datetime(start_date)
        month = df.index.to_series().sub(start_date).abs().idxmin()
        # month = df.iloc[df.index.get_loc(start_date, method="nearest")].name
        converted_df = df.div(cpi_to_use, axis=0) * cpi_to_use.loc[month]
        m_start = start_date.strftime("%Y-%m")
        col_text = f"Const. {m_start}"
    else:
        converted_df = df.div(cpi_to_use, axis=0) * cpi_to_use[start_date:end_date].mean()
        m_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
        m_end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m")
        if m_start == m_end:
            col_text = f"Const. {m_start}"
        else:
            col_text = f"Const. {m_start}_{m_end}"

    converted_df = converted_df.reindex(df.index)
    metadata._set(converted_df, inf_adj=col_text)

    return converted_df


def convert_gdp(df: pd.DataFrame, pipeline=None, errors: str = "raise") -> pd.DataFrame:
    """Convert to other units.

    See Also
    --------
    :mod:`~econuy.core.Pipeline.convert`.

    """
    if errors not in ["raise", "coerce", "ignore"]:
        raise ValueError("'errors' must be one of 'raise', 'coerce' or " "'ignore'.")
    if any(x not in df.columns.names for x in ["Área", "Unidad"]):
        raise ValueError(
            "Input dataframe's multiindex requires the 'Área' " "and 'Unidad' levels."
        )

    if pipeline is None:
        from econuy.core import Pipeline

        pipeline = Pipeline()

    checks = [
        x not in ["Regional", "Global"] and "%PBI" not in y
        for x, y in zip(df.columns.get_level_values("Área"), df.columns.get_level_values("Unidad"))
    ]
    if any(checks):
        if not all(checks) and errors == "raise":
            error_df = df.loc[:, [not check for check in checks]]
            msg = f"{error_df.columns[0][0]} does not have the " f"appropiate metadata."
            return error_handler(df=df, errors=errors, msg=msg)
        pipeline.get(name="_monthly_interpolated_gdp")
        gdp_data = pipeline.dataset
        all_metadata = df.columns.droplevel("Indicador")
        if all(x == all_metadata[0] for x in all_metadata):
            return _convert_gdp(df=df, gdp=gdp_data)
        else:
            columns = []
            for column_name, check in zip(df.columns, checks):
                df_column = df[[column_name]]
                if check is False:
                    msg = f"{column_name[0]} does not have the " f"appropiate metadata."
                    columns.append(error_handler(df=df_column, errors=errors, msg=msg))
                else:
                    converted = _convert_gdp(df=df_column, gdp=gdp_data)
                    columns.append(converted)
            return pd.concat(columns, axis=1)
    else:
        return error_handler(df=df, errors=errors)


def _convert_gdp(df: pd.DataFrame, gdp: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if gdp is None:
        from econuy.core import Pipeline

        pipeline = Pipeline()
        pipeline.get("_monthly_interpolated_gdp")
        gdp = pipeline.dataset

    inferred_freq = pd.infer_freq(df.index)
    cum = int(df.columns.get_level_values("Acum. períodos")[0])
    if inferred_freq in ["M", "MS"]:
        gdp = resample(gdp, rule=inferred_freq, operation="upsample", interpolation="linear")
        if cum != 12 and df.columns.get_level_values("Tipo")[0] == "Flujo":
            converter = int(12 / cum)
            df = rolling(df, window=converter, operation="sum")
    elif inferred_freq in ["Q", "Q-DEC"]:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()
        if cum != 4 and df.columns.get_level_values("Tipo")[0] == "Flujo":
            converter = int(4 / cum)
            df = rolling(df, window=converter, operation="sum")
    elif inferred_freq in ["A", "A-DEC"]:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()
    elif inferred_freq in ["D", "B", "C", "W", "W-SUN", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("M").sum()
        else:
            df = df.resample("M").mean()
        gdp = resample(gdp, rule="M", operation="upsample", interpolation="linear")
    else:
        raise ValueError(
            "Frequency of input dataframe not any of 'D', 'C', "
            "'W', 'B', 'M', 'MS', 'Q', 'Q-DEC', 'A' or 'A-DEC'."
        )

    if df.columns.get_level_values("Moneda")[0] == "USD":
        gdp = gdp.iloc[:, 1].to_frame()
    else:
        gdp = gdp.iloc[:, 0].to_frame()

    gdp_to_use = gdp.reindex(df.index).iloc[:, 0]
    converted_df = df.div(gdp_to_use, axis=0).multiply(100)

    metadata._set(converted_df, unit="% PBI")

    return converted_df
