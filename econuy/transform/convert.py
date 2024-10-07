from typing import Optional, Union, Literal
from datetime import datetime

import pandas as pd

from econuy.transform import rolling, resample
from econuy.utils import metadata
from econuy.utils.transform import error_handler


def _convert_usd(data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    error_handling: Literal["raise", "coerce", "ignore"] = "raise",
    ) -> pd.DataFrame:
    from econuy.retrieval.load import load_dataset

    indicators = metadata.indicator_ids
    metadata = metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata.indicator_metadata[indicators[0]]

    if single_metadata["currency"] != "UYU":
        output = error_handler(data, errors=error_handling, msg="Currency is not UYU")
        return output, metadata

    nxr = load_dataset("nxr_monthly")
    time_series_type = single_metadata["time_series_type"]
    inferred_freq = pd.infer_freq(data.index)
    target_freq = inferred_freq

    # For now we only support converting monthly or lower frequency data, so we resample first.
    if inferred_freq in ["D", "B", "C", "W", "W-SUN", None]:
        if time_series_type == "Flow":
            data = data.resample("ME").sum()
        else:
            data = data.resample("ME").last()
        target_freq = "ME"

    if time_series_type == "Stock":
        #nxr.metadata.update_dataset_metadata({"time_series_type": "Stock"})
        nxr_freq = nxr.resample(target_freq, operation="last").data.iloc[:, [1]]
    else:
        #nxr.metadata.update_dataset_metadata({"time_series_type": "Stock"})
        cum_periods = single_metadata["cumulative_periods"]
        nxr_freq = nxr.resample(target_freq, operation="mean").rolling(window=cum_periods, operation="mean").data.iloc[:, [0]]

    nxr_to_use = nxr_freq.reindex(data.index).iloc[:, 0]
    output = data.div(nxr_to_use, axis=0)
    metadata.update_dataset_metadata({"currency": "USD"})
    metadata.add_transformation_step({"convert": {"flavor": "usd"}})

    return output, metadata


def _convert_real(
    data: pd.DataFrame,
    metadata: "Metadata",  # type: ignore # noqa: F821
    start_date: Union[str, datetime, None] = None,
    end_date: Union[str, datetime, None] = None,
    error_handling: Literal["raise", "coerce", "ignore"] = "raise",
) -> pd.DataFrame:
    from econuy.retrieval.load import load_dataset

    indicators = metadata.indicator_ids
    metadata = metadata.copy()
    # We get the first one because we validated that all indicators have the same metadata, or pass them one by one
    single_metadata = metadata.indicator_metadata[indicators[0]]

    if single_metadata["currency"] != "UYU":
        output = error_handler(data, errors=error_handling, msg="Currency is not UYU")
        return output, metadata
    elif "Constant" in str(single_metadata["inflation_adjustment"]):
        output = error_handler(
            data, errors=error_handling, msg="Already inflation adjusted"
        )
        return output, metadata

    cpi = load_dataset("cpi")

    inferred_freq = pd.infer_freq(data.index)
    target_freq = inferred_freq
    if inferred_freq in ["D", "B", "C", "W", "W-SUN", None]:
        if single_metadata["time_series_type"] == "Flow":
            data = data.resample("ME").sum()
        else:
            data = data.resample("ME").mean()
        target_freq = pd.infer_freq(data.index)

    cum_periods = single_metadata["cumulative_periods"]
    cpi_to_use = cpi.resample(target_freq, operation="mean").rolling(cum_periods, operation="mean").data.iloc[:, 0]

    start_date = datetime.strptime(start_date, "%Y-%m-%d") if isinstance(start_date, str) else start_date
    end_date = datetime.strptime(end_date, "%Y-%m-%d") if isinstance(end_date, str) else end_date

    if start_date is None:
        converted_df = data.div(cpi_to_use, axis=0)
        col_text = "Constant prices"
    elif end_date is None:
        #start_date = pd.to_datetime(start_date)
        month = data.index.to_series().sub(pd.to_datetime(start_date)).abs().idxmin()
        converted_df = data.div(cpi_to_use, axis=0) * cpi_to_use.loc[month]
        m_start = start_date.strftime("%Y-%m")
        col_text = f"Constant prices {m_start}"
    else:
        converted_df = (
            data.div(cpi_to_use, axis=0) * cpi_to_use[start_date:end_date].mean()
        )
        m_start = start_date.strftime("%Y-%m")
        m_end = end_date.strftime("%Y-%m")
        if m_start == m_end:
            col_text = f"Constant prices {m_start}"
        else:
            col_text = f"Constant prices {m_start}_{m_end}"

    converted_df = converted_df.reindex(data.index)
    metadata.update_dataset_metadata({"inflation_adjustment": col_text})
    start_date_str = start_date.strftime("%Y-%m-%d") if start_date is not None else None
    end_date_str = end_date.strftime("%Y-%m-%d") if end_date is not None else None
    metadata.add_transformation_step({"convert": {"flavor": "real", "start_date": start_date_str, "end_date": end_date_str}})

    return converted_df, metadata


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
        for x, y in zip(
            df.columns.get_level_values("Área"), df.columns.get_level_values("Unidad")
        )
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
    if inferred_freq in ["M", "MS", "ME"]:
        gdp = resample(
            gdp, rule=inferred_freq, operation="upsample", interpolation="linear"
        )
        if cum != 12 and df.columns.get_level_values("Tipo")[0] == "Flujo":
            converter = int(12 / cum)
            df = rolling(df, window=converter, operation="sum")
    elif inferred_freq in ["Q", "QE-DEC", "QE-DEC"]:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()
        if cum != 4 and df.columns.get_level_values("Tipo")[0] == "Flujo":
            converter = int(4 / cum)
            df = rolling(df, window=converter, operation="sum")
    elif inferred_freq in ["A", "A-DEC", "YE-DEC"]:
        gdp = gdp.resample(inferred_freq, convention="end").asfreq()
    elif inferred_freq in ["D", "B", "C", "W", "W-SUN", None]:
        if df.columns.get_level_values("Tipo")[0] == "Flujo":
            df = df.resample("ME").sum()
        else:
            df = df.resample("ME").mean()
        gdp = resample(gdp, rule="ME", operation="upsample", interpolation="linear")
    else:
        raise ValueError(
            "Frequency of input dataframe not any of 'D', 'C', "
            "'W', 'B', 'M', 'MS', 'Q', 'QE-DEC', 'A' or 'A-DEC'."
        )

    if df.columns.get_level_values("Moneda")[0] == "USD":
        gdp = gdp.iloc[:, 1].to_frame()
    else:
        gdp = gdp.iloc[:, 0].to_frame()

    gdp_to_use = gdp.reindex(df.index).iloc[:, 0]
    converted_df = df.div(gdp_to_use, axis=0).multiply(100)

    metadata._set(converted_df, unit="% PBI")

    return converted_df
