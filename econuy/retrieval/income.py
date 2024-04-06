from urllib.error import URLError, HTTPError

import pandas as pd

from opnieuw import retry

from econuy.utils import metadata
from econuy.utils.operations import get_name_from_function, get_download_sources


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def income_household() -> pd.DataFrame:
    """Get average household income.

    Returns
    -------
    Monthly average household income : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)


    raw = (
        pd.read_excel(sources["main"], skiprows=5)
        .dropna(thresh=5)
        .loc[lambda x: x["Mes, Trimestre y Año"].str.contains("/[0-9]{2}", regex=True)]
    )

    output = raw.set_index(pd.date_range(start="2006-03-31", freq="QE-DEC", periods=len(raw))).iloc[
        :, 1:
    ]

    output.columns = [
        "Total país",
        "Montevideo",
        "Interior: total",
        "Interior: localidades de más de 5 mil hab.",
        "Interior: localidades pequeñas y rural",
    ]

    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Ingresos",
        currency="UYU",
        inf_adj="No",
        unit="Pesos",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def income_capita() -> pd.DataFrame:
    """Get average per capita income.

    Returns
    -------
    Monthly average per capita income : pd.DataFrame

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = (
        pd.read_excel(sources["main"], skiprows=5)
        .dropna(thresh=5)
        .loc[lambda x: x["Mes, Trimestre y Año "].str.contains("/[0-9]{2}", regex=True)]
    )

    output = raw.set_index(pd.date_range(start="2006-03-31", freq="QE-DEC", periods=len(raw))).iloc[
        :, 1:
    ]

    output.columns = [
        "Total país",
        "Montevideo",
        "Interior: total",
        "Interior: localidades de más de 5 mil hab.",
        "Interior: localidades pequeñas y rural",
    ]

    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Ingresos",
        currency="UYU",
        inf_adj="No",
        unit="Pesos",
        seas_adj="NSA",
        ts_type="Flujo",
        cumperiods=1,
    )

    return output
