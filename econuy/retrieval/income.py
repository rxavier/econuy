import pandas as pd

from econuy.base import Dataset, DatasetMetadata
from econuy.utils.operations import get_name_from_function, get_download_sources


def income_household() -> Dataset:
    """Get average household income.

    Returns
    -------
    Monthly average household income : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = (
        pd.read_excel(sources["main"], skiprows=5)
        .dropna(thresh=5)
        .loc[lambda x: x["Mes, Trimestre y Año"].str.contains("/[0-9]{2}", regex=True)]
    )

    output = raw.set_index(
        pd.date_range(start="2006-03-31", freq="QE-DEC", periods=len(raw))
    ).iloc[:, 1:]

    spanish_names = [
        "Total país",
        "Montevideo",
        "Interior: total",
        "Interior: localidades de más de 5 mil hab.",
        "Interior: localidades pequeñas y rural",
    ]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]
    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    base_metadata = {
        "area": "Income and expenditure",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "Pesos",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def income_capita() -> Dataset:
    """Get average per capita income.

    Returns
    -------
    Monthly average per capita income : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = (
        pd.read_excel(sources["main"], skiprows=5)
        .dropna(thresh=5)
        .loc[lambda x: x["Mes, Trimestre y Año "].str.contains("/[0-9]{2}", regex=True)]
    )

    output = raw.set_index(
        pd.date_range(start="2006-03-31", freq="QE-DEC", periods=len(raw))
    ).iloc[:, 1:]

    output.columns = [
        "Total país",
        "Montevideo",
        "Interior: total",
        "Interior: localidades de más de 5 mil hab.",
        "Interior: localidades pequeñas y rural",
    ]

    output = output.apply(pd.to_numeric, errors="coerce")
    output.rename_axis(None, inplace=True)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Income and expenditure",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "Pesos",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset
