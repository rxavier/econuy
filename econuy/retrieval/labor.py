import pandas as pd
from pandas.tseries.offsets import MonthEnd

from econuy.base import Dataset, DatasetMetadata
from econuy import load_dataset
from econuy.utils.operations import (
    get_name_from_function,
    get_download_sources,
    get_names_and_ids,
    get_base_metadata,
)


def labor_rates_gender() -> Dataset:
    """Get labor market data (LFPR, employment and unemployment).

    Returns
    -------
    Monthly participation, employment and unemployment rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=7).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")
    output.columns = [
        "Tasa de actividad: total",
        "Tasa de actividad: hombres",
        "Tasa de actividad: mujeres",
        "Tasa de empleo: total",
        "Tasa de empleo: hombres",
        "Tasa de empleo: mujeres",
        "Tasa de desempleo: total",
        "Tasa de desempleo: hombres",
        "Tasa de desempleo: mujeres",
    ]

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Labor market",
        "currency": None,
        "inflation_adjustment": None,
        "unit": "Rate",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def activity_region() -> Dataset:
    """Get activity rates by region (LFPR).

    Returns
    -------
    Monthly participation rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=8).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def employment_region() -> Dataset:
    """Get employment rates by region.

    Returns
    -------
    Monthly employment rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=8).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def unemployment_region() -> Dataset:
    """Get unemployment rates by region.

    Returns
    -------
    Monthly unemployment rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=8).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def employment_age() -> Dataset:
    """Get employment rates by age group.

    Returns
    -------
    Monthly employment rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=7).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def unemployment_contributions() -> Dataset:
    """Get contributions to unemployment rates.
    Returns
    -------
    Monthly unemployment rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=9).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def unemployment_characteristics() -> Dataset:
    """Get unemployment rates by characteristics.

    Returns
    -------
    Monthly unemployment rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=8).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def unemployment_conditions() -> Dataset:
    """Get unemployment rates by conditions.

    Returns
    -------
    Monthly unemployment rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(
        sources["main"], skiprows=9, na_values=[".."], usecols="A:I"
    ).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def unemployment_duration() -> Dataset:
    """Get average duration of unemployment in weeks.

    Returns
    -------
    Monthly average duration of unemployment : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=9, usecols="A,J").dropna(
        axis=0, thresh=2
    )
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def employment_characteristics() -> Dataset:
    """Get employment rates by characteristics.

    Returns
    -------
    Monthly employment rates : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=8).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2006-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def nominal_wages() -> Dataset:
    """Get nominal general, public and private sector wages data

    Returns
    -------
    Monthly wages separated by public and private sector : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    historical = pd.read_excel(sources["historical"], skiprows=8, usecols="A:B")
    current = pd.read_excel(sources["current"], skiprows=8, usecols="A,C:D")
    historical = historical.dropna(how="any").set_index("Unnamed: 0")
    current = current.dropna(how="any").set_index("Unnamed: 0")
    output = pd.concat([historical, current], axis=1)
    output.index = output.index + MonthEnd(1)
    output.columns = [
        "Índice medio de salarios",
        "Índice medio de salarios privados",
        "Índice medio de salarios públicos",
    ]
    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Labor market",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "2008-07=100",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def employment_sector() -> Dataset:
    """Get employment data by sector.

    Returns
    -------
    Monthly employment data by sector : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=7).dropna(axis=0, thresh=2)
    output = raw[~raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    output.index = pd.date_range(start="2011-01-31", periods=len(output), freq="ME")
    output = output.drop(columns="Unnamed: 0")

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids, spanish_names = get_names_and_ids(name, "es")
    output.columns = ids

    base_metadata = get_base_metadata(name)
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def hours_worked() -> Dataset:
    """Get average hours worked data.

    Returns
    -------
    Monthly hours worked : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"]).dropna(axis=0, thresh=2)

    output = raw[~raw.iloc[:, 0].str.contains("-|/|Total|Año", regex=True)].iloc[:, 1:]
    output.index = pd.date_range(start="2011-01-31", periods=len(output), freq="ME")
    output.columns = [
        "Total",
        "Industrias manufactureras",
        "Electricidad, gas, agua y saneamiento",
        "Construcción",
        "Comercio",
        "Transporte y almacenamiento",
        "Alojamiento y servicios de comidas",
        "Información y comunicación",
        "Actividades financieras",
        "Actividades inmobiliarias y administrativas",
        "Actividades profesionales",
        "Administración pública y seguridad social",
        "Enseñanza",
        "Salud",
        "Arte y otros servicios",
        "Act. de hogares como empleadores",
        "Agro, forestación, pesca y minería",
    ]

    output = output.rename_axis(None)
    output = output.apply(pd.to_numeric, errors="coerce")

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Labor market",
        "currency": None,
        "inflation_adjustment": None,
        "unit": "Hours per week",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def labor_rates_persons(*args, **kwargs) -> Dataset:
    """
    Get labor data, both rates and persons. Extends national data between 1991
    and 2005 with data for jurisdictions with more than 5,000 inhabitants.

    Returns
    -------
    Labor market data : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    rates = load_dataset("labor_rates", *args, **kwargs).to_named()
    rates = rates.loc[
        :,
        [
            "Tasa de actividad: total",
            "Tasa de empleo: total",
            "Tasa de desempleo: total",
        ],
    ]
    working_age = pd.read_excel(
        sources["population"], skiprows=7, index_col=0, nrows=92
    ).dropna(how="all")
    rates.columns = rates.columns.str.replace(": total", "")

    ages = list(range(14, 90)) + ["90 y más"]
    working_age = working_age.loc[ages].sum()
    working_age.index = pd.date_range(
        start="1996-06-30", end="2050-06-30", freq="YE-JUN"
    )
    monthly_working_age = working_age.resample("ME").interpolate("linear")
    monthly_working_age = monthly_working_age.reindex(rates.index)
    persons = rates.iloc[:, [0, 1]].div(100).mul(monthly_working_age, axis=0)
    persons["Desempleados"] = rates.iloc[:, 2].div(100).mul(persons.iloc[:, 0])
    persons.columns = ["Activos", "Empleados", "Desempleados"]

    output = pd.concat([rates, persons], axis=1)
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Labor market",
        "currency": None,
        "inflation_adjustment": None,
        "unit": "Rate",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, unit in zip(ids[-3:], ["Persons", "Persons", "Persons"]):
        metadata.update_indicator_metadata_value(indicator, "unit", unit)
    dataset = Dataset(name, output, metadata)

    return dataset


def real_wages(*args, **kwargs) -> Dataset:
    """
    Get real wages.

    Returns
    -------
    Real wages data : Dataset

    """
    name = get_name_from_function()

    nominal_wages = load_dataset("nominal_wages", *args, **kwargs)
    output = nominal_wages.convert("real").rebase("2008-07-31").to_named()
    output.columns = [
        "Índice medio de salarios reales",
        "Índice medio de salarios reales privados",
        "Índice medio de salarios reales públicos",
    ]

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Labor market",
        "currency": "UYU",
        "inflation_adjustment": "Const.",
        "unit": "2008-07=100",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset
