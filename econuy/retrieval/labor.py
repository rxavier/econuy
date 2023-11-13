import tempfile
from pathlib import Path
from typing import Optional
from urllib.error import URLError, HTTPError

import pandas as pd
import requests
import patoolib
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd

from econuy import transform
from econuy.core import Pipeline
from econuy.utils import metadata
from econuy.utils.sources import urls


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def labor_rates() -> pd.DataFrame:
    """Get labor market data (LFPR, employment and unemployment).

    Returns
    -------
    Monthly participation, employment and unemployment rates : pd.DataFrame

    """
    name = "labor_rates"

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls[name]["dl"]["main"])
        f.write(r.content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        excel_path = Path(temp_dir) / "ECH0103.xlsx"
        labor_raw = pd.read_excel(excel_path, skiprows=39).dropna(axis=0, thresh=2)
    labor = labor_raw[~labor_raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    labor.index = pd.date_range(start="2006-01-31", periods=len(labor), freq="M")
    labor = labor.drop(columns="Unnamed: 0")
    labor.columns = [
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

    labor = labor.apply(pd.to_numeric, errors="coerce")
    labor.rename_axis(None, inplace=True)

    metadata._set(
        labor,
        area="Mercado laboral",
        currency="-",
        inf_adj="No",
        unit="Tasa",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return labor


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def nominal_wages() -> pd.DataFrame:
    """Get nominal general, public and private sector wages data

    Returns
    -------
    Monthly wages separated by public and private sector : pd.DataFrame

    """
    name = "nominal_wages"

    historical = pd.read_excel(urls[name]["dl"]["historical"], skiprows=8, usecols="A:B")
    current = pd.read_excel(urls[name]["dl"]["current"], skiprows=8, usecols="A,C:D")
    historical = historical.dropna(how="any").set_index("Unnamed: 0")
    current = current.dropna(how="any").set_index("Unnamed: 0")
    wages = pd.concat([historical, current], axis=1)
    wages.index = wages.index + MonthEnd(1)
    wages.columns = [
        "Índice medio de salarios",
        "Índice medio de salarios privados",
        "Índice medio de salarios públicos",
    ]
    wages = wages.apply(pd.to_numeric, errors="coerce")
    wages.rename_axis(None, inplace=True)

    metadata._set(
        wages,
        area="Mercado laboral",
        currency="UYU",
        inf_adj="No",
        unit="2008-07=100",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    return wages


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def hours() -> pd.DataFrame:
    """Get average hours worked data.

    Returns
    -------
    Monthly hours worked : pd.DataFrame

    """
    name = "hours_worked"

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = requests.get(urls[name]["dl"]["main"])
        f.write(r.content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        excel_path = Path(temp_dir) / "ECH0703.xlsx"
        raw = pd.read_excel(excel_path).dropna(axis=0, thresh=2)

    output = raw[~raw.iloc[:, 0].str.contains("-|/|Total|Año", regex=True)].iloc[:, 1:]
    output.index = pd.date_range(start="2011-01-31", periods=len(output), freq="M")
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

    output.rename_axis(None, inplace=True)

    metadata._set(
        output,
        area="Mercado laboral",
        currency="-",
        inf_adj="No",
        unit="Horas por semana",
        seas_adj="NSA",
        ts_type="Stock",
        cumperiods=1,
    )

    return output


@retry(
    retry_on_exceptions=(HTTPError, URLError),
    max_calls_total=4,
    retry_window_after_first_call_in_seconds=60,
)
def rates_people(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """
    Get labor data, both rates and persons. Extends national data between 1991
    and 2005 with data for jurisdictions with more than 5,000 inhabitants.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Labor market data : pd.DataFrame

    """
    name = "labor_rates_people"
    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get("labor_rates")
    rates = pipeline.dataset
    rates = rates.loc[
        :, ["Tasa de actividad: total", "Tasa de empleo: total", "Tasa de desempleo: total"]
    ]
    working_age = pd.read_excel(
        urls[name]["dl"]["population"], skiprows=7, index_col=0, nrows=92
    ).dropna(how="all")
    rates.columns = rates.columns.set_levels(
        rates.columns.levels[0].str.replace(": total", ""), level=0
    )

    ages = list(range(14, 90)) + ["90 y más"]
    working_age = working_age.loc[ages].sum()
    working_age.index = pd.date_range(start="1996-06-30", end="2050-06-30", freq="A-JUN")
    monthly_working_age = working_age.resample("M").interpolate("linear")
    monthly_working_age = monthly_working_age.reindex(rates.index)
    persons = rates.iloc[:, [0, 1]].div(100).mul(monthly_working_age, axis=0)
    persons["Desempleados"] = rates.iloc[:, 2].div(100).mul(persons.iloc[:, 0])
    persons.columns = ["Activos", "Empleados", "Desempleados"]

    metadata._set(
        persons,
        area="Mercado laboral",
        currency="-",
        inf_adj="No",
        unit="Personas",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )

    output = pd.concat([rates, persons], axis=1)
    output.rename_axis(None, inplace=True)

    return output


def real_wages(pipeline: Optional[Pipeline] = None) -> pd.DataFrame:
    """
    Get real wages.

    Parameters
    ----------
    pipeline : econuy.core.Pipeline or None, default None
        An instance of the econuy Pipeline class.

    Returns
    -------
    Real wages data : pd.DataFrame

    """
    if pipeline is None:
        pipeline = Pipeline()

    pipeline.get("nominal_wages")
    wages = pipeline.dataset
    wages.columns = [
        "Índice medio de salarios reales",
        "Índice medio de salarios reales privados",
        "Índice medio de salarios reales públicos",
    ]
    metadata._set(
        wages,
        area="Mercado laboral",
        currency="UYU",
        inf_adj="Sí",
        seas_adj="NSA",
        ts_type="-",
        cumperiods=1,
    )
    output = transform.convert_real(wages, pipeline=pipeline)
    output = transform.rebase(output, start_date="2008-07-31")
    output.rename_axis(None, inplace=True)

    return output
