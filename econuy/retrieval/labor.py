from pathlib import Path
from io import BytesIO
from typing import Optional
from urllib.error import URLError, HTTPError

import pandas as pd
import requests
from opnieuw import retry
from pandas.tseries.offsets import MonthEnd

from econuy import transform
from econuy.core import Pipeline
from econuy.utils import metadata, get_project_root
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

    try:
        labor_raw = pd.read_excel(urls[name]["dl"]["main"], skiprows=39).dropna(axis=0, thresh=2)
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files", "ine_certs.pem")
            r = requests.get(urls[name]["dl"]["main"], verify=certificate)
            labor_raw = pd.read_excel(BytesIO(r.content), skiprows=39).dropna(axis=0, thresh=2)
        else:
            raise err
    labor = labor_raw[~labor_raw["Unnamed: 0"].str.contains("-|/|Total", regex=True)]
    labor.index = pd.date_range(start="2006-01-01", periods=len(labor), freq="M")
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
    missing = pd.read_excel(urls[name]["dl"]["missing"], index_col=0, header=0).iloc[:, :9]
    missing.columns = labor.columns
    labor = labor.append(missing)
    labor = labor.loc[~labor.index.duplicated(keep="first")]
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

    try:
        historical = pd.read_excel(urls[name]["dl"]["historical"], skiprows=8, usecols="A:B")
        current = pd.read_excel(urls[name]["dl"]["current"], skiprows=8, usecols="A,C:D")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files", "ine_certs.pem")
            r = requests.get(urls[name]["dl"]["historical"], verify=certificate)
            historical = pd.read_excel(BytesIO(r.content), skiprows=8, usecols="A:B")
            r = requests.get(urls[name]["dl"]["current"], verify=certificate)
            current = pd.read_excel(BytesIO(r.content), skiprows=8, usecols="A,C:D")
        else:
            raise err

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

    try:
        raw = pd.read_excel(
            urls[name]["dl"]["main"], sheet_name="Mensual", skiprows=5, index_col=0
        ).dropna(how="all")
        prev_hours = (
            pd.read_excel(urls[name]["dl"]["historical"], index_col=0, skiprows=8)
            .dropna(how="all")
            .iloc[:, [0]]
        )
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files", "ine_certs.pem")
            r = requests.get(urls[name]["dl"]["main"], verify=certificate)
            raw = pd.read_excel(
                BytesIO(r.content), sheet_name="Mensual", skiprows=5, index_col=0
            ).dropna(how="all")
            r = requests.get(urls[name]["dl"]["historical"], verify=certificate)
            prev_hours = (
                pd.read_excel(BytesIO(r.content), index_col=0, skiprows=8)
                .dropna(how="all")
                .iloc[:, [0]]
            )
        else:
            raise err
    raw.index = pd.to_datetime(raw.index)
    output = raw.loc[~pd.isna(raw.index)]
    output.index = output.index + MonthEnd(0)
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

    prev_hours = prev_hours.loc[~prev_hours.index.str.contains("-|Total")]
    prev_hours.index = pd.date_range(start="2006-01-31", freq="M", periods=len(prev_hours))
    prev_hours = prev_hours.loc[prev_hours.index < "2011-01-01"]
    prev_hours.columns = ["Total"]
    output = prev_hours.append(output, sort=False)
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
    try:
        act_5000 = pd.read_excel(
            urls[name]["dl"]["act_5000"],
            sheet_name="Mensual",
            index_col=0,
            skiprows=8,
            usecols="A:B",
        ).dropna(how="any")
        emp_5000 = pd.read_excel(
            urls[name]["dl"]["emp_5000"],
            sheet_name="Mensual",
            index_col=0,
            skiprows=8,
            usecols="A:B",
        ).dropna(how="any")
        des_5000 = pd.read_excel(
            urls[name]["dl"]["des_5000"],
            sheet_name="Mensual",
            index_col=0,
            skiprows=7,
            usecols="A:B",
        ).dropna(how="any")
        working_age = pd.read_excel(
            urls[name]["dl"]["population"], skiprows=7, index_col=0, nrows=92
        ).dropna(how="all")
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            certificate = Path(get_project_root(), "utils", "files", "ine_certs.pem")
            r = requests.get(urls[name]["dl"]["act_5000"], verify=certificate)
            act_5000 = pd.read_excel(
                BytesIO(r.content), sheet_name="Mensual", index_col=0, skiprows=8, usecols="A:B"
            ).dropna(how="any")
            r = requests.get(urls[name]["dl"]["emp_5000"], verify=certificate)
            emp_5000 = pd.read_excel(
                BytesIO(r.content), sheet_name="Mensual", index_col=0, skiprows=8, usecols="A:B"
            ).dropna(how="any")
            r = requests.get(urls[name]["dl"]["des_5000"], verify=certificate)
            des_5000 = pd.read_excel(
                BytesIO(r.content), sheet_name="Mensual", index_col=0, skiprows=7, usecols="A:B"
            ).dropna(how="any")
            r = requests.get(urls[name]["dl"]["population"], verify=certificate)
            working_age = pd.read_excel(
                BytesIO(r.content), skiprows=7, index_col=0, nrows=92
            ).dropna(how="all")
        else:
            raise err
    for df in [act_5000, emp_5000, des_5000]:
        df.index = df.index + MonthEnd(0)
    rates_5000 = pd.concat([act_5000, emp_5000, des_5000], axis=1)
    rates_prev = rates_5000.loc[rates_5000.index < "2006-01-31"]
    rates_prev.columns = rates.columns
    rates = pd.concat([rates_prev, rates])
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
