import datetime as dt
import re
import tempfile
import time
from pathlib import Path
from os import listdir, path

import pandas as pd
import numpy as np
import patoolib
import httpx
from pandas.tseries.offsets import MonthEnd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from econuy.utils.operations import get_download_sources, get_name_from_function
from econuy.utils.chromedriver import _build
from econuy.base import Dataset, DatasetMetadata
from econuy import load_dataset
from econuy.utils.retrieval import get_with_ssl_context


def monthly_gdp() -> Dataset:
    """Get the monthly indicator for economic activity.

    Returns
    -------
    Monthly GDP : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    with tempfile.TemporaryDirectory() as tmp_dir:
        driver = _build(tmp_dir)
        driver.get(sources["main"])
        button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "button.dt-button.buttons-excel.buttons-html5")
            )
        )
        driver.execute_script("arguments[0].click()", button)
        time.sleep(3)
        driver.quit()
        filepath = list(Path(tmp_dir).glob("*.xlsx"))[0]
        output = pd.read_excel(filepath, index_col=0)

    output.index = pd.date_range(start="2016-01-31", freq="ME", periods=len(output))
    output = output.rename_axis(None)

    spanish_names = [
        "Indicador mensual de actividad económica",
        "Indicador mensual de actividad económica (desestacionalizado)",
        "Indicador mensual de actividad económica (tendencia-ciclo)",
    ]
    spanish_names = [{"es": x} for x in spanish_names]

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": "Constant prices 2016",
        "unit": "2016=100",
        "seasonal_adjustment": "",
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }

    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    for indicator, adjustment in zip(ids, [None, "Seasonally adjusted", "Trend-cycle"]):
        metadata = metadata.update_indicator_metadata_value(
            indicator, "seasonal_adjustment", adjustment
        )
    dataset = Dataset(name, output, metadata)

    return dataset


def national_accounts_supply_constant_nsa() -> Dataset:
    """Get supply-side national accounts data in NSA constant prices, 2005-.

    Returns
    -------
    National accounts, supply side, constant prices, NSA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    r_bytes = get_with_ssl_context("bcu", sources["main"])
    raw = pd.read_excel(r_bytes, skiprows=7)
    output = raw.dropna(how="all", axis=1).iloc[:, 2:].dropna(how="all").T
    output.index = pd.date_range(start="2016-03-31", freq="QE-DEC", periods=len(output))
    output = output.apply(pd.to_numeric, errors="coerce").rename_axis(None)

    spanish_names = [
        "Agropecuario, pesca y minería",
        "Industrias manufactureras",
        "Energía eléctrica, gas y agua",
        "Construcción",
        "Comercio, alojamiento y suministro de comidas y bebidas",
        "Transporte y almacenamiento, información y comunicaciones",
        "Servicios financieros",
        "Actividades profesionales y arrendamiento",
        "Actividades de administración pública",
        "Salud, educación, act. inmobiliarias y otros servicios",
        "Valor agregado a precios básicos",
        "Impuestos menos subvenciones",
        "Producto bruto interno",
    ]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": "Constant prices 2016",
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def national_accounts_demand_constant_nsa() -> Dataset:
    """Get demand-side national accounts data in NSA constant prices, 2005-.

    Returns
    -------
    National accounts, demand side, constant prices, NSA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    r_bytes = get_with_ssl_context("bcu", sources["main"])
    raw = pd.read_excel(r_bytes, skiprows=7)
    output = raw.dropna(how="all", axis=1).iloc[:, 2:].dropna(how="all").T
    output.index = pd.date_range(start="2016-03-31", freq="QE-DEC", periods=len(output))
    output = output.apply(pd.to_numeric, errors="coerce").rename_axis(None)
    output[7] = output[7] * -1

    spanish_names = [
        "Gasto de consumo: total",
        "Gasto de consumo: hogares",
        "Gasto de consumo: gobierno y ISFLH",
        "Formación bruta de capital",
        "Formación bruta de capital: fijo",
        "Variación de existencias",
        "Exportaciones de bienes y servicios",
        "Importaciones de bienes y servicios",
        "Producto bruto interno",
    ]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": "Constant prices 2016",
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def national_accounts_demand_current_nsa() -> Dataset:
    """Get demand-side national accounts data in NSA current prices.

    Returns
    -------
    National accounts, demand side, current prices, NSA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    r_bytes = get_with_ssl_context("bcu", sources["main"])
    raw = pd.read_excel(r_bytes, skiprows=7)
    output = raw.dropna(how="all", axis=1).iloc[:, 2:].dropna(how="all").T
    output.index = pd.date_range(start="2016-03-31", freq="QE-DEC", periods=len(output))
    output = output.apply(pd.to_numeric, errors="coerce").rename_axis(None)
    output[7] = output[7] * -1

    spanish_names = [
        "Gasto de consumo: total",
        "Gasto de consumo: hogares",
        "Gasto de consumo: gobierno y ISFLH",
        "Formación bruta de capital",
        "Formación bruta de capital: fijo",
        "Variación de existencias",
        "Exportaciones de bienes y servicios",
        "Importaciones de bienes y servicios",
        "Producto bruto interno",
    ]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def national_accounts_supply_current_nsa() -> Dataset:
    """Get supply-side national accounts data in NSA current prices, 2005-.

    Returns
    -------
    National accounts, supply side, current prices, NSA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    r_bytes = get_with_ssl_context("bcu", sources["main"])
    raw = pd.read_excel(r_bytes, skiprows=7)
    output = raw.dropna(how="all", axis=1).iloc[:, 2:].dropna(how="all").T
    output.index = pd.date_range(start="2016-03-31", freq="QE-DEC", periods=len(output))
    output = output.apply(pd.to_numeric, errors="coerce").rename_axis(None)

    spanish_names = [
        "Agropecuario, pesca y minería",
        "Industrias manufactureras",
        "Energía eléctrica, gas y agua",
        "Construcción",
        "Comercio, alojamiento y suministro de comidas y bebidas",
        "Transporte y almacenamiento, información y comunicaciones",
        "Servicios financieros",
        "Actividades profesionales y arrendamiento",
        "Actividades de administración pública",
        "Salud, educación, act. inmobiliarias y otros servicios",
        "Valor agregado a precios básicos",
        "Impuestos menos subvenciones",
        "Producto bruto interno",
    ]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def gdp_index_constant_sa() -> Dataset:
    """Get supply-side national accounts data in SA real index, 1997-.

    Returns
    -------
    National accounts, supply side, real index, SA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    r_bytes = get_with_ssl_context("bcu", sources["main"])
    raw = pd.read_excel(r_bytes, skiprows=7)
    output = raw.dropna(how="all", axis=1).iloc[:, 2:].dropna(how="all").T
    output.index = pd.date_range(start="2016-03-31", freq="QE-DEC", periods=len(output))
    output = output.apply(pd.to_numeric, errors="coerce").rename_axis(None)

    spanish_names = ["Producto bruto interno"]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": "Constant prices 2016",
        "unit": "2016=100",
        "seasonal_adjustment": "Seasonally adjusted",
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def national_accounts_supply_constant_nsa_extended(
    *args,
    **kwargs,
) -> Dataset:
    """Get supply-side national accounts data in NSA constant prices, 1988-.

    Three datasets with different base years, 1983, 2005 and 2016, are spliced
    in order to get to the result DataFrame.

    Returns
    -------
    National accounts, supply side, constant prices, NSA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    data_16 = load_dataset(
        "national_accounts_supply_constant_nsa", *args, **kwargs
    ).to_detailed()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16["Otros servicios"] = (
        data_16["Servicios financieros"]
        + data_16["Actividades profesionales y arrendamiento"]
        + data_16["Actividades de administración pública"]
        + data_16["Salud, educación, act. inmobiliarias y otros servicios"]
    )
    data_16 = data_16.drop(
        [
            "Valor agregado a precios básicos",
            "Servicios financieros",
            "Actividades profesionales y arrendamiento",
            "Actividades de administración pública",
            "Salud, educación, act. inmobiliarias y otros servicios",
            "Valor agregado a precios básicos",
        ],
        axis=1,
    )

    names_05 = [
        "Actividades primarias",
        "Act. prim.: Agricultura, ganadería, caza y silvicultura",
        "Industrias manufactureras",
        "Suministro de electricidad, gas y agua",
        "Construcción",
        "Comercio, reparaciones, restaurantes y hoteles",
        "Transporte, almacenamiento y comunicaciones",
        "Otras actividades",
        "Otras actividades: SIFMI",
        "Impuestos menos subvenciones",
        "Producto bruto interno",
    ]
    r_bytes = get_with_ssl_context("bcu", sources["2005"])
    raw_05 = pd.read_excel(r_bytes, skiprows=9)
    data_05 = (
        raw_05.dropna(how="all", axis=1)
        .iloc[:, 1:]
        .set_index("Unnamed: 1")
        .dropna(how="all")
        .T
    )
    data_05.index = pd.date_range(
        start="1997-03-31", freq="QE-DEC", periods=len(data_05)
    )
    data_05 = data_05.apply(pd.to_numeric, errors="coerce").rename_axis(None)
    data_05.columns = names_05

    # In the following lines I rename and reorder the Other activities
    # column, and explicitly drop SIFMI because I have no way of adding them.
    # In order for this to be correct, I would have to substract the SIFMI
    # part from each of the other sectors, which I cannot do. Implicitly I'm
    # assuming that the share of SIFMI in each sector over time doesn't change,
    # which shouldn't affect growth rates.
    data_05["Otros servicios"] = data_05["Otras actividades"]
    data_05 = data_05.drop(
        [
            "Act. prim.: Agricultura, ganadería, caza y silvicultura",
            "Otras actividades",
            "Otras actividades: SIFMI",
        ],
        axis=1,
    )
    data_05.columns = data_16.columns
    aux_index = list(dict.fromkeys(list(data_05.index) + list(data_16.index)))
    aux = data_16.reindex(aux_index)
    for quarter in reversed(aux_index):
        if aux.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            aux.loc[quarter, :] = (
                aux.loc[next_quarter, :]
                * data_05.loc[quarter, :]
                / data_05.loc[next_quarter, :]
            )
    spanish_names = [
        "Agropecuario, pesca y minería",
        "Industrias manufactureras",
        "Energía eléctrica, gas y agua",
        "Construcción",
        "Comercio, alojamiento y suministro de comidas y bebidas",
        "Transporte y almacenamiento, información y comunicaciones",
        "Otros servicios",
        "Impuestos menos subvenciones",
        "Producto bruto interno",
    ]
    aux = aux[spanish_names]
    r_bytes = get_with_ssl_context("bcu", sources["1983"])
    data_83 = (
        pd.read_excel(
            r_bytes,
            skiprows=10,
            nrows=8,
            index_col=1,
        )
        .iloc[:, 1:]
        .T
    )
    data_83.index = pd.date_range(
        start="1988-03-31", freq="QE-DEC", periods=len(data_83)
    )
    data_83["Impuestos menos subvenciones"] = np.nan
    data_83 = data_83[
        [
            "AGROPECUARIA",
            "INDUSTRIAS MANUFACTURERAS",
            "SUMINISTRO DE ELECTRICIDAD, GAS Y AGUA",
            "CONSTRUCCION",
            "COMERCIO, REPARACIONES, RESTAURANTES Y HOTELES",
            "TRANSPORTE, ALMACENAMIENTO Y COMUNICACIONES",
            "OTRAS (1)",
            "Impuestos menos subvenciones",
            "PRODUCTO INTERNO BRUTO",
        ]
    ]
    data_83.columns = aux.columns

    complete_index = list(dict.fromkeys(list(data_83.index) + list(aux.index)))
    output = aux.reindex(complete_index)
    for quarter in reversed(complete_index):
        if output.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            output.loc[quarter, :] = (
                output.loc[next_quarter, :]
                * data_83.loc[quarter, :]
                / data_83.loc[next_quarter, :]
            )

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": "Constant prices 2016",
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def national_accounts_demand_constant_nsa_extended(*args, **kwargs) -> Dataset:
    """Get demand-side national accounts data in NSA constant prices, 1988-.

    Three datasets with different base years, 1983, 2005 and 2016, are spliced
    in order to get to the result DataFrame.

    Returns
    -------
    National accounts, demand side, constant prices, NSA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    data_16 = load_dataset(
        "national_accounts_demand_constant_nsa", *args, **kwargs
    ).to_detailed()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16 = data_16.drop(["Variación de existencias"], axis=1)

    data_05_names = [
        "Gasto de consumo final",
        "Gasto de consumo final de hogares",
        "Gasto de consumo final del gobierno general",
        "Formación bruta de capital",
        "Formación bruta de capital fijo",
        "Sector público",
        "Sector privado",
        "Exportaciones",
        "Importaciones",
        "Producto bruto interno",
    ]
    r_bytes = get_with_ssl_context("bcu", sources["2005"])
    raw_05 = pd.read_excel(r_bytes, skiprows=9)
    data_05 = (
        raw_05.dropna(how="all", axis=1)
        .iloc[:, 1:]
        .set_index("Unnamed: 1")
        .dropna(how="all")
        .T
    )
    data_05.index = pd.date_range(
        start="2005-03-31", freq="QE-DEC", periods=len(data_05)
    )
    data_05 = data_05.apply(pd.to_numeric, errors="coerce").rename_axis(None)
    data_05.columns = data_05_names
    data_05["Importaciones"] = data_05["Importaciones"] * -1

    data_05 = data_05.drop(["Sector público", "Sector privado"], axis=1)
    data_05.columns = data_16.columns

    aux_index = list(dict.fromkeys(list(data_05.index) + list(data_16.index)))
    aux = data_16.reindex(aux_index)
    for quarter in reversed(aux_index):
        if aux.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            aux.loc[quarter, :] = (
                aux.loc[next_quarter, :]
                * data_05.loc[quarter, :]
                / data_05.loc[next_quarter, :]
            )

    r_bytes = get_with_ssl_context("bcu", sources["1983"])
    data_83 = (
        pd.read_excel(
            r_bytes,
            skiprows=10,
            nrows=11,
            index_col=1,
        )
        .iloc[:, 1:]
        .T
    )
    data_83.index = pd.date_range(
        start="1988-03-31", freq="QE-DEC", periods=len(data_83)
    )
    data_83.drop(
        ["Sector público", "Sector privado", "Variación de existencias"],
        axis=1,
        inplace=True,
    )
    data_83.columns = aux.columns

    complete_index = list(dict.fromkeys(list(data_83.index) + list(aux.index)))
    output = aux.reindex(complete_index)
    for quarter in reversed(complete_index):
        if output.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            output.loc[quarter, :] = (
                output.loc[next_quarter, :]
                * data_83.loc[quarter, :]
                / data_83.loc[next_quarter, :]
            )
    spanish_names = [
        "Gasto de consumo final",
        "Gasto de consumo final de hogares",
        "Gasto de consumo final del gobierno general",
        "Formación bruta de capital",
        "Formación bruta de capital fijo",
        "Exportaciones",
        "Importaciones",
        "Producto bruto interno",
    ]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": "Constant prices 2016",
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def gdp_index_constant_sa_extended(*args, **kwargs) -> Dataset:
    """Get GDP data in SA constant prices, 1988-.

    Three datasets with different base years, 1983, 2005 and 2016, are spliced
    in order to get to the result DataFrame.

    Returns
    -------
    GDP, constant prices, SA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    data_16 = load_dataset("gdp_index_constant_sa", *args, **kwargs).to_detailed()
    data_16.columns = data_16.columns.get_level_values(0)

    names = [
        "Actividades primarias",
        "Act. prim.: Agricultura, ganadería, caza y silvicultura",
        "Industrias manufactureras",
        "Suministro de electricidad, gas y agua",
        "Construcción",
        "Comercio, reparaciones, restaurantes y hoteles",
        "Transporte, almacenamiento y comunicaciones",
        "Otras actividades",
        "Otras actividades: SIFMI",
        "Impuestos menos subvenciones",
        "Producto bruto interno",
    ]
    r_bytes = get_with_ssl_context("bcu", sources["2005"])
    raw_05 = pd.read_excel(r_bytes, skiprows=9)
    data_05 = (
        raw_05.dropna(how="all", axis=1)
        .iloc[:, 1:]
        .set_index("Unnamed: 1")
        .dropna(how="all")
        .T
    )
    data_05.index = pd.date_range(
        start="1997-03-31", freq="QE-DEC", periods=len(data_05)
    )
    data_05 = data_05.apply(pd.to_numeric, errors="coerce").rename_axis(None)
    data_05.columns = names

    data_05 = data_05[["Producto bruto interno"]]
    aux_index = list(dict.fromkeys(list(data_05.index) + list(data_16.index)))
    aux = data_16.reindex(aux_index)
    for quarter in reversed(aux_index):
        if aux.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            aux.loc[quarter, :] = (
                aux.loc[next_quarter, :]
                * data_05.loc[quarter, :]
                / data_05.loc[next_quarter, :]
            )

    r_bytes = get_with_ssl_context("bcu", sources["1983"])
    data_83 = (
        pd.read_excel(
            r_bytes,
            skiprows=10,
            nrows=8,
            index_col=1,
        )
        .iloc[:, 1:]
        .T
    )
    data_83.index = pd.date_range(
        start="1988-03-31", freq="QE-DEC", periods=len(data_83)
    )
    data_83 = data_83[["PRODUCTO INTERNO BRUTO"]]
    data_83.columns = aux.columns

    complete_index = list(dict.fromkeys(list(data_83.index) + list(aux.index)))
    output = aux.reindex(complete_index)
    for quarter in reversed(complete_index):
        if output.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            output.loc[quarter, :] = (
                output.loc[next_quarter, :]
                * data_83.loc[quarter, :]
                / data_83.loc[next_quarter, :]
            )

    spanish_names = ["Producto bruto interno"]

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": "Constant prices 2016",
        "unit": "2016=100",
        "seasonal_adjustment": "Seasonally adjusted",
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def gdp_constant_nsa_extended(*args, **kwargs) -> Dataset:
    """Get GDP data in NSA constant prices, 1988-.

    Three datasets with two different base years, 1983 and 2016, are
    spliced in order to get to the result DataFrame. It uses the BCU's working
    paper for retropolated GDP in current and constant prices for 1997-2015.

    Returns
    -------
    GDP, constant prices, NSA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    data_16 = load_dataset(
        "national_accounts_supply_constant_nsa", *args, **kwargs
    ).to_detailed()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16 = data_16[["Producto bruto interno"]]

    names = ["Producto bruto interno"]

    r_bytes = get_with_ssl_context("bcu", sources["1997"])
    raw_97 = pd.read_excel(r_bytes, skiprows=6)
    data_97 = (
        raw_97.dropna(how="all", axis=1)
        .iloc[:, 1:]
        .set_index("Unnamed: 1")
        .dropna(how="all")
        .T
    )
    data_97.index = pd.date_range(
        start="1997-03-31", freq="QE-DEC", periods=len(data_97)
    )
    data_97 = data_97.apply(pd.to_numeric, errors="coerce").rename_axis(None)
    data_97.columns = names

    aux = pd.concat([data_97, data_16], axis=0)

    r_bytes = get_with_ssl_context("bcu", sources["1983"])
    data_83 = (
        pd.read_excel(
            r_bytes,
            skiprows=10,
            nrows=8,
            index_col=1,
        )
        .iloc[:, 1:]
        .T
    )
    data_83.index = pd.date_range(
        start="1988-03-31", freq="QE-DEC", periods=len(data_83)
    )
    data_83 = data_83[["PRODUCTO INTERNO BRUTO"]]
    data_83.columns = ["Producto bruto interno"]

    reaux_index = list(dict.fromkeys(list(data_83.index) + list(aux.index)))
    output = aux.reindex(reaux_index)
    for quarter in reversed(reaux_index):
        if output.loc[quarter, :].isna().all():
            next_quarter = quarter + MonthEnd(3)
            output.loc[quarter, :] = (
                output.loc[next_quarter, :]
                * data_83.loc[quarter, :]
                / data_83.loc[next_quarter, :]
            )

    spanish_names = ["Producto bruto interno"]

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": "Constant prices 2016",
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def gdp_current_nsa_extended(*args, **kwargs) -> Dataset:
    """Get GDP data in NSA current prices, 1997-.

    It uses the BCU's working paper for retropolated GDP in current and constant prices for
    1997-2015.

    Returns
    -------
    GDP, current prices, NSA : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    data_16 = load_dataset(
        "national_accounts_supply_current_nsa", *args, **kwargs
    ).to_detailed()
    data_16.columns = data_16.columns.get_level_values(0)
    data_16 = data_16[["Producto bruto interno"]]

    names = ["Producto bruto interno"]

    r_bytes = get_with_ssl_context("bcu", sources["1997"])
    raw_97 = pd.read_excel(r_bytes, skiprows=6)
    data_97 = (
        raw_97.dropna(how="all", axis=1)
        .iloc[:, 1:]
        .set_index("Unnamed: 1")
        .dropna(how="all")
        .T
    )
    data_97.index = pd.date_range(
        start="1997-03-31", freq="QE-DEC", periods=len(data_97)
    )
    data_97 = data_97.apply(pd.to_numeric, errors="coerce").rename_axis(None)
    data_97.columns = names

    output = pd.concat([data_97, data_16], axis=0)

    spanish_names = ["Producto bruto interno"]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def gdp_denominator(*args, **kwargs):
    """Get nominal GDP data in UYU and USD with forecasts.

    Update nominal GDP data for use in the `transform.convert_gdp()` function.
    Get IMF forecasts for year of last available data point and the next
    year (for example, if the last period available at the BCU website is
    september 2019, fetch forecasts for 2019 and 2020).

    Returns
    -------
    output : Pandas dataframe
        Quarterly GDP in UYU and USD with 1 year forecasts.

    """
    name = get_name_from_function()

    data_uyu = load_dataset("gdp_current_nsa_extended", *args, **kwargs).rolling(
        window=4, operation="sum"
    )
    data_usd = data_uyu.convert("usd")
    data_uyu, data_usd = data_uyu.data, data_usd.data

    data = [data_uyu, data_usd]
    last_year = data_uyu.index.max().year
    if data_uyu.index.max().month == 12:
        last_year += 1

    results = []
    for table, gdp in zip(["NGDP", "NGDPD"], data):
        table_url = (
            f"https://www.imf.org/en/Publications/WEO/weo-database/"
            f"2024/October/weo-report?c=298,&s={table},&sy="
            f"{last_year - 1}&ey={last_year + 1}&ssm=0&scsm=1&scc=0&"
            f"ssd=1&ssc=0&sic=0&sort=country&ds=.&br=1"
        )
        imf_data = pd.to_numeric(pd.read_html(table_url)[0].iloc[0, [5, 6, 7]])
        imf_data = imf_data.reset_index(drop=True)
        fcast = (
            gdp.loc[[dt.datetime(last_year - 1, 12, 31)]]
            .multiply(imf_data.iloc[1])
            .divide(imf_data.iloc[0])
        )
        fcast = fcast.rename(
            index={dt.datetime(last_year - 1, 12, 31): dt.datetime(last_year, 12, 31)}
        )
        next_fcast = (
            gdp.loc[[dt.datetime(last_year - 1, 12, 31)]]
            .multiply(imf_data.iloc[2])
            .divide(imf_data.iloc[0])
        )
        next_fcast = next_fcast.rename(
            index={
                dt.datetime(last_year - 1, 12, 31): dt.datetime(last_year + 1, 12, 31)
            }
        )
        gdp = pd.concat([gdp, fcast, next_fcast], axis=0)
        results.append(gdp)

    output = pd.concat(results, axis=1)
    output = output.resample("QE-DEC").interpolate("linear").dropna(how="all")
    output = output.rename_axis(None)

    spanish_names = ["Producto bruto interno", "Producto bruto interno"]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Flow",
        "cumulative_periods": 4,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    metadata.update_indicator_metadata_value(f"{name}_1", "currency", "USD")
    dataset = Dataset(name, output, metadata)

    return dataset


def industrial_production() -> Dataset:
    """Get industrial production data.

    Returns
    -------
    Monthly industrial production index : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    raw = pd.read_excel(sources["main"], skiprows=4, usecols="D:DR", na_values="(s)")
    weights = pd.read_csv(sources["weights"]).dropna(how="all")
    weights[["División", "Grupo", "Agrupación / Clase"]] = weights[
        ["División", "Grupo", "Agrupación / Clase"]
    ].astype(str)
    output = raw.dropna(how="all")
    output.index = pd.date_range(start="2018-01-31", freq="ME", periods=len(output))

    column_names = []
    for c in output.columns[2:]:
        c = str(c)
        match = weights.loc[weights["División"] == c, "Denominación"]
        prefix = "División: "
        if isinstance(match, pd.Series) and match.empty:
            prefix = "Grupo: "
            match = weights.loc[weights["Grupo"] == c, "Denominación"]
            if isinstance(match, pd.Series) and match.empty:
                prefix = "Clase: "
                match = weights.loc[weights["Agrupación / Clase"] == c, "Denominación"]
        try:
            match = match.iloc[0]
        except (AttributeError, IndexError):
            pass
        try:
            match = prefix + match.capitalize().strip()
        except AttributeError:
            match = c
        match = re.sub(r"[\(\)]", "-", match)
        column_names.append(match)

    output = output.apply(pd.to_numeric, errors="coerce").rename_axis(None)

    spanish_names = [
        "Industrias manufactureras",
        "Industrias manufactureras sin refinería",
    ] + column_names
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "2018=100",
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


def core_industrial_production(*args, **kwargs) -> Dataset:
    """
    Get total industrial production, industrial production excluding oil
    refinery and core industrial production.


    Returns
    -------
    Measures of industrial production : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    data_18 = load_dataset("industrial_production", *args, **kwargs).to_detailed()
    data_18 = data_18[
        [
            "Industrias manufactureras",
            "Industrias manufactureras sin refinería",
            "Clase: Elaboración de comidas y platos preparados; elaboración de otros productos alimenticios",
            "Clase: Fabricación de pasta de celulosa, papel y cartón",
        ]
    ].copy()
    data_18.columns = ["total", "ex-refinery", "other foods", "pulp"]

    data_18_weights = {"other foods": 0.0991, "pulp": 0.0907}
    data_18["core"] = data_18["ex-refinery"] - (
        data_18["other foods"] * data_18_weights["other foods"]
        + data_18["pulp"] * data_18_weights["pulp"]
    )
    data_18 = data_18[["total", "ex-refinery", "core"]]

    data_06 = pd.read_excel(
        sources["2006"], skiprows=6, usecols="B,D,F,CF,CX", na_values="(s)"
    ).dropna(how="all")
    data_06 = data_06.loc[~data_06.iloc[:, 0].str.contains("Prom")].iloc[:, 1:]
    data_06.columns = ["total", "ex-refinery", "other foods", "pulp"]
    data_06_weights = {
        "other foods": 0.3733 * 0.3107 * 0.7089,
        "pulp": 0.0184 * 0.4395,
    }  # https://www5.ine.gub.uy/documents/Estad%C3%ADsticasecon%C3%B3micas/SERIES%20Y%20OTROS/IVFIM/Ponderadores%20de%20VBP%202006.xls
    data_06["core"] = data_06["ex-refinery"] - (
        data_06["other foods"] * data_06_weights["other foods"]
        + data_06["pulp"] * data_06_weights["pulp"]
    )
    data_06 = data_06[["total", "ex-refinery", "core"]]
    data_06.index = pd.date_range(start="2002-01-31", freq="ME", periods=len(data_06))

    aux_index = list(dict.fromkeys(list(data_06.index) + list(data_18.index)))
    output = data_18.reindex(aux_index)
    for month in reversed(aux_index):
        if output.loc[month, :].isna().all():
            next_month = month + MonthEnd(1)
            output.loc[month, :] = (
                output.loc[next_month, :]
                * data_06.loc[month, :]
                / data_06.loc[next_month, :]
            )

    output = output.rename_axis(None)

    spanish_names = [
        "Industrias manufactureras",
        "Industrias manufactureras sin refinería",
        "Núcleo industrial",
    ]

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "2018=100",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata).rebase(
        start_date="2018-01-01", end_date="2018-12-31"
    )
    dataset.metadata.update_dataset_metadata({"unit": "2018=100"})
    dataset.transformed = False

    return dataset


def livestock_slaughter() -> Dataset:
    """Get weekly livestock slaughter data.

    Returns
    -------
    Weekly livestock slaughter : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    excel = pd.ExcelFile(sources["main"])
    cattle = pd.read_excel(excel, sheet_name="BOVINOS", skiprows=8, usecols="C:H")
    sheep = pd.read_excel(excel, sheet_name="OVINOS", skiprows=8, usecols="C:H")
    output = pd.concat([cattle, sheep], axis=1).fillna(0).astype(int)
    output.index = pd.date_range(start="2005-01-02", freq="W", periods=len(output))
    output = output.rename_axis(None)

    spanish_names = [
        "Novillos",
        "Vacas",
        "Vaquillonas",
        "Terneros",
        "Toros",
        "Total bovinos",
        "Borregos",
        "Capones",
        "Carneros",
        "Corderos",
        "Ovejas",
        "Total ovinos",
    ]

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": None,
        "inflation_adjustment": None,
        "unit": "Heads",
        "seasonal_adjustment": None,
        "frequency": "W",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def milk_shipments() -> Dataset:
    """Get monthly milk shipments from farms data.

    Returns
    -------
    Monhtly milk shipments from farms : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    r = httpx.get(sources["main"])
    url = re.findall(r'href="(.+\.xls)', r.text)[0]
    raw = pd.read_excel(
        url, sheet_name="Listado Datos", usecols="C:D", skiprows=4
    ).dropna()

    output = (
        raw.set_index(pd.date_range(start="2002-01-31", freq="ME", periods=len(raw)))
        / 1000
    )
    output = output.apply(pd.to_numeric)

    spanish_names = ["Remisión, litros", "Remisión, kilogramos"]

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": None,
        "inflation_adjustment": None,
        "unit": "Thousand liters",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    metadata.update_indicator_metadata_value(
        "milk_shipments_1", "unit", "Thousand kilograms"
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def diesel_sales() -> Dataset:
    """
    Get diesel sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly diesel dales : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = httpx.get(sources["main"])
        rar_url = re.findall(
            r'(https?://[^"]*?gas%20oil[^"]*?\.rar)',
            r.text,
        )[0]
        f.write(httpx.get(rar_url).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(
            path_temp, sheet_name="vta gas oil por depto", skiprows=2, usecols="C:W"
        )
        raw.index = pd.date_range(start="2004-01-31", freq="ME", periods=len(raw))
        raw.columns = list(raw.columns.str.replace("\n", " "))[:-1] + ["Total"]
        output = raw
    output = output.rename_axis(None)

    spanish_names = output.columns

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": None,
        "inflation_adjustment": None,
        "unit": "Cubic meters",
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


def gasoline_sales() -> Dataset:
    """
    Get gasoline sales by department data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly gasoline dales : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = httpx.get(sources["main"])
        rar_url = re.findall(
            r'(https?://[^"]*?gasolinas[^"]*?\.rar)',
            r.text,
        )[0]
        f.write(httpx.get(rar_url).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(
            path_temp, sheet_name="vta gasolinas por depto", skiprows=2, usecols="C:W"
        )
        raw.index = pd.date_range(start="2004-01-31", freq="ME", periods=len(raw))
        raw.columns = list(raw.columns.str.replace("\n", " "))[:-1] + ["Total"]
        output = raw
    output = output.rename_axis(None)

    spanish_names = output.columns

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": None,
        "inflation_adjustment": None,
        "unit": "Cubic meters",
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


def electricity_sales() -> Dataset:
    """
    Get electricity sales by sector data.

    This retrieval function requires the unrar binaries to be found in your
    system.

    Returns
    -------
    Monthly electricity dales : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        r = httpx.get(sources["main"])
        rar_url = re.findall(
            r"https://www.gub.uy/ministerio-industria-energia-mineria/sites/ministerio-industria-energia-mineria/files/[0-9\-]+/Facturaci%C3%B3n%20de%20energ%C3%ADa%20el%C3%A9ctrica%20por%20sector.rar",
            r.text,
        )[0]
        f.write(httpx.get(rar_url).content)
    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir, verbosity=-1)
        xls = [x for x in listdir(temp_dir) if x.endswith(".xls")][0]
        path_temp = path.join(temp_dir, xls)
        raw = pd.read_excel(path_temp, sheet_name="fact ee", skiprows=2, usecols="C:J")
        raw.index = pd.date_range(start="2000-01-31", freq="ME", periods=len(raw))
        raw.columns = raw.columns.str.capitalize()
        output = raw
    output = output.rename_axis(None)

    spanish_names = output.columns

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Economic activity",
        "currency": None,
        "inflation_adjustment": None,
        "unit": "MWh",
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
