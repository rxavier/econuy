import datetime as dt
import re
from urllib.error import URLError

import pandas as pd
import httpx
from pandas.tseries.offsets import MonthEnd

from econuy import load_dataset
from econuy.base import Dataset, DatasetMetadata
from econuy.utils.extras import FISCAL_SHEETS, taxes_columns
from econuy.utils.operations import get_name_from_function, get_download_sources
from econuy.utils.retrieval import get_with_ssl_context


def _get_fiscal_balances(dataset_name: str) -> Dataset:
    """Helper function. See any of the `fiscal_balance_...()` functions."""
    sources = get_download_sources("fiscal_balances")
    response = httpx.get(sources["main"])
    url = re.findall(r"(http\S+Resultados.+\.xlsx)'", response.text)[0]
    xls = pd.ExcelFile(url)
    output = {}
    dataset_details = FISCAL_SHEETS[dataset_name]
    output = (
        pd.read_excel(xls, sheet_name=dataset_details["sheet"])
        .dropna(axis=0, thresh=4)
        .dropna(axis=1, thresh=4)
        .transpose()
        .set_index(2, drop=True)
    )
    output = output[output.index.notnull()].rename_axis(None)
    output.index = output.index + MonthEnd(1)
    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    ids = [f"{dataset_name}_{i}" for i in range(output.shape[1])]
    output.columns = ids
    spanish_names = dataset_details["colnames"]
    spanish_names = [{"es": x} for x in spanish_names]

    base_metadata = {
        "area": "Public sector",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Flow",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        dataset_name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(dataset_name, output, metadata)

    return dataset


def fiscal_balance_global_public_sector() -> Dataset:
    """Get fiscal balance data for the consolidated public sector.

    Returns
    -------
    Monthly fiscal balance for the consolidated public sector : Dataset

    """
    name = get_name_from_function()
    return _get_fiscal_balances(name)


def fiscal_balance_nonfinancial_public_sector() -> Dataset:
    """Get fiscal balance data for the non-financial public sector.

    Returns
    -------
    Monthly fiscal balance for the non-financial public sector : Dataset

    """
    name = get_name_from_function()
    return _get_fiscal_balances(name)


def fiscal_balance_central_government() -> Dataset:
    """Get fiscal balance data for the central government + BPS.

    Returns
    -------
    Monthly fiscal balance for the central government + BPS : Dataset

    """
    name = get_name_from_function()
    return _get_fiscal_balances(name)


def fiscal_balance_soe() -> Dataset:
    """Get fiscal balance data for public enterprises.

    Returns
    -------
    Monthly fiscal balance for public enterprises : Dataset

    """
    name = get_name_from_function()
    return _get_fiscal_balances(name)


def fiscal_balance_ancap() -> Dataset:
    """Get fiscal balance data for ANCAP.

    Returns
    -------
    Monthly fiscal balance for ANCAP : Dataset

    """
    name = get_name_from_function()
    return _get_fiscal_balances(name)


def fiscal_balance_ute() -> Dataset:
    """Get fiscal balance data for UTE.

    Returns
    -------
    Monthly fiscal balance for UTE : Dataset

    """
    name = get_name_from_function()
    return _get_fiscal_balances(name)


def fiscal_balance_antel() -> Dataset:
    """Get fiscal balance data for ANTEL.

    Returns
    -------
    Monthly fiscal balance for ANTEL : Dataset

    """
    name = get_name_from_function()
    return _get_fiscal_balances(name)


def fiscal_balance_ose() -> Dataset:
    """Get fiscal balance data for OSE.

    Returns
    -------
    Monthly fiscal balance for OSE : Dataset

    """
    name = get_name_from_function()
    return _get_fiscal_balances(name)


def tax_revenue() -> Dataset:
    """
    Get tax revenues data.

    This retrieval function requires that Ghostscript and Tkinter be found in
    your system.

    Returns
    -------
    Monthly tax revenues : Dataset

    """
    name = get_name_from_function()
    sources = get_download_sources(name)
    r = httpx.get(sources["main"], timeout=20)
    url = re.findall(
        "https://[A-z0-9-/\.]+Recaudaci%C3%B3n%20por%20impuesto%20-%20Series%20mensuales.csv",
        r.text,
    )[0]
    historical = pd.read_csv(
        url, skiprows=2, encoding="Latin", sep=";", thousands="."
    ).iloc[:, 3:41]
    historical.index = pd.date_range("1982-01-31", periods=len(historical), freq="ME")
    historical.columns = taxes_columns
    historical = historical.div(1000000)

    try:
        aux = historical.copy()
        latest = pd.read_csv(sources["pdfs"], index_col=0, parse_dates=True)
        latest.columns = [
            "IVA - Valor Agregado",
            "IMESI - Específico Interno",
            "IMEBA - Enajenación de Bienes Agropecuarios",
            "IRAE - Rentas de Actividades Económicas",
            "IRPF Cat I - Renta de las Personas Físicas",
            "IRPF Cat II - Rentas de las Personas Físicas",
            "IASS - Asistencia a la Seguridad Social",
            "IRNR - Rentas de No Residentes",
            "Impuesto de Educación Primaria",
            "Recaudación Total de la DGI",
        ]
        latest = latest.loc[[x not in aux.index for x in latest.index]]
        for col in latest.columns:
            for date in latest.index:
                prev_year = date + MonthEnd(-12)
                aux.loc[date, col] = aux.loc[prev_year, col] * (
                    1 + latest.loc[date, col] / 100
                )
        output = pd.concat([aux, latest], sort=False)
        output = output.loc[~output.index.duplicated(keep="first")]
    except Exception as e:
        print(f"Could not get PDF data | {e}")
        output = historical.copy()

    output = output.apply(pd.to_numeric, errors="coerce")
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Public sector",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "Millions",
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


def _get_public_debt(dataset_name: str) -> Dataset:
    """Helper function. See any of the `public_debt_...()` functions."""
    sources = get_download_sources("public_debt")
    colnames = [
        "Total deuda",
        "Plazo contractual: hasta 1 año",
        "Plazo contractual: entre 1 y 5 años",
        "Plazo contractual: más de 5 años",
        "Plazo residual: hasta 1 año",
        "Plazo residual: entre 1 y 5 años",
        "Plazo residual: más de 5 años",
        "Moneda: pesos",
        "Moneda: dólares",
        "Moneda: euros",
        "Moneda: yenes",
        "Moneda: DEG",
        "Moneda: otras",
        "Residencia: no residentes",
        "Residencia: residentes",
    ]
    try:
        xls = pd.ExcelFile(sources["main"])
    except URLError as err:
        if "SSL: CERTIFICATE_VERIFY_FAILED" in str(err):
            r_bytes = get_with_ssl_context("bcu", sources["main"])
            xls = pd.ExcelFile(r_bytes)

    if dataset_name == "public_debt_global_public_sector":
        gps_raw = pd.read_excel(
            xls,
            sheet_name="SPG2",
            usecols="B:Q",
            index_col=0,
            skiprows=10,
            nrows=(dt.datetime.now().year - 1999) * 4,
        )
        output = gps_raw.dropna(thresh=2)
        output.index = pd.date_range(
            start="1999-12-31", periods=len(output), freq="QE-DEC"
        )
        output.columns = colnames

    elif dataset_name == "public_debt_nonfinancial_public_sector":
        nfps_raw = pd.read_excel(
            xls, sheet_name="SPNM bruta", usecols="B:O", index_col=0
        )
        loc = nfps_raw.index.get_loc(
            "9. Deuda Bruta del Sector Público no " "monetario por plazo y  moneda."
        )
        output = nfps_raw.iloc[loc + 5 :, :].dropna(how="any")
        output.index = pd.date_range(
            start="1999-12-31", periods=len(output), freq="QE-DEC"
        )
        nfps_extra_raw = pd.read_excel(
            xls,
            sheet_name="SPNM bruta",
            usecols="O:P",
            skiprows=11,
            nrows=(dt.datetime.now().year - 1999) * 4,
        )
        nfps_extra = nfps_extra_raw.dropna(how="all")
        nfps_extra.index = output.index
        output = pd.concat([output, nfps_extra], axis=1)
        output.columns = colnames

    elif dataset_name == "public_debt_central_bank":
        cb_raw = pd.read_excel(
            xls,
            sheet_name="BCU bruta",
            usecols="B:O",
            index_col=0,
            skiprows=(dt.datetime.now().year - 1999) * 8 + 20,
        )
        output = cb_raw.dropna(how="any")
        output.index = pd.date_range(
            start="1999-12-31", periods=len(output), freq="QE-DEC"
        )
        cb_extra_raw = pd.read_excel(
            xls,
            sheet_name="BCU bruta",
            usecols="O:P",
            skiprows=11,
            nrows=(dt.datetime.now().year - 1999) * 4,
        )
        bcu_extra = cb_extra_raw.dropna(how="all")
        bcu_extra.index = output.index
        output = pd.concat([output, bcu_extra], axis=1)
        output.columns = colnames

    else:
        assets_raw = pd.read_excel(
            xls,
            sheet_name="Activos Neta",
            usecols="B,C,D,K",
            index_col=0,
            skiprows=13,
            nrows=(dt.datetime.now().year - 1999) * 4 - 1,
        )
        output = assets_raw.dropna(how="any")
        output.index = pd.date_range(
            start="1999-12-31", periods=len(output), freq="QE-DEC"
        )
        output.columns = ["Total activos", "Sector público no monetario", "BCU"]

    output = output.apply(pd.to_numeric, errors="coerce")

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{dataset_name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Public sector",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "ME",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        dataset_name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(dataset_name, output, metadata)

    return dataset


def public_debt_global_public_sector() -> Dataset:
    """Get public debt data for the consolidated public sector.

    Returns
    -------
    Quarterly public debt data for the consolidated public sector: Dataset

    """
    name = get_name_from_function()
    return _get_public_debt(name)


def public_debt_nonfinancial_public_sector() -> Dataset:
    """Get public debt data for the non-financial public sector.

    Returns
    -------
    Quarterly public debt data for the non-financial public sector: Dataset

    """
    name = get_name_from_function()
    return _get_public_debt(name)


def public_debt_central_bank() -> Dataset:
    """Get public debt data for the central bank

    Returns
    -------
    Quarterly public debt data for the central bank : Dataset

    """
    name = get_name_from_function()
    return _get_public_debt(name)


def public_assets() -> Dataset:
    """Get public sector assets data.

    Returns
    -------
    Quarterly public sector assets: Dataset

    """
    name = get_name_from_function()
    return _get_public_debt(name)


def net_public_debt_global_public_sector(*args, **kwargs) -> Dataset:
    """
    Get net public debt excluding deposits at the central bank.


    Returns
    -------
    Net public debt excl. deposits at the central bank : Dataset

    """
    name = get_name_from_function()

    gross_debt = load_dataset(
        "public_debt_global_public_sector", *args, **kwargs
    ).to_named()[["Total deuda"]]
    assets = load_dataset("public_assets", *args, **kwargs).to_named()[
        ["Total activos"]
    ]
    gross_debt.columns = ["Deuda neta del sector público global excl. encajes"]
    assets.columns = gross_debt.columns
    deposits = (
        load_dataset("international_reserves", *args, **kwargs)
        .resample("QE-DEC", "last")
        .to_named()[["Obligaciones en ME con el sector financiero"]]
    )
    deposits = deposits.reindex(gross_debt.index).squeeze()
    output = gross_debt.add(assets).add(deposits, axis=0).dropna()
    output = output.rename_axis(None)

    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]
    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Public sector",
        "currency": "USD",
        "inflation_adjustment": None,
        "unit": "Millions",
        "seasonal_adjustment": None,
        "frequency": "QE-DEC",
        "time_series_type": "Stock",
        "cumulative_periods": 1,
        "transformations": [],
    }
    metadata = DatasetMetadata.from_cast(
        name, base_metadata, output.columns, spanish_names
    )
    dataset = Dataset(name, output, metadata)

    return dataset


def fiscal_balance_summary(*args, **kwargs) -> Dataset:
    """
    Get the summary fiscal balance table found in the `Budget Law
    <https://www.gub.uy/contaduria-general-nacion/sites/
    contaduria-general-nacion/files/2020-09/
    Mensaje%20y%20Exposici%C3%B3n%20de%20motivos.pdf>`_. Includes adjustments
    for the `Social Security Fund <https://www.impo.com.uy/bases/decretos/
    71-2018/25>`_.

    Returns
    -------
    Summary fiscal balance table : Dataset

    """
    name = get_name_from_function()

    datasets = {}
    for dataset_name in [
        "fiscal_balance_global_public_sector",
        "fiscal_balance_nonfinancial_public_sector",
        "fiscal_balance_central_government",
        "fiscal_balance_soe",
    ]:
        d = load_dataset(dataset_name, *args, **kwargs).to_detailed()
        d.columns = d.columns.get_level_values(0)
        datasets.update({dataset_name: d})
    gps = datasets["fiscal_balance_global_public_sector"]
    nfps = datasets["fiscal_balance_nonfinancial_public_sector"]
    gc = datasets["fiscal_balance_central_government"]
    pe = datasets["fiscal_balance_soe"]
    proc = pd.DataFrame(index=gps.index)

    proc["Ingresos: GC-BPS"] = gc["Ingresos: GC-BPS"]
    proc["Ingresos: GC-BPS ex. FSS"] = (
        gc["Ingresos: GC-BPS"] - gc["Ingresos: FSS - Cincuentones"]
    )
    proc["Ingresos: GC"] = gc["Ingresos: GC"]
    proc["Ingresos: DGI"] = gc["Ingresos: DGI"]
    proc["Ingresos: Comercio ext."] = gc["Ingresos: Comercio ext."]
    proc["Ingresos: Otros"] = (
        gc["Ingresos: GC"] - gc["Ingresos: DGI"] - gc["Ingresos: Comercio ext."]
    )
    proc["Ingresos: BPS"] = gc["Ingresos: BPS neto"]
    proc["Ingresos: FSS - Cincuentones"] = gc["Ingresos: FSS - Cincuentones"]
    proc["Ingresos: BPS ex FSS"] = (
        gc["Ingresos: BPS neto"] - gc["Ingresos: FSS - Cincuentones"]
    )
    proc["Egresos: Primarios GC-BPS"] = gc["Egresos: GC-BPS"] - gc["Intereses: Total"]
    proc["Egresos: Primarios corrientes GC-BPS"] = (
        proc["Egresos: Primarios GC-BPS"] - gc["Egresos: Inversión"].squeeze()
    )
    proc["Egresos: Remuneraciones"] = gc["Egresos: Remuneraciones"]
    proc["Egresos: No personales"] = gc["Egresos: No personales"]
    proc["Egresos: Pasividades"] = gc["Egresos: Pasividades"]
    proc["Egresos: Transferencias"] = gc["Egresos: Transferencias"]
    proc["Egresos: Inversión"] = gc["Egresos: Inversión"]
    proc["Resultado: Primario GC-BPS"] = (
        proc["Ingresos: GC-BPS"] - proc["Egresos: Primarios GC-BPS"]
    )
    proc["Resultado: Primario GC-BPS ex FSS"] = (
        proc["Ingresos: GC-BPS ex. FSS"] - proc["Egresos: Primarios GC-BPS"]
    )
    proc["Intereses: GC-BPS"] = gc["Intereses: Total"]
    proc["Intereses: FSS - Cincuentones"] = gc["Intereses: FSS - Cincuentones"]
    proc["Intereses: GC-BPS ex FSS"] = (
        proc["Intereses: GC-BPS"] - proc["Intereses: FSS - Cincuentones"]
    )
    proc["Resultado: Global GC-BPS"] = (
        proc["Resultado: Primario GC-BPS"] - proc["Intereses: GC-BPS"]
    )
    proc["Resultado: Global GC-BPS ex FSS"] = (
        proc["Resultado: Primario GC-BPS ex FSS"] - proc["Intereses: GC-BPS ex FSS"]
    )

    proc["Resultado: Primario corriente EEPP"] = nfps[
        "Ingresos: Res. primario corriente EEPP"
    ]
    proc["Egresos: Inversiones EEPP"] = pe["Egresos: Inversiones"]
    proc["Resultado: Primario EEPP"] = (
        proc["Resultado: Primario corriente EEPP"] - proc["Egresos: Inversiones EEPP"]
    )
    proc["Intereses: EEPP"] = pe["Intereses"]
    proc["Resultado: Global EEPP"] = (
        proc["Resultado: Primario EEPP"] - proc["Intereses: EEPP"]
    )

    proc["Resultado: Primario intendencias"] = nfps["Resultado: Primario intendencias"]
    proc["Intereses: Intendencias"] = nfps["Intereses: Intendencias"]
    proc["Resultado: Global intendencias"] = (
        proc["Resultado: Primario intendencias"] - proc["Intereses: Intendencias"]
    )

    proc["Resultado: Primario BSE"] = nfps["Resultado: Primario BSE"]
    proc["Intereses: BSE"] = nfps["Intereses: BSE"]
    proc["Resultado: Global BSE"] = (
        proc["Resultado: Primario BSE"] - proc["Intereses: BSE"]
    )

    proc["Resultado: Primario resto SPNF"] = (
        proc["Resultado: Primario EEPP"]
        + proc["Resultado: Primario intendencias"]
        + proc["Resultado: Primario BSE"]
    )
    proc["Intereses: Resto SPNF"] = (
        proc["Intereses: EEPP"]
        + proc["Intereses: Intendencias"]
        + proc["Intereses: BSE"]
    )
    proc["Resultado: Global resto SPNF"] = (
        proc["Resultado: Global EEPP"]
        + proc["Resultado: Global intendencias"]
        + proc["Resultado: Global BSE"]
    )
    proc["Resultado: Primario SPNF"] = nfps["Resultado: Primario SPNF"]
    proc["Resultado: Primario SPNF ex FSS"] = (
        proc["Resultado: Primario SPNF"] - proc["Ingresos: FSS - Cincuentones"]
    )
    proc["Intereses: SPNF"] = nfps["Intereses: Totales"]
    proc["Intereses: SPNF ex FSS"] = (
        proc["Intereses: SPNF"] - proc["Intereses: FSS - Cincuentones"]
    )
    proc["Resultado: Global SPNF"] = nfps["Resultado: Global SPNF"]
    proc["Resultado: Global SPNF ex FSS"] = (
        proc["Resultado: Primario SPNF ex FSS"] - proc["Intereses: SPNF ex FSS"]
    )

    proc["Resultado: Primario BCU"] = gps["Resultado: Primario BCU"]
    proc["Intereses: BCU"] = gps["Intereses: BCU"]
    proc["Resultado: Global BCU"] = gps["Resultado: Global BCU"]

    proc["Resultado: Primario SPC"] = gps["Resultado: Primario SPC"]
    proc["Resultado: Primario SPC ex FSS"] = (
        proc["Resultado: Primario SPNF ex FSS"] + proc["Resultado: Primario BCU"]
    )
    proc["Intereses: SPC"] = proc["Intereses: SPNF"] + proc["Intereses: BCU"]
    proc["Intereses: SPC ex FSS"] = (
        proc["Intereses: SPNF ex FSS"] + proc["Intereses: BCU"]
    )
    proc["Resultado: Global SPC"] = (
        proc["Resultado: Global SPNF"] + proc["Resultado: Global BCU"]
    )
    proc["Resultado: Global SPC ex FSS"] = (
        proc["Resultado: Global SPNF ex FSS"] + proc["Resultado: Global BCU"]
    )
    output = proc
    output = output.rename_axis(None)
    spanish_names = output.columns
    spanish_names = [{"es": x} for x in spanish_names]

    ids = [f"{name}_{i}" for i in range(output.shape[1])]
    output.columns = ids

    base_metadata = {
        "area": "Public sector",
        "currency": "UYU",
        "inflation_adjustment": None,
        "unit": "Millions",
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
