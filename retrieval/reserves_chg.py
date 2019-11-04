import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
import urllib

from processing import colnames

BASE_URL = "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Informe%20Diario%20Pasivos%20Monetarios/infd_"
MONTHS = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "set", "oct", "nov", "dic"]
COLUMNS = ['SALDO AL INICIO DEL PERÍODO', '1. Compras netas de moneda extranjera', '1.1. Compras netas en el mercado',
           '1.2. Integración en dólares de valores del BCU ', '1.3. Prefinanciación de exportaciones',
           '1.4. Cancelación de contratos forward', '1.5. Gobierno Central',
           '1.5.1. Utilizaciones de préstamos internacionales', '1.5.2. Otras compras netas al Gobierno Central',
           '1.6. Otros', '2. Depósitos del Sistema Bancario en el Banco Central', '2.1. Banca pública',
           '2.2. Banca privada', '3.- Otros Depósitos en el Banco Central',
           '3.1. Depósitos de otras empresas de intermediación financiera ',
           '3.2. Depósitos de casas de cambio y otras Instituciones',
           '3.3. Depósitos de empresas públicas y gobiernos departamentales', '4. Divisas de exportación a liquidar',
           '5.- Obligaciones netas en moneda extranjera con Gobierno Central   ',
           '5.1. Colocación neta de bonos y letras  ', '5.1.1. Colocación bruta', '5.1.2. Amortizaciones  ',
           '5.1.3. Intereses y comisiones', '5.2. Otras obligaciones netas en moneda extranjera  ',
           '5.2.1. Desembolsos de préstamos internacionales', '5.2.2. Servicio neto de préstamos internacionales',
           '5.3.3. Utilizaciones de préstamos internacionales', '5.4.5. Aporte de entes a cuenta de resultados',
           '5.4.6.  Compras de moneda extranjera', '5.4.7. Giros hacia y desde el BROU',
           '5.4.8. Integración en dólares de tíulos en UI y pesos (neto)', '5.4.9. Otros', '6.- Intereses netos',
           '6.1.Intereses pagados sobre depósitos del sistema financiero',
           '6.2. Intereses cobrados sobre fondos colocados en el exterior', '6.3. Otros intereses y comisiones netos',
           '7.- Otros', '7.1.  Préstamos y financiamientos empresas públicas',
           '7.2. Cuentas con organismos internacionales', ' 7.3. Fondos administrados', '7.4. Diferencias de arbitraje',
           '7.5. Diferencias de cotización e intereses devengados', '7.6. Depósitos especiales Clearstream Banking ',
           '7.7. Solicitudes de giro al exterior en trámite', '7.8. Otros',
           'VARIACIÓN TOTAL DEL PERÍODO (1+2+3+4+5+6+7)', 'SALDO AL FINAL  DEL PERÍODO']
YEARS = list(range(2013, dt.datetime.now().year + 1))
FILES = [month + str(year) for year in YEARS for month in MONTHS]


def base_reports(files, update=None):

    urls = [f"{BASE_URL}{file}.xls" for file in files]

    if update is not None:
        urls = urls[-12:]
        previous_data = pd.read_csv(update, sep=" ", index_col=0, header=[0, 1, 2, 3, 4, 5, 6, 7, 8])
        previous_data.columns = COLUMNS[1:46]
        previous_data.index = pd.to_datetime(previous_data.index)

    reports = []
    for url in urls:

        try:
            month_of_report = pd.read_excel(url, sheet_name="INDICE")
            first_day = month_of_report.iloc[7, 4]
            last_day = first_day + relativedelta(months=1) - dt.timedelta(days=1)

            base = pd.read_excel(url, sheet_name="ACTIVOS DE RESERVA",
                                 skiprows=3).dropna(axis=0, thresh=20).dropna(axis=1, thresh=20)
            base_transpose = base.transpose()
            base_transpose.index.name = "Date"
            base_transpose = base_transpose.iloc[:, 1:46]
            base_transpose.columns = COLUMNS[1:46]
            base_transpose = base_transpose.iloc[1:]
            base_transpose.index = pd.to_datetime(base_transpose.index, errors="coerce")
            base_transpose = base_transpose.loc[base_transpose.index.dropna()]
            base_transpose = base_transpose.loc[first_day:last_day]

            reports.append(base_transpose)

        except urllib.error.HTTPError:
            print(f"{url} could not be reached.")
            pass

    reserves = pd.concat(reports, sort=False)

    if update is not None:
        previous_data.append(reserves, sort=False)
        reserves = previous_data.loc[~previous_data.index.duplicated(keep="last")]

    reserves = reserves.apply(pd.to_numeric, errors="coerce")

    return reserves


def missing_reports(online, offline):

    missing_online = base_reports(online)

    missing_offline = []
    for file in offline:
        offline_aux = pd.read_csv(file, sep=" ", index_col=0)
        offline_aux.index = pd.to_datetime(offline_aux.index, errors="coerce")

        missing_offline.append(offline_aux)

    missing_offline = pd.concat(missing_offline, sort=False)

    missing = missing_online.append(missing_offline, sort=False)
    missing = missing.apply(pd.to_numeric, errors="coerce")

    return missing


def get_reserves_chg(files, online=None, offline=None, update=None, save=None):

    reserves = base_reports(files=files, update=update)

    if online is not None or offline is not None:
        missing = missing_reports(online=online, offline=offline)
        reserves = reserves.append(missing, sort=False)
        reserves.sort_index(inplace=True)

    colnames.set_colnames(reserves, area="International reserves", currency="USD", inf_adj="No",
                          index="No", seas_adj="NSA", ts_type="Flow", cumperiods=1)

    if save is not None:
        reserves.to_csv(save, sep=" ")

    return reserves


if __name__ == "__main__":
    int_reserves = get_reserves_chg(files=FILES, online=None, offline=None,
                                    update="../data/reserves_chg.csv", save="../data/reserves_chg.csv")
