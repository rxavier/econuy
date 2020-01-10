import os
import re
import tempfile

import pandas as pd
from pandas.tseries.offsets import MonthEnd
import patoolib
import requests
from bs4 import BeautifulSoup

from config import ROOT_DIR
from processing import colnames, update_revise

DATA_PATH = os.path.join(ROOT_DIR, "data")
update_threshold = 25
URL = ("https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/"
       "informacion-resultados-del-sector-publico")
SHEETS = {"Sector Público No Financiero":
          {"Name": "fiscal_nfps",
           "Colnames": ["Ingresos: SPNF", "Ingresos: Gobierno central", "Ingresos: DGI", "Ingresos: IRP",
                        "Ingresos: Comercio ext.", "Ingresos: Otros", "Ingresos: BPS",
                        "Ingresos: Res. primario corriente EEPP", "Egresos: Primarios SPNF",
                        "Egresos: Primarios corrientes GC-BPS", "Egresos: Remuneraciones", "Egresos: No personales",
                        "Egresos: Pasividades", "Egresos: Transferencias", "Egresos: Inversiones",
                        "Resultado: Primario intendencias", "Resultado: Primario BSE", "Resultado: Primario SPNF",
                        "Intereses: Totales", "Intereses: GC-BPS", "Intereses: EEPP", "Intereses: Intendencias",
                        "Intereses: BSE", "Resultado: Global SPNF"]},
          "Sector Público Consolidado":
          {"Name": "fiscal_gps",
           "Colnames": ["Resultado: Primario SPNF", "Intereses: SPNF", "Resultado: Global SPNF",
                        "Resultado: Primario BCU", "Intereses: BCU", "Resultado: Global BCU",
                        "Resultado: Primario SPC", "Resultado: Global SPC"]},
          "Gobierno Central - BPS":
          {"Name": "fiscal_gc-bps",
           "Colnames": ["Ingresos: GC-BPS", "Ingresos: GC", "Ingresos: Comercio ext.", "Ingresos: DGI",
                        "Ingresos: DGI bruto", "Ingresos: DGI CDI", "Ingresos: Loterías", "Ingresos: Venta energía",
                        "Ingresos: TGN/otros", "Ingresos: FIMTOP", "Ingresos: Aportes EEPP", "Ingresos: IRP",
                        "Ingresos: Rec. Lib. Disp", "Ingresos: BPS neto", "Ingresos: BPS bruto", "Ingresos: FSS",
                        "Ingresos: BPS CDI", "Ingresos: BPS otros", "Egresos: GC-BPS",
                        "Egresos: Remuneraciones", "Egresos: Remuneraciones adm. central",
                        "Egresos: Remuneraciones org. docentes", "Egresos: Remuneraciones retenc./otros",
                        "Egresos: Remuneraciones BPS", "Egresos: Pasividades", "Egresos: Pasividades Caja Policial",
                        "Egresos: Pasividades Caja Militar", "Egresos: Pasividades BPS", "Egresos: No personales",
                        "Egresos: No personales adm. central", "Egresos: No personales org. docentes",
                        "Egresos: No personales suministros", "Egresos: No personales plan emergencia",
                        "Egresos: No personales BPS", "Egresos: Transferencias", "Egresos: Transferencias GC",
                        "Egresos: Transferencias GC Entes", "Egresos: Transferencias GC deuda",
                        "Egresos: Transferencias GC otros org.", "Egresos: Transferencias GC rentas afectadas",
                        "Egresos: Transferencias BPS", "Egresos: Transferencias BPS enfermedad",
                        "Egresos: Transferencias BPS AFAM y otras prestaciones",
                        "Egresos: Transferencias BPS desempleo", "Egresos: Transferencias BPS 2",
                        "Egresos: Transferencias BPS -IRP/IRPF", "Egresos: Transferencias BPS AFAP",
                        "Egresos: Transferencias BPS otros", "Egresos: Transferencias otros",
                        "Egresos: Inversión", "Egresos: Inversión MTOP", "Egresos: Inversión MVOTMA",
                        "Egresos: Inversión Presidencia", "Egresos: Inversión org. docentes",
                        "Egresos: Inversión resto", "Intereses: Total", "Intereses: GC", "Intereses: BPS-FSS",
                        "Resultado: Global GC-BPS"]},
          "Empresas Públicas Consolidado":
          {"Name": "fiscal_pe",
           "Colnames": ["Ingresos", "Ingresos: Venta bienes y servicios", "Ingresos: Otros",
                        "Ingresos: Transferencias GC", "Egresos", "Egresos: Corrientes", "Egresos: Remuneraciones",
                        "Egresos: Compras bienes y servicios", "Egresos: Intereses", "Egresos: DGI", "Egresos: BPS",
                        "Egresos: No corrientes", "Egresos: Inversiones", "Egresos: Dividendo",
                        "Resultado: Global"]},
          "ANCAP":
          {"Name": "fiscal_ancap",
           "Colnames": ["Ingresos", "Ingresos: Venta bienes y servicios", "Ingresos: Otros",
                        "Ingresos: Transferencias GC", "Egresos", "Egresos: Corrientes", "Egresos: Remuneraciones",
                        "Egresos: Compras bienes y servicios", "Egresos: Intereses", "Egresos: DGI", "Egresos: BPS",
                        "Egresos: No corrientes", "Egresos: Inversiones", "Egresos: Var. stock petróleo",
                        "Egresos: Otros", "Egresos: Dividendo", "Resultado: Global"]},
          "ANTEL":
          {"Name": "fiscal_ancap",
           "Colnames": ["Ingresos", "Ingresos: Venta bienes y servicios", "Ingresos: Otros",
                        "Ingresos: Transferencias GC", "Egresos", "Egresos: Corrientes", "Egresos: Remuneraciones",
                        "Egresos: Compras bienes y servicios", "Egresos: Intereses", "Egresos: DGI", "Egresos: BPS",
                        "Egresos: No corrientes", "Egresos: Inversiones", "Egresos: Var. stock petróleo",
                        "Egresos: Otros", "Egresos: Dividendo", "Resultado: Global"]},
          "OSE":
          {"Name": "fiscal_ose",
           "Colnames": ["Ingresos", "Ingresos: Venta bienes y servicios", "Ingresos: Otros",
                        "Ingresos: Transferencias GC", "Egresos", "Egresos: Corrientes", "Egresos: Remuneraciones",
                        "Egresos: Compras bienes y servicios", "Egresos: Intereses", "Egresos: DGI", "Egresos: BPS",
                        "Egresos: No corrientes", "Egresos: Inversiones", "Egresos: Dividendo",
                        "Resultado: Global"]},
          "UTE":
          {"Name": "fiscal_ute",
           "Colnames": ["Ingresos", "Ingresos: Venta bienes y servicios", "Ingresos: Otros",
                        "Ingresos: Transferencias GC", "Egresos", "Egresos: Corrientes", "Egresos: Remuneraciones",
                        "Egresos: Compras bienes y servicios", "Egresos: Intereses", "Egresos: DGI", "Egresos: BPS",
                        "Egresos: No corrientes", "Egresos: Inversiones", "Egresos: Dividendo",
                        "Resultado: Global"]}}


def get(update=False, revise=0, save=False, force_update=False):
    response = requests.get(URL)
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all(href=re.compile("\\.rar$"))
    rar = links[0]["href"]

    temp_rar = tempfile.NamedTemporaryFile(suffix=".rar").name
    with open(temp_rar, "wb") as f:
        f.write(requests.get(rar).content)

    with tempfile.TemporaryDirectory() as temp_dir:
        patoolib.extract_archive(temp_rar, outdir=temp_dir)
        path = os.path.join(temp_dir, os.listdir(temp_dir)[0])

        output = {}
        with pd.ExcelFile(path) as xls:

            for sheet, metadata in SHEETS.items():

                if update is True:
                    update_path = os.path.join(DATA_PATH, metadata['Name'] + ".csv")
                    delta, previous_data = update_revise.check_modified(update_path)

                    if delta < update_threshold and force_update is False:
                        print(f"File in update path was modified within {update_threshold} day(s)."
                              f"Skipping download...")
                        output.update({metadata["Name"]: previous_data})
                        continue

                data = (pd.read_excel(xls, sheet_name=sheet).
                        dropna(axis=0, thresh=4).dropna(axis=1, thresh=4).
                        transpose().set_index(2, drop=True))
                data.columns = data.iloc[0]
                data = data[data.index.notnull()].rename_axis(None)
                data.index = data.index + MonthEnd(1)
                data.columns = metadata["Colnames"]

                if update is True:
                    data = update_revise.upd_rev(new_data=data, prev_data=previous_data, revise=revise)

                data = data.apply(pd.to_numeric, errors="coerce")
                colnames.set_colnames(data, area="Cuentas fiscales y deuda", currency="UYU", inf_adj="No",
                                      index="No", seas_adj="NSA", ts_type="Flujo", cumperiods=1)

                if save is True:
                    save_path = os.path.join(DATA_PATH, metadata["Name"] + ".csv")
                    data.to_csv(save_path, sep=" ")

                output.update({metadata["Name"]: data})

    return output


if __name__ == "__main__":
    fiscal_accounts = get(save=True)
