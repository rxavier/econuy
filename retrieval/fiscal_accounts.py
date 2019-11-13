import os
import re
import tempfile

import pandas as pd
import patoolib
import requests
from bs4 import BeautifulSoup

from config import ROOT_DIR
from processing import colnames, update_revise

DATA_PATH = os.path.join(ROOT_DIR, "data")
URL = ("https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/"
       "informacion-resultados-del-sector-publico")
SHEETS = {"Sector Público No Financiero": "fiscal_nfps", "Sector Público Consolidado": "fiscal_gps",
          "Gobierno Central - BPS": "fiscal_gc-bps", "Empresas Públicas Consolidado": "fiscal_pe",
          "ANCAP": "fiscal_ancap", "ANTEL": "fiscal_antel", "OSE": "fiscal_ose", "UTE": "fiscal_ute"}


def get(update=None, revise=0, save=None):

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

            for sheet, filename in SHEETS.items():

                data = (pd.read_excel(xls, sheet_name=sheet).
                        dropna(axis=0, thresh=4).dropna(axis=1, thresh=4).
                        transpose().set_index(2, drop=True))
                data.columns = data.iloc[0]
                data = data[data.index.notnull()].rename_axis(None)

                if update is True:
                    update_path = os.path.join(DATA_PATH, filename + ".csv")
                    data = update_revise.upd_rev(data, prev_data=update_path, revise=revise)

                data = data.apply(pd.to_numeric, errors="coerce")
                colnames.set_colnames(data, area="Fiscal accounts", currency="UYU", inf_adj="No",
                                      index="No", seas_adj="NSA", ts_type="Flow", cumperiods=1)

                if save is True:
                    save_path = os.path.join(DATA_PATH, filename + ".csv")
                    data.to_csv(save_path, sep=" ")

                output.update({sheet: data})

    return output


if __name__ == "__main__":
    fiscal_accounts = get(update=True, revise=6, save=True)
