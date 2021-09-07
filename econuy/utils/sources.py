import datetime as dt

urls = {
    "call": {
        "dl": {"main": "https://web.bevsa.com.uy/Mercado/MercadoDinero/CallHistoricoDiario.aspx"},
        "source": {
            "direct": [],
            "indirect": [
                "https://web.bevsa.com.uy/Mercado/MercadoDinero/CallHistoricoDiario.aspx"
            ],
            "provider": ["BEVSA"],
        },
    },
    "bonds": {
        "dl": {
            "usd": "https://web.bevsa.com.uy/CurvasVectorPrecios/CurvasIndices/Indices/IndiceITBGL.aspx",
            "ui": "https://web.bevsa.com.uy/CurvasVectorPrecios/CurvasIndices/Indices/IndiceINDUI.aspx",
            "uyu": "https://web.bevsa.com.uy/CurvasVectorPrecios/CurvasIndices/Indices/IndiceITLUP.aspx",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://web.bevsa.com.uy/CurvasVectorPrecios/CurvasIndices/Indices/IndiceITBGL.aspx",
                "https://web.bevsa.com.uy/CurvasVectorPrecios/CurvasIndices/Indices/IndiceINDUI.aspx",
                "https://web.bevsa.com.uy/CurvasVectorPrecios/CurvasIndices/Indices/IndiceITLUP.aspx",
            ],
            "provider": ["BEVSA"],
        },
    },
    "deposits": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Servicios-Financieros-SSF/Series%20IF/Depositos.xlsx"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Servicios-Financieros-SSF/Series%20IF/Depositos.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Servicios-Financieros-SSF/Paginas/Series-estadisticas-Depositos.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "credit": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Servicios-Financieros-SSF/Series%20IF/Creditos.xlsx "
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Servicios-Financieros-SSF/Series%20IF/Creditos.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Servicios-Financieros-SSF/Paginas/Series-Estadisticas-Creditos.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "interest_rates": {
        "dl": {"main": "https://www.bcu.gub.uy/Servicios-Financieros-SSF/Series%20IF/tasas.xls"},
        "source": {
            "direct": ["https://www.bcu.gub.uy/Servicios-Financieros-SSF/Series%20IF/tasas.xls"],
            "indirect": [
                "https://www.bcu.gub.uy/Servicios-Financieros-SSF/Paginas/Series-Estadisticas-Tasas.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "cpi": {
        "dl": {
            "main": "https://www.ine.gub.uy/c/document_library/get_file?uuid=2e92084a-94ec-4fec-b5ca-42b40d5d2826&groupId=10181"
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=2e92084a-94ec-4fec-b5ca-42b40d5d2826&groupId=10181"
            ],
            "indirect": ["http://www.ine.gub.uy/web/guest/ipc-indice-de-precios-del-consumo"],
            "provider": ["INE"],
        },
    },
    "nxr_monthly": {
        "dl": {
            "main": "https://www.ine.gub.uy/c/document_library/get_file?uuid=3fbf4ffd-a829-420c-aca9-9f01ecd7919a&groupId=10181"
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=3fbf4ffd-a829-420c-aca9-9f01ecd7919a&groupId=10181"
            ],
            "indirect": ["http://www.ine.gub.uy/web/guest/cotizacion-de-monedas2"],
            "provider": ["INE"],
        },
    },
    "nxr_daily": {
        "dl": {
            "main": "https://www.bcu.gub.uy/_layouts/15/BCU.Cotizaciones/handler/FileHandler.ashx?op=downloadcotizacionesexcel&KeyValuePairs={%22KeyValuePairs%22:{%22Monedas%22:[{%22Val%22:%222224%22,%22Text%22:%22DLS.%20USA%20CABLE%22}],"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Cotizaciones.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "commodity_prices": {
        "dl": {
            "beef": "https://www.inac.uy/innovaportal/v/9799/10/innova.front/serie-semanal-ingreso-medio-de-exportacion---bovino-ovino-y-otros-productos",
            "pulp": f"https://www.insee.fr/en/statistiques/serie/telecharger/csv/010600339?ordre="
            f"antechronologique&transposition=donneescolonne&periodeDebut=1&anneeDebut=1990&periodeFin=4&anneeFin="
            f"{dt.datetime.now().year}",
            "soybean": "https://www.quandl.com/api/v3/datasets/CHRIS/CME_S1.csv?api_key=3TPxACcrxy9WsE871Lqe",
            "wheat": "https://www.quandl.com/api/v3/datasets/CHRIS/CME_W1.csv?api_key=3TPxACcrxy9WsE871Lqe",
            "milk1": "https://www.inale.org/estadisticas/",
            "milk2": "https://ec.europa.eu/info/sites/info/files/food-farming-fisheries/farming/documents/eu-milk-historical-price-series_en.xls",
            "imf": "https://www.imf.org/en/Research/commodity-prices",
        },
        "source": {
            "direct": [
                "https://www.inac.uy/innovaportal/v/9799/10/innova.front/serie-semanal-ingreso-medio-de-exportacion---bovino-ovino-y-otros-productos",
                f"https://www.insee.fr/en/statistiques/serie/telecharger/csv/010600339?ordre=antechronologique&"
                f"transposition=donneescolonne&periodeDebut=1&anneeDebut=1990&periodeFin=3&anneeFin="
                f"{dt.datetime.now().year}",
                "https://www.quandl.com/api/v3/datasets/CHRIS/CME_S1.csv?api_key=3TPxACcrxy9WsE871Lqe",
                "https://www.quandl.com/api/v3/datasets/CHRIS/CME_W1.csv?api_key=3TPxACcrxy9WsE871Lqe",
                "https://ec.europa.eu/info/sites/info/files/food-farming-fisheries/farming/documents/eu-milk-historical-price-series_en.xls",
            ],
            "indirect": [
                "https://www.inac.uy/innovaportal/v/5541/10/innova.front/precios",
                "https://www.insee.fr/fr/statistiques/serie/010600339",
                "https://www.quandl.com/data/CHRIS/CME_S1-Soybean-Futures-Continuous-Contract-1-S1-Front-Month",
                "https://www.quandl.com/data/CHRIS/CME_W1-Wheat-Futures-Continuous-Contract-1-W1-Front-Month",
                "https://www.inale.org/estadisticas/",
                "https://ec.europa.eu/info/food-farming-fisheries/farming/facts-and-figures/markets/overviews/market-observatories/milk",
                "https://www.imf.org/en/Research/commodity-prices",
            ],
            "provider": ["econuy en base a INAC, INSEE, Quandl, INALE, Comisión Europea y FMI"],
        },
    },
    "commodity_index": {
        "dl": {},
        "source": {
            "direct": [
                "https://www.inac.uy/innovaportal/v/9799/10/innova.front/serie-semanal-ingreso-medio-de-exportacion---bovino-ovino-y-otros-productos",
                f"https://www.insee.fr/en/statistiques/serie/telecharger/010600339?ordre=antechronologique&"
                f"transposition=donneescolonne&periodeDebut=1&anneeDebut=1990&periodeFin=11&anneeFin="
                f"{dt.datetime.now().year}",
                "https://www.quandl.com/api/v3/datasets/CHRIS/CME_S1.csv?api_key=3TPxACcrxy9WsE871Lqe",
                "https://www.quandl.com/api/v3/datasets/CHRIS/CME_W1.csv?api_key=3TPxACcrxy9WsE871Lqe",
                "https://ec.europa.eu/info/sites/info/files/food-farming-fisheries/farming/documents/eu-milk-historical-price-series_en.xls",
            ],
            "indirect": [
                "https://www.inac.uy/innovaportal/v/5541/10/innova.front/precios",
                "https://www.insee.fr/fr/statistiques/serie/010600339",
                "https://www.quandl.com/data/CHRIS/CME_S1-Soybean-Futures-Continuous-Contract-1-S1-Front-Month",
                "https://www.quandl.com/data/CHRIS/CME_W1-Wheat-Futures-Continuous-Contract-1-W1-Front-Month",
                "https://www.inale.org/estadisticas/",
                "https://ec.europa.eu/info/food-farming-fisheries/farming/facts-and-figures/markets/overviews/market-observatories/milk",
                "https://www.imf.org/en/Research/commodity-prices",
                "https://comtrade.un.org/",
            ],
            "provider": [
                "econuy en base a INAC, INSEE, Quandl, INALE, Comisión Europea, FMI y Naciones Unidas"
            ],
        },
    },
    "balance_gps": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
            ],
            "provider": ["MEF"],
        },
    },
    "balance_nfps": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
            ],
            "provider": ["MEF"],
        },
    },
    "balance_cg-bps": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
            ],
            "provider": ["MEF"],
        },
    },
    "balance_pe": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
            ],
            "provider": ["MEF"],
        },
    },
    "balance_ancap": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
            ],
            "provider": ["MEF"],
        },
    },
    "balance_ute": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
            ],
            "provider": ["MEF"],
        },
    },
    "balance_antel": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
            ],
            "provider": ["MEF"],
        },
    },
    "balance_ose": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
            ],
            "provider": ["MEF"],
        },
    },
    "balance_summary": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos/informacion-resultados-del-sector-publico"
            ],
            "provider": ["MEF"],
        },
    },
    "tax_revenue": {
        "dl": {
            "main": "https://www.dgi.gub.uy/wdgi/afiledownload?2,4,865,O,S,0,19353%3BS%3B100%3B108,",
            "pdfs": "https://raw.githubusercontent.com/rxavier/econuy-extras/main/econuy_extras/retrieval/taxes_pdf.csv",
        },
        "source": {
            "direct": [
                "https://www.dgi.gub.uy/wdgi/afiledownload?2,4,865,O,S,0,19353%3BS%3B100%3B108,"
            ],
            "indirect": [
                "https://www.dgi.gub.uy/wdgi/page?2,principal,dgi--datos-y-series-estadisticas--serie-de-datos--recaudacion-anual-y-mensual-por-impuesto,O,es,0,",
                f"https://www.dgi.gub.uy/wdgi/page?2,principal,dgi--datos-y-series-estadisticas--informes-mensuales-de-la-recaudacion-{dt.datetime.now().year},O,es,0,",
            ],
            "provider": ["DGI"],
        },
    },
    "public_debt_gps": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Endeudamiento%20Pblico/resdspg.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Endeudamiento%20Pblico/resdspg.xls"
            ],
            "indirect": ["https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Default.aspx"],
            "provider": ["BCU"],
        },
    },
    "public_debt_nfps": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Endeudamiento%20Pblico/resdspg.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Endeudamiento%20Pblico/resdspg.xls"
            ],
            "indirect": ["https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Default.aspx"],
            "provider": ["BCU"],
        },
    },
    "public_debt_cb": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Endeudamiento%20Pblico/resdspg.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Endeudamiento%20Pblico/resdspg.xls"
            ],
            "indirect": ["https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Default.aspx"],
            "provider": ["BCU"],
        },
    },
    "net_public_debt": {
        "dl": {},
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Endeudamiento%20Pblico/resdspg.xls",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/MonedayCredito/Activos-de-Reserva/reservas.xls",
            ],
            "indirect": ["https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Default.aspx"],
            "provider": ["econuy en base a BCU"],
        },
    },
    "diesel": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-industria-energia-mineria/datos-y-estadisticas/datos/series-estadisticas-petroleo-derivados"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-industria-energia-mineria/datos-y-estadisticas/datos/series-estadisticas-petroleo-derivados"
            ],
            "provider": ["MIEM"],
        },
    },
    "gasoline": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-industria-energia-mineria/datos-y-estadisticas/datos/series-estadisticas-petroleo-derivados"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-industria-energia-mineria/datos-y-estadisticas/datos/series-estadisticas-petroleo-derivados"
            ],
            "provider": ["MIEM"],
        },
    },
    "electricity": {
        "dl": {
            "main": "https://www.gub.uy/ministerio-industria-energia-mineria/datos-y-estadisticas/datos/series-estadisticas-energia-electrica"
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-industria-energia-mineria/datos-y-estadisticas/datos/series-estadisticas-energia-electrica"
            ],
            "provider": ["MIEM"],
        },
    },
    "labor_rates": {
        "dl": {
            "main": "https://www.ine.gub.uy/c/document_library/get_file?uuid=50ae926c-1ddc-4409-afc6-1fecf641e3d0&groupId=10181",
            "missing": "https://docs.google.com/spreadsheets/d/1amqU3fUSok0kDB_LYvCZlBtVpkZeTK5vW5O8bShlOtw/export?format=xlsx&authuser=0",
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=50ae926c-1ddc-4409-afc6-1fecf641e3d0&groupId=10181"
            ],
            "indirect": ["http://www.ine.gub.uy/web/guest/actividad-empleo-y-desempleo"],
            "provider": ["INE"],
        },
    },
    "nominal_wages": {
        "dl": {
            "historical": "https://www.ine.gub.uy/c/document_library/get_file?uuid=a76433b7-5fba-40fc-9958-dd913338e989&groupId=10181",
            "current": "https://www.ine.gub.uy/c/document_library/get_file?uuid=97f07fd8-9410-476e-bf81-e6b1c11467ef&groupId=10181",
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=a76433b7-5fba-40fc-9958-dd913338e989&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=97f07fd8-9410-476e-bf81-e6b1c11467ef&groupId=10181",
            ],
            "indirect": ["http://www.ine.gub.uy/web/guest/ims-indice-medio-de-salarios"],
            "provider": ["INE"],
        },
    },
    "hours_worked": {
        "dl": {
            "main": "https://www.ine.gub.uy/c/document_library/get_file?uuid=167e0db0-95ca-45d2-8e81-3b8c5bb8f9ee&groupId=10181",
            "historical": "https://www.ine.gub.uy/c/document_library/get_file?uuid=73ac6ede-8452-48b8-ad32-993d3b047091&groupId=10181",
            "missing": "https://docs.google.com/spreadsheets/d/1amqU3fUSok0kDB_LYvCZlBtVpkZeTK5vW5O8bShlOtw/export?format=xlsx&authuser=0",
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=167e0db0-95ca-45d2-8e81-3b8c5bb8f9ee&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=73ac6ede-8452-48b8-ad32-993d3b047091&groupId=10181",
            ],
            "indirect": ["http://www.ine.gub.uy/web/guest/actividad-empleo-y-desempleo"],
            "provider": ["INE"],
        },
    },
    "income_household": {
        "dl": {
            "main": "https://www.ine.gub.uy/c/document_library/get_file?uuid=40bd0267-3922-478d-8bc0-252f508a72fe&groupId=10181",
            "missing": "https://docs.google.com/spreadsheets/d/1amqU3fUSok0kDB_LYvCZlBtVpkZeTK5vW5O8bShlOtw/export?format=xlsx&authuser=0",
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=40bd0267-3922-478d-8bc0-252f508a72fe&groupId=10181"
            ],
            "indirect": [
                "http://www.ine.gub.uy/web/guest/gastos-e-ingresos-de-las-personas-y-los-hogares"
            ],
            "provider": ["INE"],
        },
    },
    "income_capita": {
        "dl": {
            "main": "https://www.ine.gub.uy/c/document_library/get_file?uuid=ca57dafa-8091-4c2f-8df8-7b8445859b93&groupId=10181",
            "missing": "https://docs.google.com/spreadsheets/d/1amqU3fUSok0kDB_LYvCZlBtVpkZeTK5vW5O8bShlOtw/export?format=xlsx&authuser=0",
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=ca57dafa-8091-4c2f-8df8-7b8445859b93&groupId=10181"
            ],
            "indirect": [
                "http://www.ine.gub.uy/web/guest/gastos-e-ingresos-de-las-personas-y-los-hogares"
            ],
            "provider": ["INE"],
        },
    },
    "natacc_ind_con_nsa": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/1.%20Actividades_K.xlsx"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/1.%20Actividades_K.xlsx"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-industrias.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "natacc_ind_cur_nsa": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/2.%20Actividades_C.xlsx"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/2.%20Actividades_C.xlsx"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-industrias.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "natacc_gas_con_nsa": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/1.%20Gasto_K.xlsx"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/1.%20Gasto_K.xlsx"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-Componente-del-gasto.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "natacc_gas_cur_nsa": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/2.%20Gasto_C.xlsx"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/2.%20Gasto_C.xlsx"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-Componente-del-gasto.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "gdp_con_idx_sa": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/5.%20Desestacionalizado.xlsx"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/5.%20Desestacionalizado.xlsx"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-industrias.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "natacc_ind_con_nsa_long": {
        "dl": {
            "2005": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_132t.xls",
            "1983": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/cuadro_42t83.xls",
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_132t.xls",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/cuadro_42t83.xls"
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/1.%20Actividades_K.xlsx",
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/presentacion05t.htm",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/PRESENTACION83t.HTM",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-industrias.aspx",
            ],
            "provider": ["BCU"],
        },
    },
    "natacc_gas_con_nsa_long": {
        "dl": {
            "2005": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_104t.xls",
            "1983": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/cuadro_45t83.xls",
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_104t.xls",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/cuadro_45t83.xls"
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/1.%20Gasto_K.xlsx",
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/presentacion05t.htm",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/PRESENTACION83t.HTM",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-Componente-del-gasto.aspx",
            ],
            "provider": ["BCU"],
        },
    },
    "gdp_con_idx_sa_long": {
        "dl": {
            "2005": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_133t.xls",
            "1983": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/cuadro_55t83.xls",
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_133t.xls",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/cuadro_55t83.xls",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/5.%20Desestacionalizado.xlsx",
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/presentacion05t.htm",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/PRESENTACION83t.HTM",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-industrias.aspx",
            ],
            "provider": ["BCU"],
        },
    },
    "gdp_con_nsa_long": {
        "dl": {
            "1997": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/estudios/Documents/pib_k_backcasting.xlsx",
            "1983": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/cuadro_42t83.xls",
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/cuadro_42t83.xls",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/estudios/Documents/pib_k_backcasting.xlsx",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/5.%20Desestacionalizado.xlsx",
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/estudios/Paginas/Detalle.aspx?itm=49",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/base_1983/PRESENTACION83t.HTM",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-industrias.aspx",
            ],
            "provider": ["BCU"],
        },
    },
    "gdp_cur_nsa_long": {
        "dl": {
            "1997": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/estudios/Documents/pib_c_backcasting.xlsx",
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/estudios/Documents/pib_c_backcasting.xlsx",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/5.%20Desestacionalizado.xlsx",
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/estudios/Paginas/Detalle.aspx?itm=49",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Series-Estadisticas-del-PIB-por-industrias.aspx",
            ],
            "provider": ["BCU"],
        },
    },
    "reserves": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/MonedayCredito/Activos-de-Reserva/reservas.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/MonedayCredito/Activos-de-Reserva/reservas.xls"
            ],
            "indirect": ["https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Default.aspx"],
            "provider": ["BCU"],
        },
    },
    "industrial_production": {
        "dl": {
            "main": "https://www.ine.gub.uy/c/document_library/get_file?uuid=e5ee6e11-601f-45ff-9335-68cd5191fa39&groupId=10181",
            "weights": "https://raw.githubusercontent.com/rxavier/econuy-extras/main/econuy_extras/manual_data/industrial_production_2018_weights.csv",
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=e5ee6e11-601f-45ff-9335-68cd5191fa39&groupId=10181"
            ],
            "indirect": ["http://www.ine.gub.uy/web/guest/industria-manufacturera"],
            "provider": ["INE"],
        },
    },
    "cattle": {
        "dl": {
            "main": "https://www.inac.uy/innovaportal/v/11998/10/innova.front/serie-semanal-faena---bovinos-y-ovinos"
        },
        "source": {
            "direct": [
                "https://www.inac.uy/innovaportal/v/11998/10/innova.front/serie-semanal-faena---bovinos-y-ovinos"
            ],
            "indirect": ["https://www.inac.uy/innovaportal/v/5539/10/innova.front/faena"],
            "provider": ["INAC"],
        },
    },
    "milk": {
        "dl": {"main": "https://www.inale.org/estadisticas/remision-a-planta/"},
        "source": {
            "direct": [],
            "indirect": ["https://www.inale.org/estadisticas/remision-a-planta/"],
            "provider": ["INALE"],
        },
    },
    "cement": {
        "dl": {"main": "http://www.ciu.com.uy/innovaportal/file/83062/1/cemento-web.xlsx"},
        "source": {
            "direct": ["http://www.ciu.com.uy/innovaportal/file/83062/1/cemento-web.xlsx"],
            "indirect": [
                "http://www.ciu.com.uy/innovaportal/v/83062/9/innova.front/series-de-ventas-mensuales-por-destino.html"
            ],
            "provider": ["AFCPU y CIU"],
        },
    },
    "consumer_confidence": {
        "dl": {
            "main": "https://ucu.edu.uy/sites/default/files/facultad/fce/i_competitividad/serie_icc_-.xlsx"
        },
        "source": {
            "direct": [
                "https://ucu.edu.uy/sites/default/files/facultad/fce/i_competitividad/serie_icc_-.xlsx"
            ],
            "indirect": ["https://ucu.edu.uy/es/icc"],
            "provider": ["UCU"],
        },
    },
    "sovereign_risk": {
        "dl": {
            "historical": "https://www4.rafap.com.uy/internet/images/indicadores/UBI_Historico.xls",
            "current": "https://www4.rafap.com.uy/internet/servlet/hextubicd",
        },
        "source": {
            "direct": [],
            "indirect": ["https://www.rafap.com.uy/mvdcms/Institucional/UBI-uc89"],
            "provider": ["República AFAP"],
        },
    },
    "reserves_changes": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Informe%20Diario%20Pasivos%20Monetarios/infd_",
            "missing": "https://docs.google.com/spreadsheets/d/1tXwv8SaigbBrfBSSCVGBjSs88f3dgTq4nIANPn7vjYI/export?format=xlsx&authuser=0",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Informe-Diario-Pasivos-Monetarios.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "rxr_official": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Indice_Cambio_Real/TCRE.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Indice_Cambio_Real/TCRE.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Cambio-Real-Efectivo.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "rxr_custom": {
        "dl": {},
        "source": {
            "direct": [],
            "indirect": [
                "https://data.imf.org/?sk=4c514d48-b6ba-49ed-8ab9-52b0c1a0179b",
                "http://www.ine.gub.uy/web/guest/ipc-indice-de-precios-del-consumo",
                "http://www.ine.gub.uy/web/guest/cotizacion-de-monedas2",
                "https://www.ambito.com/contenidos/dolar-informal.html",
                "http://www.bcra.gov.ar/PublicacionesEstadisticas/Principales_variables_datos.asp?serie=7931&detalle=Inflaci%F3n%20mensual%A0(variaci%F3n%20en%20%)",
                "http://www.inflacionverdadera.com/argentina/",
                "http://www.ipeadata.gov.br/Default.aspx",
                "https://www.ibge.gov.br/estatisticas/economicas/precos-e-custos/9256-indice-nacional-de-precos-ao-consumidor-amplo.html?=&t=o-que-e",
            ],
            "provider": [
                "econuy en base a INE, BCRA, Ipea, IBGE, FMI, Ámbito e Inflación Verdadera"
            ],
        },
    },
    "trade_x_prod_val": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/exp_ciiu_val.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/exp_ciiu_val.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_x_prod_vol": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_exp_ciiu_ivf.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_exp_ciiu_ivf.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_x_prod_pri": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_exp_ciiu_ip.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_exp_ciiu_ip.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_x_dest_val": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/exp_pais_val.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/exp_pais_val.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_x_dest_vol": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_exp_pais_ivf.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_exp_pais_ivf.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_x_dest_pri": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_exp_pais_ip.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_exp_pais_ip.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_m_sect_val": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/imp_gce_val.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/imp_gce_val.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_m_sect_vol": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_imp_gce_ivf.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_imp_gce_ivf.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_m_sect_pri": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_imp_gce_ip.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_imp_gce_ip.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_m_orig_val": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/imp_pais_val.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/imp_pais_val.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_m_orig_vol": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_imp_pais_ivf.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_imp_pais_ivf.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "trade_m_orig_pri": {
        "dl": {
            "main": "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_imp_pais_ip.xls"
        },
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_imp_pais_ip.xls"
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["BCU"],
        },
    },
    "cpi_measures": {
        "dl": {
            "2010-14": "https://www.ine.gub.uy/c/document_library/get_file?uuid=668d4f77-74d8-46ba-8360-77bd867996df&groupId=10181",
            "2015-": "https://www.ine.gub.uy/c/document_library/get_file?uuid=ad969d52-cebc-4b40-9a1f-34ce277e463e&groupId=10181",
            "1997": "https://www.ine.gub.uy/c/document_library/get_file?uuid=1cd81500-420a-44d0-ae4d-0add9d913107&groupId=10181",
            "1997_weights": "https://docs.google.com/spreadsheets/d/1gSQdp6b97udmki0DZBndhLajLv6uGDX7kYb66BUswj8/export#gid=0",
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=668d4f77-74d8-46ba-8360-77bd867996df&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=ad969d52-cebc-4b40-9a1f-34ce277e463e&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=1cd81500-420a-44d0-ae4d-0add9d913107&groupId=10181",
                "http://www.ine.gub.uy/c/document_library/get_file?uuid=5f2e75d2-5df6-48da-978d-e7930d47c037&groupId=10181",
            ],
            "indirect": ["http://www.ine.gub.uy/web/guest/ipc-indice-de-precios-del-consumo"],
            "provider": ["econuy en base a INE"],
        },
    },
    "balance_fss": {
        "dl": {},
        "source": {
            "direct": [],
            "indirect": [
                "https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/datos"
            ],
            "provider": ["econuy en base a MEF"],
        },
    },
    "labor_rates_people": {
        "dl": {
            "act_5000": "https://www.ine.gub.uy/c/document_library/get_file?uuid=b51d8104-d367-4d0f-828b-189eefc29de2&groupId=10181",
            "emp_5000": "https://www.ine.gub.uy/c/document_library/get_file?uuid=0902797e-e588-4da3-91cd-153c4d1d28a5&groupId=10181",
            "des_5000": "https://www.ine.gub.uy/c/document_library/get_file?uuid=d1434567-3da4-4321-9341-4fb3d8b6a09c&groupId=10181",
            "population": "https://www.ine.gub.uy/c/document_library/get_file?uuid=2a5c1e6e-b02f-4a63-963f-925edea7c17e&groupId=10181",
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=50ae926c-1ddc-4409-afc6-1fecf641e3d0&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=b51d8104-d367-4d0f-828b-189eefc29de2&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=0902797e-e588-4da3-91cd-153c4d1d28a5&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=d1434567-3da4-4321-9341-4fb3d8b6a09c&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=2a5c1e6e-b02f-4a63-963f-925edea7c17e&groupId=10181",
            ],
            "indirect": [
                "http://www.ine.gub.uy/web/guest/actividad-empleo-y-desempleo",
                "http://www.ine.gub.uy/estimaciones-y-proyecciones",
            ],
            "provider": ["econuy en base a INE"],
        },
    },
    "real_wages": {
        "dl": {},
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=a76433b7-5fba-40fc-9958-dd913338e989&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=97f07fd8-9410-476e-bf81-e6b1c11467ef&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=97f07fd8-9410-476e-bf81-e6b1c11467ef&groupId=10181",
            ],
            "indirect": [
                "http://www.ine.gub.uy/web/guest/ims-indice-medio-de-salarios",
                "http://www.ine.gub.uy/web/guest/ipc-indice-de-precios-del-consumo",
            ],
            "provider": ["econuy en base a INE"],
        },
    },
    "trade_balance": {
        "dl": {},
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/exp_pais_val.xls",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/imp_pais_val.xls",
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["econuy en base a BCU"],
        },
    },
    "terms_of_trade": {
        "dl": {},
        "source": {
            "direct": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_exp_pais_ip.xls",
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/web_imp_pais_ip.xls",
            ],
            "indirect": [
                "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Intercambio-Comercial-.aspx"
            ],
            "provider": ["econuy en base a BCU"],
        },
    },
    "core_industrial": {
        "dl": {
            "2018": "https://www.ine.gub.uy/c/document_library/get_file?uuid=e5ee6e11-601f-45ff-9335-68cd5191fa39&groupId=10181",
            "2006": "https://www.ine.gub.uy/c/document_library/get_file?uuid=8e08c0dc-acc2-44f7-b302-daa32e0b978b&groupId=10181",
        },
        "source": {
            "direct": [
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=e5ee6e11-601f-45ff-9335-68cd5191fa39&groupId=10181",
                "https://www.ine.gub.uy/c/document_library/get_file?uuid=17185e91-4198-4449-877a-59d69479e45d&groupId=10181",
            ],
            "indirect": ["http://www.ine.gub.uy/web/guest/industria-manufacturera"],
            "provider": ["econuy en base a INE"],
        },
    },
    "global_gdp": {
        "dl": {
            "fred": "https://api.stlouisfed.org/fred/series/observations?series_id=",
            "chn_oecd": "https://stats.oecd.org/SDMX-JSON/data/QNA/CHN.B1_GE.GYSA+GPSA.Q/all?startTime=1960-Q1&endTime=",
            "chn_obs": "https://docs.google.com/spreadsheets/d/1JwHqYSyBCOj9E60X0JCnPIn4WEzfu6rcPN8Xg76AhyU/export?format=xlsx&authuser=0",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://fred.stlouisfed.org/",
                "https://stats.oecd.org/Index.aspx",
                "https://data.stats.gov.cn/english/easyquery.htm?cn=B01",
            ],
            "provider": ["econuy en base a FRB St. Louis, OECD y NBS China"],
        },
    },
    "global_stocks": {
        "dl": {
            "spy": f"https://query1.finance.yahoo.com/v7/finance/download/%5EGSPC?period1=-1325635200&period2={dt.datetime.now().timestamp().__round__()}&interval=1d&events=history&includeAdjustedClose=true",
            "n100": f"https://query1.finance.yahoo.com/v7/finance/download/%5EN100?period1=946598400&period2={dt.datetime.now().timestamp().__round__()}&interval=1d&events=history&includeAdjustedClose=true",
            "sse": f"https://query1.finance.yahoo.com/v7/finance/download/000001.SS?period1=867715200&period2={dt.datetime.now().timestamp().__round__()}&interval=1d&events=history&includeAdjustedClose=true",
            "nikkei": f"https://query1.finance.yahoo.com/v7/finance/download/%5EN225?period1=-157507200&period2={dt.datetime.now().timestamp().__round__()}&interval=1d&events=history&includeAdjustedClose=true",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://finance.yahoo.com/quote/%5EGSPC/history/",
                "https://finance.yahoo.com/quote/%5EN100/history/",
                "https://finance.yahoo.com/quote/000001.SS/history/",
                "https://finance.yahoo.com/quote/%5EN225/history/",
            ],
            "provider": ["Yahoo Finance"],
        },
    },
    "global_policy_rates": {
        "dl": {
            "main": "https://www.bis.org/statistics/full_webstats_cbpol_d_dataflow_csv_row.zip"
        },
        "source": {
            "direct": [
                "https://www.bis.org/statistics/full_webstats_cbpol_d_dataflow_csv_row.zip"
            ],
            "indirect": ["https://www.bis.org/statistics/cbpol.htm"],
            "provider": ["BIS"],
        },
    },
    "global_long_rates": {
        "dl": {
            "main": "https://www.investing.com/instruments/HistoricalDataAjax",
            "fred": "https://api.stlouisfed.org/fred/series/observations?series_id=",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://fred.stlouisfed.org/series/DGS10",
                "https://www.investing.com/rates-bonds/government-bond-spreads",
            ],
            "provider": ["FRB St. Louis", "Investing.com"],
        },
    },
    "global_nxr": {
        "dl": {
            "dollar": f"https://query1.finance.yahoo.com/v7/finance/download/DX-Y.NYB?period1=31795200&period2={dt.datetime.now().timestamp().__round__()}&interval=1d&events=history&includeAdjustedClose=true",
            "eur": f"https://query1.finance.yahoo.com/v7/finance/download/USDEUR=X?period1=1070150400&period2={dt.datetime.now().timestamp().__round__()}&interval=1d&events=history&includeAdjustedClose=true",
            "jpy": f"https://query1.finance.yahoo.com/v7/finance/download/USDJPY=X?period1=846547200&period2={dt.datetime.now().timestamp().__round__()}&interval=1d&events=history&includeAdjustedClose=true",
            "cny": f"https://query1.finance.yahoo.com/v7/finance/download/USDCNY=X?period1=991180800&period2={dt.datetime.now().timestamp().__round__()}&interval=1d&events=history&includeAdjustedClose=true",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://finance.yahoo.com/quote/DX-Y.NYB?/history/",
                "https://finance.yahoo.com/quote/USDEUR=X/history/",
                "https://finance.yahoo.com/quote/USDJPY=X/history/",
                "https://finance.yahoo.com/quote/USDCNY=X/history/",
            ],
            "provider": ["Yahoo Finance"],
        },
    },
    "regional_embi_spreads": {
        "dl": {
            "global": "https://cdn.bancentral.gov.do/documents/entorno-internacional/documents/Serie_Historica_Spread_del_EMBI.xlsx",
            "brasil": f"https://mercados.ambito.com//riesgopaisinternacional/brasil/historico-general/01-01-1990/{dt.datetime.now().strftime('%d-%m-%Y')}",
            "argentina": f"https://mercados.ambito.com//riesgopais/historico-general/01-01-1990/{dt.datetime.now().strftime('%d-%m-%Y')}",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.ambito.com/contenidos/riesgo-pais.html",
                "https://www.bancentral.gov.do/a/d/2585-entorno-internacional",
            ],
            "provider": ["Ámbito, BCRD"],
        },
    },
    "regional_embi_yields": {
        "source": {
            "direct": [],
            "indirect": [
                "https://fred.stlouisfed.org/series/DGS10",
                "https://www.ambito.com/contenidos/riesgo-pais.html",
                "https://www.bancentral.gov.do/a/d/2585-entorno-internacional",
            ],
            "provider": ["econuy en base a FRB St. Louis, Ámbito y BCRD"],
        }
    },
    "regional_gdp": {
        "dl": {
            "arg_new": "https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-9-47",
            "arg_old": "https://www.indec.gob.ar/ftp/nuevaweb/cuadros/17/cuadro12.xls",
            "bra": "https://ftp.ibge.gov.br/Contas_Nacionais/Contas_Nacionais_Trimestrais/Tabelas_Completas/Tab_Compl_CNT.zip",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-9-47",
                "https://www.indec.gob.ar/indec/web/Institucional-Indec-InformacionDeArchivo-5",
                "https://sidra.ibge.gov.br/tabela/1621#/n1/all/v/all/p/all/c11255/90687,90691,90696,90705,90707,93404,93405,93406,93407,93408/d/v584%201/l/v,c11255,t+p/resultado",
            ],
            "provider": ["INDEC", "IBGE"],
        },
    },
    "regional_cpi": {
        "dl": {
            "ar": "http://www.bcra.gov.ar/PublicacionesEstadisticas/Principales_variables_datos.asp",
            "ar_payload": f"fecha_desde=1970-01-01&fecha_hasta={dt.datetime.now().strftime('%Y-%m-%d')}&B1=Enviar&primeravez=1&fecha_desde=19600101&fecha_hasta={dt.datetime.now().strftime('%Y%m%d')}&serie=7931&serie1=0&serie2=0&serie3=0&serie4=0&detalle=Inflaci%F3n+mensual%A0%28variaci%F3n+en+%29",
            "ar_unofficial": "http://www.inflacionverdadera.com/Argentina_inflation.xls",
            "bra": f"https://servicodados.ibge.gov.br/api/v1/conjunturais?&d=s&user=ibge&t=1737&v=63&p=197001-{dt.datetime.now().strftime('%Y%m')}&ng=1(1)&c=",
        },
        "source": {
            "direct": [],
            "indirect": [
                "http://www.bcra.gov.ar/PublicacionesEstadisticas/Principales_variables_datos.asp",
                "http://www.inflacionverdadera.com/",
                "https://www.ibge.gov.br/estatisticas/economicas/precos-e-custos/9256-indice-nacional-de-precos-ao-consumidor-amplo.html?=&t=o-que-e",
            ],
            "provider": ["econuy en base a BRCA, Inflación Verdadera e IBGE"],
        },
    },
    "regional_nxr": {
        "dl": {
            "ar": f"https://mercados.ambito.com/dolar/oficial/historico-general/09-04-2002/{dt.datetime.now().strftime('%d-%m-%Y')}",
            "ar_unofficial": f"https://mercados.ambito.com/dolar/informal/historico-general/09-04-2002/{dt.datetime.now().strftime('%d-%m-%Y')}",
            "bra": "http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='GM366_ERV366')",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.ambito.com/contenidos/dolar-oficial.html",
                "https://www.ambito.com/contenidos/dolar-informal.html",
                "http://www.ipeadata.gov.br/Default.aspx",
            ],
            "provider": ["Ámbito", "Ipea"],
        },
    },
    "regional_policy_rates": {
        "dl": {
            "main": "https://www.bis.org/statistics/full_webstats_cbpol_d_dataflow_csv_row.zip"
        },
        "source": {
            "direct": [
                "https://www.bis.org/statistics/full_webstats_cbpol_d_dataflow_csv_row.zip"
            ],
            "indirect": ["https://www.bis.org/statistics/cbpol.htm"],
            "provider": ["BIS"],
        },
    },
    "regional_monthly_gdp": {
        "dl": {
            "arg": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls",
            "bra": "http://api.bcb.gov.br/dados/serie/bcdata.sgs.24364/dados?formato=csv",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-9-48",
                "https://dadosabertos.bcb.gov.br/dataset/24364-indice-de-atividade-economica-do-banco-central-ibc-br---com-ajuste-sazonal",
            ],
            "provider": ["INDEC", "BCB"],
        },
    },
    "regional_stocks": {
        "dl": {
            "arg": "https://www.investing.com/instruments/HistoricalDataAjax",
            "bra": f"https://query1.finance.yahoo.com/v7/finance/download/%5EBVSP?period1=735868800&period2={dt.datetime.now().timestamp().__round__()}&interval=1d&events=history&includeAdjustedClose=true",
        },
        "source": {
            "direct": [],
            "indirect": [
                "https://www.investing.com/indices/merv-historical-data",
                "https://finance.yahoo.com/quote/%5EBVSP/history?period1=735868800&period2=1607212800&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true",
            ],
            "provider": ["Investing.com", "Yahoo Finance"],
        },
    },
    "regional_rxr": {
        "dl": {},
        "source": {
            "direct": [],
            "indirect": [
                "https://data.imf.org/?sk=4c514d48-b6ba-49ed-8ab9-52b0c1a0179b",
                "https://www.ambito.com/contenidos/dolar-informal.html",
                "http://www.bcra.gov.ar/PublicacionesEstadisticas/Principales_variables_datos.asp?serie=7931&detalle=Inflaci%F3n%20mensual%A0(variaci%F3n%20en%20%)",
                "http://www.inflacionverdadera.com/argentina/",
                "http://www.ipeadata.gov.br/Default.aspx",
                "https://www.ibge.gov.br/estatisticas/economicas/precos-e-custos/9256-indice-nacional-de-precos-ao-consumidor-amplo.html?=&t=o-que-e",
            ],
            "provider": ["econuy en base a BCRA, Ámbito, Ipea, IBGE, FMI e Inflación Verdadera"],
        },
    },
}
