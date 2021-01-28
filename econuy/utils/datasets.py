from econuy.retrieval import (prices, fiscal_accounts, economic_activity,
                              labor, external_sector, financial_sector, income,
                              international, regional)

original = {"naccounts": {"description": "Cuentas nacionales (demanda precios constantes, "
                          "oferta precios constantes, oferta índice real, "
                          "oferta índice real desestacionalizado, oferta "
                          "precios corrientes, PBI precios corrientes)",
                          "function": economic_activity.national_accounts},
            "industrial_production": {"description": "Producción industrial",
                                      "function": economic_activity.industrial_production},
            "electricity": {"description": "Ventas de energía eléctrica por sector",
                            "function": economic_activity.electricity},
            "gasoline": {"description": "Ventas de nafta por departamento",
                         "function": economic_activity.gasoline},
            "diesel": {"description": " Ventas de gasoil por departamento",
                       "function": economic_activity.diesel},
            "cattle": {"description": "Faena de bovinos",
                       "function": economic_activity.cattle},
            "milk": {"description": "Remisión de leche a planta",
                     "function": economic_activity.milk},
            "cement": {"description": " Ventas de cemento",
                       "function": economic_activity.cement},
            "cpi": {"description": "Índice de precios al consumidor",
                    "function": prices.cpi},
            "nxr_monthly": {"description": "Tipo de cambio interbancario mensual",
                            "function": prices.nxr_monthly},
            "nxr_daily": {"description": "Tipo de cambio interbancario diario",
                          "function": prices.nxr_daily},
            "balance": {"description": "Resultado fiscal (SPC, NFPS, GC-BPS, "
                        "ANCAP, ANTEL, UTE, OSE, empresas públicas)",
                        "function": fiscal_accounts.balance},
            "taxes": {"description": "Recaudación impositiva",
                      "function": fiscal_accounts.tax_revenue},
            "public_debt": {"description": "Deuda pública (global, no monetario y BCU)",
                            "function": fiscal_accounts.public_debt},
            "labor": {"description": "Tasa de actividad, empleo y desempleo",
                      "function": labor.labor_rates},
            "hours": {"description": "Horas promedio trabajadas por sector",
                      "function": labor.hours},
            "wages": {"description": "Salarios nominales totales, sector público y privado",
                      "function": labor.nominal_wages},
            "real_wages": {"description": "Salarios reales totales, sector público y privado",
                           "function": labor.real_wages},
            "trade": {"description": "Comercio internacional de bienes (exportaciones e "
                      "importaciones por origen, destino, sector, valor, "
                      "volumen y precio)",
                      "function": external_sector.trade},
            "rxr_official": {"description": "Tipos de cambio reales BCU",
                             "function": external_sector.rxr_official},
            "reserves": {"description": "Reservas internacionales",
                         "function": external_sector.reserves},
            "reserves_changes": {"description": "Cambio en las reservas internacionales del BCU",
                                 "function": external_sector.reserves_changes},
            "deposits": {"description": "Depósitos en el sistema bancario",
                         "function": financial_sector.deposits},
            "credit": {"description": "Créditos del sistema bancario al sector no financiero",
                       "function": financial_sector.credit},
            "interest_rates": {"description": "Tasas de interés activas y pasivas",
                               "function": financial_sector.interest_rates},
            "call": {"description": "Tasa call a 1 día",
                     "function": financial_sector.call_rate},
            "sovereign_risk": {"description": "Spread de bonos uruguayos respecto de bonos "
                               "norteamericanos",
                               "function": financial_sector.sovereign_risk},
            "household_income": {"description": "Ingreso medio de los hogares sin VL ni "
                                 "aguinaldo",
                                 "function": income.income_household},
            "capita_income": {"description": "Ingreso medio per cápita sin VL ni aguinaldo",
                              "function": income.income_capita},
            "consumer_confidence": {"description": "Índice de confianza de los consumidores",
                                    "function": income.consumer_confidence}}

custom = {"core_industrial": {"description": "Producción industrial total, sin refinería y "
                              "núcleo",
                              "function": economic_activity.core_industrial},
          "cpi_measures": {"description": "índice de precios transable, no transable, "
                           "subyacente, residual y Winsorized 5%",
                           "function": prices.cpi_measures},
          "balance_fss": {"description": "Resultado fiscal (SPC, NFPS y GC-BPS, ajustado y "
                          "no ajustado por el FSS",
                          "function": fiscal_accounts.balance_fss},
          "net_public_debt": {"description": "Deuda neta excluyendo encajes",
                              "function": fiscal_accounts.net_public_debt},
          "rates_people": {"description": "Tasas de actividad, empleo y desempleo, incluyendo "
                           "localidades de más de 5 mil personas, y cantidad "
                           "de personas",
                           "function": labor.rates_people},
          "net_trade": {"description": "Balanza comercial por país",
                        "function": external_sector.trade_balance},
          "terms_of_trade": {"description": "Términos de intercambio",
                             "function": external_sector.terms_of_trade},
          "commodity_index": {"description": "Índice de precios de materias primas",
                              "function": external_sector.commodity_index},
          "rxr_custom": {"description": "Tipos de cambio reales vs. EE.UU., Argentina y "
                         "Brasil",
                         "function": external_sector.rxr_custom},
          "bonds": {"description": "Rendimiento de bonos soberanos en USD, UI y pesos",
                    "function": financial_sector.bonds},
          "global_gdp": {"description": "PBI real desestacionalizado de Estados Unidos,"
                         "Unión Europea, Japón y China",
                         "function": international.gdp},
          "global_stocks": {"description": "Índices bursátiles (S&P 500, Euronext 100, Nikkei"
                            "225 y Shanghai Composite)",
                            "function": international.stocks},
          "global_policy_rates": {"description": "Tasas de política monetaria de Estados "
                                  "Unidos, Eurozona, Japón y China",
                                  "function": international.policy_rates},
          "global_long_rates": {"description": "Tasas de bonos soberanos a 10 años de Estados "
                                "Unidos, Alemania, Francia, Italia, España, "
                                "Reino unido, Japón y China",
                                "function": international.long_rates},
          "global_nxr": {"description": "Cotización de monedas de Estados Unidos "
                         "(dollar index), USDUER, USDJPY y USDCNY",
                         "function": international.nxr},
          "regional_gdp": {"description": "PBI real desestacionalizado de Argentina y Brasil",
                           "function": regional.gdp},
          "regional_monthly_gdp": {"description": "PBI mensual real desestacionalizado de "
                                   "Argentina y Brasil",
                                   "function": regional.monthly_gdp},
          "regional_cpi": {"description": "Índice de precios al consumo de Argentina y Brasil",
                           "function": regional.cpi},
          "regional_embi_spreads": {"description": "EMBI spread de Argentina, Brasil y "
                                    "EMBI Global",
                                    "function": regional.embi_spreads},
          "regional_embi_yields": {"description": "EMBI yield de Argentina, Brasil y "
                                   "EMBI Global",
                                   "function": regional.embi_yields},
          "regional_nxr": {"description": "Tipo de cambio de Argentina (oficial y "
                           "paralelo) y Brasil",
                           "function": regional.nxr},
          "regional_policy_rates": {"description": "Tasas de política monetaria de Argentina "
                                    "y Brasil",
                                    "function": regional.policy_rates},
          "regional_stocks": {"description": "Índices bursátiles (MERVAL y BOVESPA) "
                              "en dólares",
                              "function": regional.stocks},
          "regional_rxr": {"description": "Tipo de cambio real de Argentina y Brasil "
                           "vis-à-vis EE.UU.",
                           "function": regional.rxr}
          }
