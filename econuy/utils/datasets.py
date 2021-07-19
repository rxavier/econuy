def original():
    from econuy.retrieval import (
        prices,
        fiscal_accounts,
        economic_activity,
        labor,
        external_sector,
        financial_sector,
        income,
    )

    return {
        "natacc_ind_con_nsa": {
            "description": "Cuentas nacionales: Oferta, precios constantes (mar-16, T)",
            "function": economic_activity.natacc_ind_con_nsa,
        },
        "natacc_gas_con_nsa": {
            "description": "Cuentas nacionales: Demanda, precios constantes (mar-16, T)",
            "function": economic_activity.natacc_gas_con_nsa,
        },
        "natacc_ind_cur_nsa": {
            "description": "Cuentas nacionales: Oferta, precios corrientes (mar-16, T)",
            "function": economic_activity.natacc_ind_cur_nsa,
        },
        "natacc_gas_cur_nsa": {
            "description": "Cuentas nacionales: Demanda, precios corrientes (mar-16, T)",
            "function": economic_activity.natacc_gas_cur_nsa,
        },
        "gdp_con_idx_sa": {
            "description": "PBI: índice real, desestacionalizado (mar-16, T)",
            "function": economic_activity.gdp_con_idx_sa,
        },
        "industrial_production": {
            "description": "Producción industrial: por división, agrupaciones y clases (ene-02, M)",
            "function": economic_activity.industrial_production,
        },
        "electricity": {
            "description": "Ventas de energía eléctrica por sector (ene-00, M)",
            "function": economic_activity.electricity,
        },
        "gasoline": {
            "description": "Ventas de nafta por departamento (ene-04, M)",
            "function": economic_activity.gasoline,
        },
        "diesel": {
            "description": "Ventas de gasoil por departamento (ene-04, M)",
            "function": economic_activity.diesel,
        },
        "cattle": {
            "description": "Faena de bovinos (2-ene-05, S)",
            "function": economic_activity.cattle,
        },
        "milk": {
            "description": "Remisión de leche a planta (ene-02, M)",
            "function": economic_activity.milk,
        },
        "cement": {
            "description": "Ventas de cemento (ene-90, M)",
            "function": economic_activity.cement,
        },
        "cpi": {
            "description": "Índice de precios al consumidor - IPC (ene-37, M)",
            "function": prices.cpi,
        },
        "nxr_monthly": {
            "description": "Tipo de cambio, interbancario, mensual (abr-72, M)",
            "function": prices.nxr_monthly,
        },
        "nxr_daily": {
            "description": "Tipo de cambio, interbancario, diario (3-ene-00, D)",
            "function": prices.nxr_daily,
        },
        "balance_gps": {
            "description": "Resultado fiscal: Sector público consolidado (ene-99, M)",
            "function": fiscal_accounts.balance_gps,
        },
        "balance_nfps": {
            "description": "Resultado fiscal: Sector público no financiero (ene-99, M)",
            "function": fiscal_accounts.balance_nfps,
        },
        "balance_cg-bps": {
            "description": "Resultado fiscal: Gobierno central-BPS (ene-99, M)",
            "function": fiscal_accounts.balance_cg_bps,
        },
        "balance_pe": {
            "description": "Resultado fiscal de las empresas públicas",
            "function": fiscal_accounts.balance_pe,
        },
        "balance_ancap": {
            "description": "Resultado fiscal: ANCAP (ene-01, M)",
            "function": fiscal_accounts.balance_ancap,
        },
        "balance_ute": {
            "description": "Resultado fiscal: UTE (ene-99, M)",
            "function": fiscal_accounts.balance_ute,
        },
        "balance_antel": {
            "description": "Resultado fiscal: ANTEL (ene-99, M)",
            "function": fiscal_accounts.balance_antel,
        },
        "balance_ose": {
            "description": "Resultado fiscal: OSE (ene-99, M)",
            "function": fiscal_accounts.balance_ose,
        },
        "tax_revenue": {
            "description": "Recaudación por impuesto (ene-82, M)",
            "function": fiscal_accounts.tax_revenue,
        },
        "public_debt_gps": {
            "description": "Deuda del sector público global: por plazo contractual, residual, moneda y residencia (dic-99, T)",
            "function": fiscal_accounts.public_debt_gps,
        },
        "public_debt_nfps": {
            "description": "Deuda del sector público no monetario: por plazo contractual, residual, moneda y residencia (dic-99, T)",
            "function": fiscal_accounts.public_debt_nfps,
        },
        "public_debt_cb": {
            "description": "Deuda del Banco Central: por plazo contractual, residual, moneda y residencia (dic-99, T)",
            "function": fiscal_accounts.public_debt_cb,
        },
        "public_assets": {
            "description": "Activos del sectro público (dic-99, T)",
            "function": fiscal_accounts.public_assets,
        },
        "labor_rates": {
            "description": "Tasas de actividad, empleo y desempleo por sexo (ene-06, M)",
            "function": labor.labor_rates,
        },
        "hours_worked": {
            "description": "Horas promedio trabajadas por sector (ene-06, M)",
            "function": labor.hours,
        },
        "nominal_wages": {
            "description": "Salarios nominales: total, públicos y privados (ene-68, M)",
            "function": labor.nominal_wages,
        },
        "real_wages": {
            "description": "Salarios reales: total, públicos y privados (ene-68, M)",
            "function": labor.real_wages,
        },
        "trade_x_prod_val": {
            "description": "Exportaciones por sector, valor (ene-00, M)",
            "function": external_sector.trade_x_prod_val,
        },
        "trade_x_prod_vol": {
            "description": "Exportaciones por sector, volumen (ene-05, M)",
            "function": external_sector.trade_x_prod_vol,
        },
        "trade_x_prod_pri": {
            "description": "Exportaciones por sector, precio (ene-05, M)",
            "function": external_sector.trade_x_prod_pri,
        },
        "trade_x_dest_val": {
            "description": "Exportaciones por destino, valor (ene-00, M)",
            "function": external_sector.trade_x_dest_val,
        },
        "trade_x_dest_vol": {
            "description": "Exportaciones por destino, volumen (ene-05, M)",
            "function": external_sector.trade_x_dest_vol,
        },
        "trade_x_dest_pri": {
            "description": "Exportaciones por destino, precio (ene-05, M)",
            "function": external_sector.trade_x_dest_pri,
        },
        "trade_m_sect_val": {
            "description": "Importaciones por tipo, valor (ene-00, M)",
            "function": external_sector.trade_m_sect_val,
        },
        "trade_m_sect_vol": {
            "description": "Importaciones por tipo, volumen (ene-05, M)",
            "function": external_sector.trade_m_sect_vol,
        },
        "trade_m_sect_pri": {
            "description": "Importaciones por tipo, precio (ene-05, M)",
            "function": external_sector.trade_m_sect_pri,
        },
        "trade_m_orig_val": {
            "description": "Importaciones por origen, valor (ene-00, M)",
            "function": external_sector.trade_m_orig_val,
        },
        "trade_m_orig_vol": {
            "description": "Importaciones por origen, volumen (ene-05, M)",
            "function": external_sector.trade_m_orig_vol,
        },
        "trade_m_orig_pri": {
            "description": "Importaciones por origen, precio (ene-05, M)",
            "function": external_sector.trade_m_orig_pri,
        },
        "rxr_official": {
            "description": "Tipos de cambio reales, BCU (ene-00, M)",
            "function": external_sector.rxr_official,
        },
        "reserves": {
            "description": "Reservas internacionales (26-jun-02, D)",
            "function": external_sector.reserves,
        },
        "reserves_changes": {
            "description": "Variación en las reservas del BCU por fuente (2-ene-13, D)",
            "function": external_sector.reserves_changes,
        },
        "deposits": {
            "description": "Depósitos en el sistema bancario (dic-98, M)",
            "function": financial_sector.deposits,
        },
        "credit": {
            "description": "Créditos del sistema bancario al sector no financiero (dic-98, M)",
            "function": financial_sector.credit,
        },
        "interest_rates": {
            "description": "Tasas de interés activas y pasivas (ene-98, M)",
            "function": financial_sector.interest_rates,
        },
        "call": {
            "description": "Tasa call a 1 día (02-ene-02, D)",
            "function": financial_sector.call_rate,
        },
        "sovereign_risk": {
            "description": "Uruguay Bond Index - riesgo soberano (01-ene-99, D)",
            "function": financial_sector.sovereign_risk,
        },
        "income_household": {
            "description": "Ingreso medio de los hogares sin valor ni aguinaldo (ene-06, M)",
            "function": income.income_household,
        },
        "income_capita": {
            "description": "Ingreso medio per cápita sin valor locativo ni aguinaldo (ene-06, M)",
            "function": income.income_capita,
        },
        "consumer_confidence": {
            "description": "Índice de Confianza del Consumidor (ago-07, M)",
            "function": income.consumer_confidence,
        },
    }


def custom():
    from econuy.retrieval import (
        prices,
        fiscal_accounts,
        economic_activity,
        labor,
        external_sector,
        financial_sector,
        international,
        regional,
    )

    return {
        "core_industrial": {
            "description": "Producción industrial: total, sin refinería y núcleo (ene-02, M [e])",
            "function": economic_activity.core_industrial,
        },
        "natacc_ind_con_nsa_long": {
            "description": "Cuentas nacionales: Oferta, precios constantes, serie empalmada (mar-88, T [e])",
            "function": economic_activity.natacc_ind_con_nsa_long,
        },
        "natacc_gas_con_nsa_long": {
            "description": "Cuentas nacionales: Demanda, precios constantes, serie empalmada (mar-88, T [e])",
            "function": economic_activity.natacc_gas_con_nsa_long,
        },
        "gdp_con_idx_sa_long": {
            "description": "PBI: índice real, desestacionalizado, serie empalmada (mar-88, T [e])",
            "function": economic_activity.gdp_con_idx_sa_long,
        },
        "gdp_con_nsa_long": {
            "description": "PBI: constante, serie empalmada (wp BCU 12-15) (mar-88, T [e])",
            "function": economic_activity.gdp_con_nsa_long,
        },
        "gdp_cur_nsa_long": {
            "description": "PBI: corriente, serie empalmada (wp BCU 12-15) (mar-97, T [e])",
            "function": economic_activity.gdp_cur_nsa_long,
        },
        "cpi_measures": {
            "description": "IPC transable, no transable, subyacente y residual (mar-97, M [e])",
            "function": prices.cpi_measures,
        },
        "balance_summary": {
            "description": "Resultado fiscal: Todas las agregaciones, inc. aj. FSS (ene-99, M [e])",
            "function": fiscal_accounts.balance_summary,
        },
        "net_public_debt": {
            "description": "Deuda neta del sector público global excl. encajes (dic-99, T [e])",
            "function": fiscal_accounts.net_public_debt,
        },
        "labor_rates_people": {
            "description": "Actividad, empleo y desempleo: series extendidas de tasas y personas (ene-91, M [e])",
            "function": labor.rates_people,
        },
        "trade_balance": {
            "description": "Balanza comercial por país (ene-00, M [e])",
            "function": external_sector.trade_balance,
        },
        "terms_of_trade": {
            "description": "Términos de intercambio (ene-05, M [e])",
            "function": external_sector.terms_of_trade,
        },
        "commodity_prices": {
            "description": "Precios de commodities seleccionados (ene-02, M [e])",
            "function": external_sector.commodity_prices,
        },
        "commodity_index": {
            "description": "Índice econuy de precios de commodities (ene-02, M [e])",
            "function": external_sector.commodity_index,
        },
        "rxr_custom": {
            "description": "Tipos de cambio reales, cálculos econuy (dic-79, M [e])",
            "function": external_sector.rxr_custom,
        },
        "bonds": {
            "description": "Rendimiento de bonos soberanos (02-jun-03, D [e])",
            "function": financial_sector.bonds,
        },
        "global_gdp": {
            "description": "PBI real desestacionalizado de EE.UU., Unión Europea, Japón y China (mar-47, T)",
            "function": international.gdp,
        },
        "global_stocks": {
            "description": "Índices bursátiles de EE.UU., Unión Europea, Japón y China (30-dic-27, D)",
            "function": international.stocks,
        },
        "global_policy_rates": {
            "description": "Tasas de interés de política monetaria de EE.UU., Eurozona, Japón y China (01-ene-46, D)",
            "function": international.policy_rates,
        },
        "global_long_rates": {
            "description": "Tasas de interés de largo plazo (10 años) de EE.UU., Alemania, Francia, España, Italia, Reino Unido, Japón y China (02-ene-62, D)",
            "function": international.long_rates,
        },
        "global_nxr": {
            "description": "Cotización de monedas de EE.UU., Eurozona, Japón y China (04-ene-71, D)",
            "function": international.nxr,
        },
        "regional_gdp": {
            "description": "PBI real desestacionalizado de Argentina y Brasil (mar-93, T)",
            "function": regional.gdp,
        },
        "regional_monthly_gdp": {
            "description": "PBI real mensual desestacionalizado de Argentina y Brasil (ene-03, M)",
            "function": regional.monthly_gdp,
        },
        "regional_cpi": {
            "description": "Índice de precios al consumo de Argentina y Brasil (ene-70, M)",
            "function": regional.cpi,
        },
        "regional_embi_spreads": {
            "description": "EMBI spread (JP Morgan) de Argentina, Brasil y Global (11-dic-98, D)",
            "function": regional.embi_spreads,
        },
        "regional_embi_yields": {
            "description": "EMBI yield (JP Morgan) de Argentina, Brasil y Global (11-dic-98, D)",
            "function": regional.embi_yields,
        },
        "regional_nxr": {
            "description": "Tipo de cambio nominal diario de Argentina y Brasil (9-abr-02, D)",
            "function": regional.nxr,
        },
        "regional_policy_rates": {
            "description": "Tasas de interés de política monetaria de Argentina y Brasil (4-jun-86, D)",
            "function": regional.policy_rates,
        },
        "regional_stocks": {
            "description": "Índices bursátiles en USD de Argentina y Brasil (9-abr-02, D)",
            "function": regional.stocks,
        },
        "regional_rxr": {
            "description": "Tipo de cambio real vs. EE.UU. (dic-70, M)",
            "function": regional.rxr,
        },
        "_lin_gdp": {
            "description": "PBI corriente acumulado mensual linealizado en pesos y USD con proyecciones",
            "function": economic_activity._lin_gdp,
        },
    }
