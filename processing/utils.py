import datetime as dt

BEEF_URL = ("https://www.inac.uy/innovaportal/v/9799/10/innova.front/serie-semanal"
            "-ingreso-medio-de-exportacion---bovino-ovino-y-otros-productos")
PULP_URL = (f"https://www.insee.fr/en/statistiques/serie/telecharger/010600339?ordre=antechronologique&"
            f"transposition=donneescolonne&periodeDebut=1&anneeDebut=1990&periodeFin=11&anneeFin="
            f"{dt.datetime.now().year}")
SOYBEAN_URL = "https://www.quandl.com/api/v3/datasets/CHRIS/CME_S1.csv?api_key=3TPxACcrxy9WsE871Lqe"
WHEAT_URL = "https://www.quandl.com/api/v3/datasets/CHRIS/CME_W1.csv?api_key=3TPxACcrxy9WsE871Lqe"
IMF_URL = "https://www.imf.org/~/media/Files/Research/CommodityPrices/Monthly/ExternalData.ashx"
MILK1_URL = "https://www.inale.org/wp-content/uploads/2019/12/Precios-exportaci%C3%B3n-de-Ocean%C3%ADa-1.xls"
MILK2_URL = "https://www.inale.org/wp-content/uploads/2019/12/Precios-exportaci%C3%B3n-de-Europa-2.xls"
