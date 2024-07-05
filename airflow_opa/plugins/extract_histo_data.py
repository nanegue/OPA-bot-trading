# import packages

from binance.client import Client
from binance import ThreadedWebsocketManager
# import websocket
import json
import pandas as pd
from sqlalchemy import create_engine
import time
import pandas as pd
import os
import boto3
from pprint import pprint

def extract_histo_data():
    
    # CONNEXION A API BINANCE 

    # récupération des clés
    # au préalable, les variable ont été enregistrés dans venv à l'aide du fichier venv/bin/activate
    KEY = os.environ["API_KEY"]
    SECRET = os.environ["SECRET_KEY"]


    # création de client
    client = Client(api_key=KEY, api_secret=SECRET)

    # RECUPERATION DES DONNEES SUR DES BOUGIES HORAIRES
    # Choix des cryptos par capitalisation du marché
    tickers = [ "BTCUSDT","ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

    # boucle de récupération des données historiques des differentes crypto
    data = [ "btc","eth", "bnb","sol", "xrp"]  # 

    # calcul d une date a partir de laquelle je souhaite recuperer les donnees histo des crypto
    dt_obj = time.time() - 86400
    time_ms = int(dt_obj* 1000)

    for i, ticker in enumerate(tickers):

        data[i] = client.get_historical_klines(ticker, "1h", time_ms)
        print(ticker)
        for  row_data in data[i]:    
            row_data.insert(0, ticker)
        
        df_data_1h = pd.DataFrame(data[i], columns = ["symbol","Timestamp", "Open", "High", "Low", "Close", "Volume", "Close_time", "Asset_volume", "Trades", "Tb_base_av", "Tb_quote_av", "Ignore"])
        filename = "data_"+ticker+'.csv'
        df_data_1h.to_csv("./"+filename, sep=',',index=False)

        # info for the s3 bucket 
        print(" debut de l envoi vers s3")
        bucket_name = os.getenv("S3_BUCKET_NAME")
        data_folder = os.getenv("S3_DATA_FOLDER")
    #  providing credentials  to Boto3  by setting environment variables  AWS_ACCESS_KEY  and AWS_SECRET
        client_s3 = boto3.client('s3',
                                aws_access_key_id= AWS_ACCESS_KEY,
                                aws_secret_access_key= AWS_SECRET)
        print(' ouverture du fichier :', filename)
        # upload the file in s3
        with open(filename,"rb") as f:
            client_s3.upload_fileobj(f, bucket_name, data_folder+filename)
        print(' fin de l envoi du fichier vers s3')
        
