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
from pprint import pprint

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

for i, ticker in enumerate(tickers):
  timestamp = client._get_earliest_valid_timestamp(ticker, '1h')
  data[i] = client.get_historical_klines(ticker, "1h", timestamp)

# IMPORT DES DONNEES HISTORIQUES DANS LA BASE DE DONNEES

# connection au serveur de base de donnee Posgresql sur le cloud 
import psycopg2 
conn_string = 'postgresql://opauser:opadatapwd@ec2-16-171-16-2.eu-north-1.compute.amazonaws.com:5432/opadb'
print (' connexion à la base de donnees')
# Connexion avec psycopg2
conn = psycopg2.connect(conn_string) 
print('connexion créé')
cursor = conn.cursor()
print( 'curseur sur la connexion créé')

for i, ticker in enumerate(tickers):
    # création de DataFrame à partir des données récupérées
    df_data_1h = pd.DataFrame(data[i], columns = ["Timestamp", "Open", "High", "Low", "Close", "Volume", "Close_time", "Asset_volume", "Trades", "Tb_base_av", "Tb_quote_av", "Ignore"])
    
    # on garde que les 6 premières colonnes et la colonnes "trades" 
    df_data_1h = pd.concat([df_data_1h.iloc[0:, 0:6] ,df_data_1h.iloc[0:, 8]], axis = 1)

    # transformation du temps en index
    df_data_1h = df_data_1h.set_index(df_data_1h["Timestamp"])
    df_data_1h.index = pd.to_datetime(df_data_1h.index, unit="ms")
    #del df_data_1h["Timestamp"]

    # on transform les colonnes en valeurs numériques sauf la colonne timestamp

    for column in df_data_1h.columns:
        if column != 'Timestamp':
            df_data_1h[column] = pd.to_numeric(df_data_1h[column])

    # on transforme la colonne timestamp en date heure
    df_data_1h['Timestamp'] = pd.to_datetime(df_data_1h['Timestamp'], unit="ms")   

    # on alimente la table en base de donnees avec le contenu du DF 
    
    symb= ticker # on travaille sur la crypto ayant le symbole ticker
    print('debut insertion des donnees de la crypto ', ticker)
    # construction de la chaine de caractere de la requete sql d insertion des donnees
    insert_query = """  INSERT INTO historical_klines(symbol, timestamp, open, high, low, close, volume, trades)	VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    for data_line in df_data_1h.values :
        t = data_line[0]
        # construction de la ligne de donnees a inserer dans la table en base de donnees
        record_to_insert = (symb, data_line[0] , data_line[1], data_line[2], data_line[3], data_line[4], data_line[5], data_line[6])
        # execution de la requete sql d insertion de la ligne dans la table
        cursor.execute(insert_query, record_to_insert)
        # validation de l insertion
        cursor.execute("commit");
        print('ligne insérée')
    print('fin insertion des donnees de la crypto ', ticker)
    
#fermeture du curseur et de la connection a la base
cursor.close()
conn.close()
print("la connexion a PostgreSQL est cloturee ")
