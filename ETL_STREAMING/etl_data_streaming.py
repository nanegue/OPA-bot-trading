import websocket
import json
import pandas as pd
import psycopg2
import os

# Connection à la base des données Posgresql sur le cloud
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
adress = os.getenv("DB_ADRESS")
conn_string = f"postgresql://{user}:{password}@{adress}:5432/opadb"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# Création d'un DataFrame vide
streaming_df = pd.DataFrame(columns=["symbol","Timestamp", "Open", "High", "Low", "Close", "Volume", "Trades"], index=[1])

# Définition de socket pour suivre les prix des cryptos (interval d'1h)
socket = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1h/ethusdt@kline_1h/bnbusdt@kline_1h/solusdt@kline_1h/xrpusdt@kline_1h"

# Définition de la fonction de sauvegardes des données dans la bdd
def on_message(ws, message):
  message = json.loads(message)

  # Récuperation des données de bougies (Open, High, Low, Close, Volume, Trades)
  kline_data = message['data']['k']

  if kline_data['x']:  # Si x = True, alors il s'agit de la clôture de la bougie d'1 heure
    symbol = kline_data['s']
    Timestamp = pd.to_datetime(kline_data['T'], unit='ms')
    Open = float(kline_data['o'])
    High = float(kline_data['h'])
    Low = float(kline_data['l'])
    Close =  float(kline_data['c'])
    Volume = float(kline_data['v'])
    Trades = int(kline_data['n'])
    streaming_df.iloc[0] = [symbol, Timestamp, Open, High, Low, Close, Volume, Trades]
    print('ligne à inserer en base')

     # construction de la chaine de caractere de la requete sql d'insertion des donnees
    insert_query = """INSERT INTO streaming_klines(symbol, timestamp, open, high, low, close, volume, trades)	VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

    # construction de la ligne de donnees a inserer dans la table en base de donnees
    record_to_insert = (symbol, Timestamp, Open, High, Low, Close, Volume, Trades)

    # execution de la requete sql d insertion de la ligne dans la table
    cursor.execute(insert_query, record_to_insert)

    # validation de l'insertion
    cursor.execute("commit");
    print("Les données ont été envoyées...")

# Lancement de streaming
ws = websocket.WebSocketApp(socket, on_message=on_message)
print("Lancement du streaming")
ws.run_forever()
print("Le streaming a été arreté")
websocket.WebSocket