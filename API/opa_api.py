# import des librairies
from fastapi import FastAPI, Header
from pydantic import BaseModel
from sqlalchemy import create_engine
import pandas as pd
import joblib
import os
import datetime 

# création d'instance d'API
api = FastAPI(
    title="OPA Bot Trading",
    description="My own API for bot trading powered by FastAPI.",
    version="1.0.1",
    openapi_tags=[
        {
            "name": "Data",
            "description": "get personnalized historic data"
        },
        {
            "name": "Price prediction",
            "description": "price prediction bot based on Machine Learning algorithm"
        }
    ]
    )

# import des modèles et des scalers
btc_model = joblib.load("models/btc_model")
btc_scaler = joblib.load("models/btc_scaler")
eth_model = joblib.load("models/eth_model")
eth_scaler = joblib.load("models/eth_scaler")
bnb_model = joblib.load("models/bnb_model")
bnb_scaler = joblib.load("models/bnb_scaler")
sol_model = joblib.load("models/sol_model")
sol_scaler = joblib.load("models/sol_scaler")
xrp_model = joblib.load("models/xrp_model")
xrp_scaler = joblib.load("models/xrp_scaler")

# définitions des cryptos
tickers = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

# connection string for the database =  'postgresql://username:password@localhost:5432/your_database'
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
adress = os.getenv("DB_ADRESS")
conn_string = f'postgresql://{user}:{password}@{adress}:5432/klines'
engine = create_engine(conn_string) # Connexion avec sqlalchemy

# Création des routes
@api.get('/', name = 'OPA: BOT - Crypto')
def get_index():
    """
    Welcome on OPA bot trading for cryptocurrencies
    """
    return {'Message': 'Welcome on OPA bot trading for cryptocurrencies'}

@api.get('/kline info', name='Get hour Kline information for cryptocurrency in a day', tags=["Data"])
def get_kline_info(crypto_currency: str = Header(None, description='symbol of cryptocurrency'), the_day: str = Header(None, description = 'Day concerned')):
    """
    Select and display informations for a cryptocurrency klines on a day
    Enter a symbol for cryptocurrencies between the following : BTCUSDT, ETHUSDT , BNBUSDT, SOLUSDT, XRPUSDT
    Enter the day in the format : 'YYYY-MM-DD' example 2027-08-17
    """
        #query to select klines for cryptocurrency  on day given in paramters
    query = "SELECT Timestamp, Open, High, Low, Close, Volume, Trades FROM historical_klines where symbol = '"+ crypto_currency+"' and TO_CHAR(timestamp, 'YYYY-MM-DD')= '"+ the_day +"' ORDER BY timestamp ASC"
    data = pd.read_sql(query, engine)
    list_kline_infos =[]
    pos =0
    while pos <len(data):
        print('iteration ',pos+1,' de la boucle  while')
        elt = data.values[pos]
        list_kline_infos.append(('Timestamp: ', elt[0], 'Open: ' , elt[1], 'High: ' ,elt[2], 'Low: ' , elt[3], 'Close: ' ,elt[4], 'Volume: ' , elt[5], 'Trades: ' ,elt[6]))
        pos +=1 
    return {
        'Klines informations for': crypto_currency,
        'Date' : the_day,
        'Datas' : list_kline_infos
    }

@api.get("/{crypto:str}/prediction", name="Get the next hour predicted price for your favourite cryptocurrency", tags=["Price prediction"])
def get_crypto_prediction(crypto):
    """
    Select and display the next hour live price prediction from our Machine Learning algorithme named OPA
    For now our predictions are awailable for next cryptocurrencies : BTCUSDT, ETHUSDT , BNBUSDT, SOLUSDT, XRPUSDT
    """
    if crypto == "BTCUSDT":
        model = btc_model
        scaler = btc_scaler
    elif crypto == "ETHUSDT":
        model = eth_model
        scaler = eth_scaler
    elif crypto == "BNBUSDT":
        model = bnb_model
        scaler = bnb_scaler
    elif crypto == "SOLUSDT":
        model = sol_model
        scaler = sol_scaler
    elif crypto == "XRPUSDT":
        model = xrp_model
        scaler = xrp_scaler
    else:
        return f"Sorry, this crypto in not supported yet...Chose between this list: {tickers}"
    # récupération des données de streaming
    query = f"SELECT * FROM streaming_klines WHERE symbol = '{crypto}' ORDER BY timestamp DESC LIMIT 2"
    data = pd.read_sql(query, engine)
    # data manipulation
    timestamp = data["timestamp"]
    input_data = data[['open', 'high', 'low', 'close', 'volume', 'trades']]
    # prediction des résultats
    input_scaled = scaler.transform(input_data.values)
    prediction = model.predict(input_scaled)
    # variables
    prediction_h1 = prediction[0] # prédiction de prix sur la dernière bougie
    prev_prediction = prediction[1] # prédiction de prix de la bougie précédente
    resultat = input_data.iloc[0, 3] # le dernier prix 'close'
    potentiel = prediction_h1 - resultat # le gain ou perte potentielle
    prev_estimation = prev_prediction - input_data.iloc[1, 0] # le gain ou perte potentielle de la précédente bougie
    prev_realisation = input_data.iloc[1, 3] - input_data.iloc[1, 0] # le gain ou perte réel de la précédente bougie
    actual_time = datetime.datetime.now(tz=None) + datetime.timedelta(hours=2)
    closed_time = timestamp.iloc[0] + datetime.timedelta(hours=2)
    prediction_time = timestamp.iloc[0] + datetime.timedelta(hours=3) # l'heure du prix estimé, timestamp affiché est celui du début de la bougie

    return {
        "Actual Time": actual_time.strftime("%d/%m/%Y, %H:%M:%S"), # l'heure actuelle, il faut ajouter 2h 
        "Last closed Time": closed_time.strftime("%d/%m/%Y, %H:%M:%S"),
        "Last closed Price": f"{round(resultat, ndigits=2)}",
        "Prediction Time": prediction_time.strftime("%d/%m/%Y, %H:%M:%S"),
        "Prediction Price": f"{round(prediction_h1, ndigits=2)}",
        f"{'Hausse potentielle' if potentiel > 0 else 'Baisse potentielle'}": f"{round(potentiel, ndigits=2)} $",
        "Précédente estimation": f"{'hausse de' if prev_estimation > 0 else 'baisse de'} {round(prev_estimation, ndigits=2)} $",
        "Précédente réalisation": f"{'hausse de' if prev_realisation > 0 else 'baisse de'} {round(prev_realisation, ndigits=2)} $"
        }

# Créer une route avec le conseil d'investissement - quel crypto réalisera la meilleure perf la prochaine heure
@api.get("/trading_advisor", name="Take some trading advises", tags=["Price prediction"])
def get_trading_advisor():
    """
    Take best buying and selling advises from OPA bot based on prices predictions of BTCUSDT, ETHUSDT , BNBUSDT, SOLUSDT and XRPUSDT
    """
    # récupération des dernières informations sur les prix
    query = "SELECT * FROM streaming_klines ORDER BY timestamp DESC LIMIT 5"
    data = pd.read_sql(query, engine)
    # récupération des prix par crypto
    btc =  data[data["symbol"] == "BTCUSDT"]
    eth =  data[data["symbol"] == "ETHUSDT"]
    bnb =  data[data["symbol"] == "BNBUSDT"]
    sol =  data[data["symbol"] == "SOLUSDT"]
    xrp =  data[data["symbol"] == "XRPUSDT"]
    btc_input = btc[["open", "high", "low", "close", "volume", "trades"]]
    eth_input = eth[["open", "high", "low", "close", "volume", "trades"]]
    bnb_input = bnb[["open", "high", "low", "close", "volume", "trades"]]
    sol_input = sol[["open", "high", "low", "close", "volume", "trades"]]
    xrp_input = xrp[["open", "high", "low", "close", "volume", "trades"]]
    btc_scaled = btc_scaler.transform(btc_input)
    eth_scaled = eth_scaler.transform(eth_input)
    bnb_scaled = bnb_scaler.transform(bnb_input)
    sol_scaled = sol_scaler.transform(sol_input)
    xrp_scaled = xrp_scaler.transform(xrp_input)
    # prédictions par crypto
    btc_prediction = btc_model.predict(btc_scaled)
    eth_prediction = eth_model.predict(eth_scaled)
    bnb_prediction = bnb_model.predict(bnb_scaled)
    sol_prediction = sol_model.predict(sol_scaled)
    xrp_prediction = xrp_model.predict(xrp_scaled)
    # prédictions de gain/pertes potentielles
    btc_potential = ((btc_prediction - btc_input.iloc[0, 3]) / btc_input.iloc[0, 3]) * 100
    eth_potential = ((eth_prediction - eth_input.iloc[0, 3]) / eth_input.iloc[0, 3]) * 100
    bnb_potential = ((bnb_prediction - bnb_input.iloc[0, 3]) / bnb_input.iloc[0, 3]) * 100
    sol_potential = ((sol_prediction - sol_input.iloc[0, 3]) / sol_input.iloc[0, 3]) * 100
    xrp_potential = ((xrp_prediction - xrp_input.iloc[0, 3]) / xrp_input.iloc[0, 3]) * 100
    potential_return = {"BTCUSDT": btc_potential, 
                        "ETHUSDT": eth_potential, 
                        "BNBUSDT": bnb_potential, 
                        "SOLUSDT": sol_potential, 
                        "XRPUSDT": xrp_potential
                        }
    best_return_crypto = max(potential_return, key=potential_return.get)
    best_return_value = max(potential_return.values())
    worst_return_crypto = min(potential_return, key=potential_return.get)
    worst_return_value = min(potential_return.values())
    # affichage des résultats
    message = {}
    # BUY message
    if best_return_value >= 0.5:
        message["DON'T MISS"] = f"{best_return_crypto} next hour return is estimated to {round(best_return_value[0], ndigits=2)} %"
    elif best_return_value >= 0.2:
        message["OPPORTUNITY"] = f"You may watch to buy {best_return_crypto}, because the next hour the return is estimated to {round(best_return_value[0], ndigits=2)} %"
    elif best_return_value >= 0:
        message["INFO"] = f"Nothing special, but you can consider to buy {best_return_crypto} with estimated return of {round(best_return_value[0], ndigits=2)} %"
    else:
        message["DON'T BUY"] = "All cryptos are estimated to go down the next hour"
    # SELL message
    if worst_return_value <= -0.5:
        message["SELL"] = f"{worst_return_crypto} next hour return is estimated to {round(worst_return_value[0], ndigits=2)} %"
    elif worst_return_value <= -0.2:
        message["ATTENTION"] = f"You may watch to sell {worst_return_crypto}, because the next hour the return is estimated to {round(worst_return_value[0], ndigits=2)} %"
    elif worst_return_value <= 0:
        message["DOWN"] = f"Nothing special, but you may pay attention to {worst_return_crypto} with estimated return of {round(worst_return_value[0], ndigits=2)} %"
    else:
        message["DON'T SELL"] = "All cryptos are estimated to go up the next hour"
    # Return
    return message