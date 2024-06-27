# import libraries
import pandas as pd
import psycopg2 # connexion bdd
import os
import joblib
# machine learning
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_absolute_error, r2_score, accuracy_score
from sklearn.preprocessing import StandardScaler

# Example: 'postgresql://username:password@localhost:5432/your_database'
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
adress = os.getenv("DB_ADRESS")
conn_string = f'postgresql://{user}:{password}@{adress}:5432/opadb'

# Connexion avec psycopg2
conn = psycopg2.connect(conn_string)

# Open a cursor to perform database operations
cursor = conn.cursor()

# la requête
query = "SELECT * FROM historical_klines WHERE timestamp <= '2024-06-22' ORDER BY timestamp ASC"

# récupération des données
data = pd.read_sql_query(query, conn)

# transformation du temps en index
data = data.set_index(data["timestamp"])
del data["timestamp"] # suppression de la colonne timestamp

btc = data[data["symbol"] == "BTCUSDT"]

# suppression de la colonne 'symbol'
del btc["symbol"]

# Création des colonnes target qu'on predira pour les 2 types de ML
btc["in_1_hour"] = btc["close"].shift(-1)   # variable target correspond au prix de la crypto dans 1 h
btc["price_change"] = btc["in_1_hour"] - btc["close"]
btc["price_up"] = (btc["price_change"] > 0).astype(int) # 1 si on prédit une hausse, 0 si une baisse
# on supprime la dernière ligne car il n'y a de valeur pour btc["close"].shift(-1)
btc = btc[:-1]

# divisions des données en variable 'features' et  variable 'target"
feats = btc[['open', 'high', 'low', 'close', 'volume', 'trades']]
target_reg = btc["in_1_hour"]
target_clf = btc["price_up"]

# on sépare les données en se basant sur  une date
train_date, test_date = "2023-12-30", "2024-01-01"
# donnees de "train" vont  de la date la plus ancienne jusqu 'au "2023-12-30"
#donnees de "test" commence "2024-01-01" jusqu'a la date la plus recente des donnees extraites

X_train, X_test = feats.loc[:train_date], feats[test_date:]
y_reg_train, y_reg_test = target_reg[:train_date], target_reg[test_date:]
y_clf_train, y_clf_test = target_clf[:train_date], target_clf[test_date:]

# Normalisation des caractéristiques
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Entrainement des modèles de regression
rf_regressor = RandomForestRegressor()
gb_regressor = GradientBoostingRegressor()
lr_regressor = LinearRegression()

rf_regressor.fit(X_train_scaled, y_reg_train)
gb_regressor.fit(X_train_scaled, y_reg_train)
lr_regressor.fit(X_train_scaled, y_reg_train)

# Entrainement des modèles de classification
rf_classifier = RandomForestClassifier()
gb_classifier = GradientBoostingClassifier()
lr_classifier = LogisticRegression()

rf_classifier.fit(X_train_scaled, y_clf_train)
gb_classifier.fit(X_train_scaled, y_clf_train)
lr_classifier.fit(X_train_scaled, y_clf_train)


# Enregistrer le scaler avec joblib
joblib.dump(scaler, "models/btc_scaler")

# Enregistrement du modèle avec joblib
joblib.dump(gb_regressor, "models/btc_gb_regressor_model")