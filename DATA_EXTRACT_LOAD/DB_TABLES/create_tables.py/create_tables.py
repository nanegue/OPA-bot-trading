
import psycopg2 
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
adress = os.getenv("DB_ADRESS")
conn_string = f"postgresql://{user}:{password}@{adress}:5432/opadb"

# Connexion avec psycopg2
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# supprime les tables si elles existent
cursor.execute("DROP TABLE IF EXISTS historical_klines");
cursor.execute("DROP TABLE IF EXISTS streaming_klines");

# creation de la table des donnees historiques des bougies "historical_klines"
cursor.execute("CREATE TABLE historical_klines (Symbol VARCHAR(10), Timestamp timestamp without time zone, Open NUMERIC, High NUMERIC,  Low NUMERIC, Close NUMERIC, Volume NUMERIC, Trades BIGINT)");

# creation de la table des donnees de streaming des bougies "streaming_klines"
cursor.execute("CREATE TABLE streaming_klines (Symbol VARCHAR(10), Timestamp timestamp without time zone, Open NUMERIC, High NUMERIC,  Low NUMERIC, Close NUMERIC, Volume NUMERIC, Trades BIGINT)");

cursor.execute("commit");

#fermeture du curseur et de la connection a la base
cursor.close()
conn.close()
print("la connexion a PostgreSQL est cloturee ")
