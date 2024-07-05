import psycopg2 
import pandas as pd
import boto3
import os

def load_histo_data():
    
    # IMPORT DES DONNEES HISTORIQUES DANS LA BASE DE DONNEES

    # connection au serveur de base de donnee Posgresql sur le cloud 
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    adress = os.getenv("DB_ADRESS")
    
    print (' connexion à la base de donnees')
    # Connexion avec psycopg2
    conn = psycopg2.connect(conn_string) 
    print('connexion créé')
    cursor = conn.cursor()
    print( 'curseur sur la connexion créé')
    # tickers = [ "BTCUSDT","ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    # for i, ticker in enumerate(tickers):
      # read a csv file from S3 bucket and put in a dataframe
    #  providing credentials  to Boto3  by setting environment variables  AWS_ACCESS_KEY  and AWS_SECRET
    client_s3 = boto3.client('s3',
                                aws_access_key_id= AWS_ACCESS_KEY,
                                aws_secret_access_key= AWS_SECRET)
    # 's3' is a key word. create connection to S3 using default config and all buckets within S3
    bucket_name = os.getenv("S3_BUCKET_NAME")
    data_folder = os.getenv("S3_DATA_FOLDER")

    #tickers = [ "BTCUSDT","ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    
    list_data_filename =  [ "transf_data_BTCUSDT.csv","transf_data_ETHUSDT.csv", "transf_data_BNBUSDT.csv", "transf_data_SOLUSDT.csv", "transf_data_XRPUSDT.csv"]
    for i, filename in enumerate(list_data_filename):
        # création de DataFrame à partir des données récupérées
        print('===============je suis sur l ouverture du fichier ', data_folder+filename,' sur le bucket ', bucket_name )
        obj = client_s3.get_object(Bucket= bucket_name, Key= data_folder+filename) 
        print('===================fichier ouvert=============')
        df_data_1h = pd.read_csv(obj['Body']) # 'Body' is a key word
        print('filename = ', filename)
    #   symb= ticker # on travaille sur la crypto ayant le symbole ticker
        # print('debut insertion des donnees de la crypto ', ticker)
        # construction de la chaine de caractere de la requete sql d insertion des donnees
        insert_query = """  INSERT INTO historical_klines(symbol, timestamp, open, high, low, close, volume, trades)	VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
        for data_line in df_data_1h.values :
            t = data_line[0]
            # construction de la ligne de donnees a inserer dans la table en base de donnees
            record_to_insert = (data_line[0] , data_line[1], data_line[2], data_line[3], data_line[4], data_line[5], data_line[6], data_line[7])
            # execution de la requete sql d insertion de la ligne dans la table
            cursor.execute(insert_query, record_to_insert)
            # validation de l insertion
            cursor.execute("commit");
            print('ligne insérée')
        print('fin insertion des donnees du fichier  ', filename)
        
    #fermeture du curseur et de la connection a la base
    cursor.close()
    conn.close()
    print("la connexion a PostgreSQL est cloturee ")

