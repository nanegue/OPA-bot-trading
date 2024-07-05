import pandas as pd
import boto3
import os

def transform_histo_data():
    # read a csv file from S3 bucket and put in a dataframe
        #  providing credentials  to Boto3  by setting environment variables  AWS_ACCESS_KEY  and AWS_SECRET
    client_s3 = boto3.client('s3',
                                aws_access_key_id= AWS_ACCESS_KEY,
                                aws_secret_access_key= AWS_SECRET)
    # 's3' is a key word. create connection to S3 using default config and all buckets within S3
    bucket_name = os.getenv("S3_BUCKET_NAME")
    data_folder = os.getenv("S3_DATA_FOLDER")
    
    #tickers = [ "BTCUSDT","ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    
    list_data_filename =  [ "data_BTCUSDT.csv","data_ETHUSDT.csv", "data_BNBUSDT.csv", "data_SOLUSDT.csv", "data_XRPUSDT.csv"]
    # for i, ticker in enumerate(tickers):
    for i, filename in enumerate(list_data_filename):
        # création de DataFrame à partir des données récupérées
        print('===============je suis sur l ouverture du fichier ', data_folder+filename,' sur le bucket ', bucket_name )
        obj = client_s3.get_object(Bucket= bucket_name, Key= data_folder+filename) 
        print('===================fichier ouvert=============')
        df_data_1h = pd.read_csv(obj['Body']) # 'Body' is a key word
        print('filename = ', filename)
        # df_data_1h = pd.DataFrame(data[i], columns = ["Timestamp", "Open", "High", "Low", "Close", "Volume", "Close_time", "Asset_volume", "Trades", "Tb_base_av", "Tb_quote_av", "Ignore"])
        
        # on garde que les 6 premières colonnes et la colonnes "trades" 
        df_data_1h = pd.concat([df_data_1h.iloc[0:, 0:7] ,df_data_1h.iloc[0:, 9]], axis = 1)

        # transformation du temps en index
        df_data_1h = df_data_1h.set_index(df_data_1h["Timestamp"])
        df_data_1h.index = pd.to_datetime(df_data_1h.index, unit="ms")
        #del df_data_1h["Timestamp"]

        # on transform les colonnes en valeurs numériques sauf la colonne timestamp

        for column in df_data_1h.columns:
            if column != 'Timestamp' and column !='symbol':
                df_data_1h[column] = pd.to_numeric(df_data_1h[column])

        # on transforme la colonne timestamp en date heure
        df_data_1h['Timestamp'] = pd.to_datetime(df_data_1h['Timestamp'], unit="ms")  
        transf_filename = 'transf_' + filename 
        df_data_1h.to_csv("./"+transf_filename, sep=',',index=False)
        # upload the file in s3
        with open(transf_filename,"rb") as f:
            client_s3.upload_fileobj(f, bucket_name, data_folder+transf_filename)
        print('================fichier envoye : ', transf_filename)

