from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from extract_histo_data import extract_histo_data
from load_histo_data import load_histo_data
from transform_histo_data import transform_histo_data
import datetime


etl_data_histo_dag = DAG(
    dag_id='my_etl_data_histo_dag',
    description='My DAG created for ETL historic data on crypto currencies from binance',
    tags=['project', 'datascientest'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
    }
)

extract_data_task = PythonOperator(
    task_id='my_extract_data_task',
    python_callable=extract_histo_data,
    dag=etl_data_histo_dag
)


transform_data_task = PythonOperator(
    task_id='my_transform_data_task',
    python_callable=transform_histo_data,
    dag=etl_data_histo_dag
)


load_data_task = PythonOperator(
    task_id='my_load_data_task', 
    python_callable=load_histo_data,
    dag=etl_data_histo_dag
)

extract_data_task >>transform_data_task>>load_data_task