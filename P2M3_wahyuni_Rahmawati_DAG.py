# Grade Challange 3

# =======================================================

# Nama  : Wahyuni Rahmawati \
# Batch : RMT-25

# =======================================================

#import Library
import psycopg2
import re
import pandas as pd
import datetime as dt
import warnings
from elasticsearch import Elasticsearch
warnings.filterwarnings("ignore")


from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# =======================================================
# A. FETCH FROM POSTGRESQL
def fetch_data():
    # Config Database
    db_name = 'airflow'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = '5432'

    # Connect to Database
    connection = psycopg2.connect(
        database = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
    )

    # Get All Data
    select_query = 'SELECT* FROM table_m3;'
    df = pd.read_sql(select_query, connection)

    # Close the Connection
    connection.close()

    #Save the CSV
    df.to_csv('/opt/airflow/dags/P2M3_rahma_data_raw.csv', index=False)

# =======================================================
# B. DATA CLEANING
def data_cleaning():
    df = pd.read_csv('/opt/airflow/dags/P2M3_rahma_data_raw.csv')

    #columns normalization
    cols = df.columns.tolist()
    new_cols = []

    for col in cols:
        new_col = re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))',col)
        new_col = [x.lower() for x in new_col]
        new_col = '_'.join(new_col)
        new_cols.append(new_col)

    df.columns = new_cols

    #deleting duplicate data
    df = df.drop_duplicates()

    #handling missing value
    df.fillna(df.mode().iloc[0], inplace=True)

    # Save into csv
    df.to_csv('/opt/airflow/dags/P2M3_rahma_data_clean.csv', index=False)

# =======================================================
# C. INSERT INTO ELASTIC SEARCH - WITH AIRFLOW
    
def insert_into_elastic_manual():
    df = pd.read_csv('/opt/airflow/dags/P2M3_rahma_data_clean.csv')

    #Check Connection
    es = Elasticsearch('http://elasticsearch:9200')
    print('Connection status : ',es.ping())

    # Insert SCV file to Elasticsearch
    failed_insert = []
    for i, r in df.iterrows():
        doc = r.to_json()
        try:
            print(i,r['name'])
            res = es.index(index="P2M3_rahma_data_clean", doc_type="doc", body=doc)
        except:
            print('Index Gagal : ', r['index'])
            failed_insert.spend(r['index'])
            pass

    print('DONE')
    print('Failed Insert : ', failed_insert)


# =======================================================
# D. DATA PIPELINE
    
default_args = {
    'owner' : 'DianSastro',
    'start_date': dt.datetime(2023, 11, 25, 1, 22, 0) - dt.timedelta(hours=7),
    'retries':1,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG(
    'p2m3',
    default_args = default_args,
    schedule_interval = '*/30 * * * *',
    catchup = False) as dag:

    node_start = BashOperator(
        task_id= 'starting',
        bash_command='echo "I am reading the CSV now. . . ."')
    node_fetch_data = PythonOperator(
        task_id='fetch-data',
        python_callable=fetch_data)
    node_data_cleaning = PythonOperator(
        task_id='data-cleaning',
        python_callable=data_cleaning)
    node_insert_data_to_elastic = PythonOperator(
        task_id='insert-data-to-elastic',
        python_callable=insert_into_elastic_manual)
    
node_start >> node_fetch_data >> node_data_cleaning >> node_insert_data_to_elastic 