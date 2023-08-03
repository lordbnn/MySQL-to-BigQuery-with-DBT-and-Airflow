from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd

default_args={
    'owner': 'Byron',
    'retries':2,
    'retry_delay':timedelta(minutes=5)
    }


def connect_to_sql():
    conn = create_engine('postgresql://airflow:airflow@host.docker.internal:5436/postgres')
    #connection_url = 'mysql://root:Obiageli5.@localhost:3306/employees'
    return conn

def extract_new_data(conn):
    sql_query = '''
    Select * from employees
    '''
    df=pd.read_sql(sql_query,conn)
    return df

def load_to_bigquery(df,bg_con):
    df.to_sql('employees',bg_con,if_exists='replace', schema='public')


conn = connect_to_sql()
df = extract_new_data(conn)




with DAG(
    dag_id='full_load_data_pipeline_v2',
    description='Moving data from MySQL to BigQuery',
    start_date=datetime(2023,8,3),
    schedule_interval=None,
    catchup=False,
    default_args = default_args
    
    ) as dag:
    
    dbt_run=BashOperator(
        task_id='dbt_run',
        bash_command='cd /dbt/ && dbt run'
        )
    
    extract_new_data_task = PythonOperator(
        task_id = 'extract_new_data_task',
        python_callable = extract_new_data,
        op_args = [conn]
        
    )
    
    load_to_bigquery_task = PythonOperator(
        task_id = 'load_to_bigquery_task',
        python_callable = load_to_bigquery,
        op_args = [df, conn]
    )    


