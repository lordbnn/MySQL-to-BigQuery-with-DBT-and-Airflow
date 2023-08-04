from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
import pandas_gbq

default_args={
    'owner': 'Byron',
    'retries':2,
    'retry_delay':timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }


def connect_to_sql():
    conn = 'mysql://root:{username}.@{host}:{port}/{db}'
    return conn

def extract_new_data(conn):
    sql_query = '''
    Select * from public.employees
    '''
    df=pd.read_sql(sql_query,conn) #convert to dataframe
    return df
def dbt_run():
    BashOperator(task_id='dbt_run', bash_command='cd /dbt/ && dbt run')
def dbt_test():    
    BashOperator(task_id='dbt_test', bash_command='cd /dbt/ && dbt test')

    
def load_to_bigquery(df,project_id):
    table_id = "credit-direct.byron_creditdirect.employees"
    # Upload the DataFrame to BigQuery
    pandas_gbq.to_gbq(df, table_id, project_id,if_exists='replace')

#returned values
conn = connect_to_sql()
df = extract_new_data(conn)




with DAG(
    dag_id='full_load_data_pipeline_v2',
    description='Moving data from MySQL to BigQuery',
    catchup=False,
    default_args = default_args
    
    ) as dag:
    
    dbt_run=PythonOperator(
        task_id='dbt_run',
        python_callable=dbt_run
        )
    dbt_test=PythonOperator(
        task_id='dbt_test',
        python_callable=dbt_test
        )
    
    extract_new_data_task = PythonOperator(
        task_id = 'extract_new_data_task',
        python_callable = extract_new_data,
        op_args = [conn]
        
    )
    
    load_to_bigquery_task = PythonOperator(
        task_id = 'load_to_bigquery_task',
        python_callable = load_to_bigquery,
        op_args = [df, "credit-direct"]
    )    


dbt_run >> dbt_test >> extract_new_data_task >> load_to_bigquery_task