from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.mysql_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Byron',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'incremental_load_dag',
    default_args=default_args,
    description='DAG for incremental load from MySQL to BigQuery',
    schedule_interval=timedelta(days=1),  # Run once per day
    
    ) as dag:
    
    dbt_run=BashOperator(
        task_id='dbt_run', bash_command='cd /dbt/ && dbt run'
        )
    
    dbt_test=BashOperator(
        task_id='dbt_test', bash_command='cd /dbt/ && dbt test'
        )  
      
    mysql_to_bigquery_task = MySqlOperator(
        task_id='mysql_to_bigquery',
        sql="SELECT * FROM employees WHERE last_modified >= '{{ ds }}'",
        mysql_conn_id='mysql_conn',
        bigquery_conn_id='bigquery_conn',
        write_disposition='WRITE_APPEND',  # Append data in BigQuery on each run
        destination_dataset_table='project_id.dataset_id.employees',
    
)
    

dbt_run >> dbt_test >> mysql_to_bigquery_task