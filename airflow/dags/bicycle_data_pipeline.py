from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from airflow.models import Variable

if not Variable.get("processing_date", default_var=None):
    Variable.set("processing_date", "2023-12-07")
    
processing_date = Variable.get("processing_date")
# DAG Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


# DAG Definition
with DAG(
    'bicycle_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for processing bicycle theft data',
    schedule_interval=None,  
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['bicycle_data', 'hdfs', 'spark', 'postgres'],
) as dag:

    send_data_to_hdfs = SimpleHttpOperator( 
        task_id='send_data_to_hdfs',
        http_conn_id='data_ingestion_service',
        endpoint=f"/send_data_by_date?date={processing_date}",  # Gửi date qua query
        method='POST',  # POST vẫn hợp lệ nếu không có body
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )


    process_data_with_spark = SparkSubmitOperator(
        task_id='process_data_with_spark',
        application='app/app.py',  
        conn_id='spark_default',
        executor_cores=2,
        executor_memory='2g',
        name='bicycle_data_processing',
        jars='jars/postgresql-42.7.4.jar',
        verbose=True,
    )


    send_data_to_hdfs >> process_data_with_spark
