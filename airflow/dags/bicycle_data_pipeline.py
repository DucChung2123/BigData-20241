from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='bicycle_data_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    catchup=False,
) as dag:

    # 1. Get date for filtering data
    # Option 1: Use Params (passed when triggering the DAG)
    date_param = "{{ params.date }}"  # Pass this when triggering the DAG

    # Option 2: Use Variables (set globally in Airflow Admin > Variables)
    default_date = Variable.get("default_date", default_var=datetime.now().strftime('%Y-%m-%d'))

    # Option 3: Use Macros (e.g., {{ ds }} for the execution date)
    execution_date = "{{ ds }}"  # Automatically fetch DAG run execution date

    # Task 1: Send data to HDFS via API
    send_data_to_hdfs = SimpleHttpOperator(
        task_id='send_data_to_hdfs',
        http_conn_id='data_ingestion_service',  # HTTP Connection ID
        endpoint='/send_data_by_date',
        method='POST',
        data={
            "date": date_param  # Change to `default_date` or `execution_date` if preferred
        },
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )

    # Task 2: Run Spark job to process data
    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/opt/spark/app/app.py',  # Path to Spark job in the container
        conn_id='spark_default',  # Spark connection ID
        jars='/opt/spark/jars/postgresql-42.7.4.jar',  # JDBC driver
        verbose=True,
    )

    # Define task dependencies
    send_data_to_hdfs >> run_spark_job
