from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def print_message(**kwargs):
    print("Hello World from new dag")


default_args = {
    'owner': 'atul saurabh',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id ='operation_with_python_operator',
    start_date=datetime(2024,4,22,11),
    default_args=default_args,
    schedule_interval='@daily'
) as dag:
    db_fetch_task = PythonOperator(
        task_id='python_operator_task',
        python_callable=print_message,
        dag=dag,
        provide_context=True
    )

    db_fetch_task