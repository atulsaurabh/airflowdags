from datetime import datetime,timedelta
from airflow import DAG
from airflow.operator.bash import BashOperator


default_args = {
    'owner': 'atul saurabh',
    'retries': 5,
    'retry_delay': timedelta(minute = 2)
}

with DAG(
   dag_id= 'first_dag_example',
   default_args=default_args,
   description = 'I am learning Airflow dag and this is my first example',
   start_date=datetime(2024,4,21, 2),
   scheduler_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'echo_task',
        bash_command='echo Hello World, This is first example'
    )


 task1   