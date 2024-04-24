from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'atul saurabh',
    'retries': 5,
    'retry_delay': timedelta(minutes = 2)
}

with DAG(
   dag_id= 'first_dag_example',
   default_args=default_args,
   description = 'I am learning Airflow dag and this is my first example',
   start_date=datetime(2024,4,21, 2),
   schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'echo_task',
        bash_command='echo Hello World, This is first example'
    )

    task2 = BashOperator(
        task_id = 'again_echo_task',
        bash_command = 'echo Hi buddy How are you?'
    )


    task1.set_downstream(task2)