from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner' : 'atulsaurabh',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 3)
}

with DAG(
    dag_id='db_connection_example_with_postgres',
    default_args=default_args,
    start_date=datetime(2024,4,21, 13),
    schedule_interval='@daily'
) as dag:

    db_insert_task = PostgresOperator(
        task_id='Insert_Data_Task',
        postgres_conn_id ='local_postgres_connection',
        sql=""" 
            insert into rate_enquiry (id,rate_type,user_name,rate) values (1, 'MarginRate','1011123476',84.08)
        """
    )

    db_insert_task