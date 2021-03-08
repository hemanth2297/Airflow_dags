import datetime as dt
import os
import sys
sys.path.append("/opt/bitnami/airflow/dags/local/dag_folder/DAG_1")
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from sample import test

def print_world():
    print(test())
    print('world')


default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}


with DAG('airflow_tutorial_v01',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         ) as dag:

    print_hello = BashOperator(task_id='print_hello',
                               bash_command='echo "hello"')
    sleep = BashOperator(task_id='sleep',
                         bash_command='sleep 5')
    print_world = PythonOperator(task_id='print_world',
                                 python_callable=print_world)
                                 
print_hello >> sleep >> print_world

