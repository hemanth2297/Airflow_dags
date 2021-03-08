from datetime import datetime
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from subdags.test1 import test1
from subdags.test2 import test2

DAG_NAME = 'test-dag'

dag = DAG(DAG_NAME,
          description='Test workflow',
          catchup=False,
          schedule_interval='0 0 * * *',
          start_date=datetime(2018, 8, 24))

test1 = SubDagOperator(
    subdag=test1(DAG_NAME,
                 dag.start_date,
                 dag.schedule_interval),
    task_id='test1',
    dag=dag
)

test2 = SubDagOperator(
    subdag=test2(DAG_NAME,
                 dag.start_date,
                 dag.schedule_interval,
                 parent_dag=dag),
    task_id='test2',
    dag=dag,
    provide_context=True)
)

test1 >> test2
