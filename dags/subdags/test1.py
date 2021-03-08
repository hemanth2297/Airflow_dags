from airflow.models import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

log = logging.getLogger(__name__)


def test1(parent_dag_name, start_date, schedule_interval):
    dag = DAG(
        '%s.test1' % parent_dag_name,
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    def return_list():
        return ['test1', 'test2']

    list_extract_folder = PythonOperator(
        task_id='list',
        dag=dag,
        python_callable=return_list
    )

    clean_xcoms = PostgresOperator(
        task_id='clean_xcoms',
        postgres_conn_id='airflow_db',
        sql="delete from xcom where dag_id='{{ dag.dag_id }}'",
        dag=dag)

    clean_xcoms >> list_extract_folder

    return dag
