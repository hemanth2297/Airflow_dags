from airflow.models import DAG, settings
import logging
from airflow.operators.dummy_operator import DummyOperator

log = logging.getLogger(__name__)


def test2(parent_dag_name, start_date, schedule_interval, parent_dag=None):
    dag = DAG(
        '%s.test2' % parent_dag_name,
        schedule_interval=schedule_interval,
        start_date=start_date
    )

    if len(parent_dag.get_active_runs()) > 0:
        test_list = parent_dag.get_task_instances(settings.Session, start_date=parent_dag.get_active_runs()[-1])[-1].xcom_pull(
            dag_id='%s.%s' % (parent_dag_name, 'test1'),
            task_ids='list')
        if test_list:
            for i in test_list:
                test = DummyOperator(
                    task_id=i,
                    dag=dag
                )

    return dag
