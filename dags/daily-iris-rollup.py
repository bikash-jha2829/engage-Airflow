from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id="daily_iris_rollup",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule=None,
        tags=["daily-iris-rollup", "dataflow_job", "planet-admin-staging"],
) as dag:
    start_task = EmptyOperator(task_id='start_task')

    daily_iris_rollup = EmptyOperator(task_id='daily-iris-rollup')

    check_dataflow_status = EmptyOperator(task_id='check_daily-iris-rollup-job-status')
    end_task = EmptyOperator(task_id='end_task')

    start_task >> daily_iris_rollup >> check_dataflow_status >> end_task
