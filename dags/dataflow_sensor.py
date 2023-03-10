from __future__ import annotations

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor

with DAG(
        dag_id="checkStatusDataflow",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule=None,
        tags=["Test"],
) as dag:
    start_task = EmptyOperator(task_id='start_task')

    dataflow_sensor = DataflowJobStatusSensor(
        task_id='dataflow_job_status_sensor',
        job_id="2023-03-10_06_12_14-2399155084892943273",
        project_id="planet-admin-staging",
        location="us-central1",
        expected_statuses="JOB_STATE_DONE",
        dag=dag
    )

    end_task = EmptyOperator(task_id='end_task', dag=dag)

    start_task >> dataflow_sensor >> end_task
