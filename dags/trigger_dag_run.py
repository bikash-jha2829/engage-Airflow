from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
        dag_id="master_usage_dagrun",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule=None,
        tags=["RunDataflowJob"],
) as dag:
    start_task = EmptyOperator(task_id='start_usage_stack')
    end_task = EmptyOperator(task_id='end_usage_task')

# Define the TriggerDagRunOperator
    trigger_dag_op = TriggerDagRunOperator(
        task_id='trigger_child_aum_job',
        trigger_dag_id='target_dag_id',
        dag=dag,
    )

    start_task >> trigger_dag_op >> end_task

# target_dag_id is the ID of the DAG you want to trigger
