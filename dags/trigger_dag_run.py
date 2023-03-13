from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG('master_usage_dagrun', default_args=default_args, schedule_interval='@once')

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
