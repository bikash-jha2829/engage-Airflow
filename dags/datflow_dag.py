from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.operators.empty import EmptyOperator


import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# dag = DAG('dag_usage_run', default_args=default_args, schedule_interval=None)


def run_curl_command():
    headers = {
        'X-API-KEY': '513bc1c9-30ae-48e0-a8b8-86d6fcbc3428',
        'Content-Type': 'application/json',
    }

    data = {
        "job_name": "testcomposer-daily-aum-rollup-dfl-1-0-0",
        "num_workers": 1,
        "machine_type": "n1-highmem-64",
        "max_num_workers": 4,
        "bq_query": "SELECT id, bucket_id, user_prn, item_type, assets_processed, result, window_end, window_start, window_size, org_id, subscription_id, source_event_timestamp, event_type, footprint_used FROM planet-admin-staging.usage_views.aum",
        "input_source": "planet-admin-staging.usagev3.usage_daily_asset_level_0_0_1",
        "input_parser": "usage_asset_level_0_0_1",
        "aggregators": ["asset_area_under_management_sqkm_v1_0"],
        "windows": [86400],
        "asset_level_output_table": "planet-admin-staging.bikash.qe_usage_daily_asset_level_0_0_1",
        "definition_output": "gs://planet-admin-staging/bikash_test/asset_area_under_management_sqkm_unary_31743",
        "definition_output_cadence": "daily",
        "dateshard_output_table": False,
        "enable_cloud_logging": False
    }

    response = requests.post('https://dataflow-launcher.staging.planet-labs.com/dataflows/new', headers=headers, json=data)
    response.raise_for_status()

    job_id = response.json()['job_id']
    return job_id


def save_job_id(**context):
    job_id = context['task_instance'].xcom_pull(task_ids='run_curl_command')
    context['task_instance'].xcom_push(key='job_id', value=job_id)



with DAG(
        dag_id="checkStatusDataflow",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule=None,
        tags=["RunDataflowJob"],
) as dag:

    start_task = EmptyOperator(task_id='start_task')

    run_curl_command_operator = PythonOperator(
        task_id='run_curl_command',
        python_callable=run_curl_command,
    )

    save_job_id_task = PythonOperator(
        task_id='save_job_id',
        python_callable=save_job_id,
        provide_context=True,
    )

    status_check_bash_operator = BashOperator(
        task_id='status_check_bash_operator',
        bash_command='echo "Job ID: {{ task_instance.xcom_pull("save_job_id", key="job_id") }}"',
        dag=dag,
    )
    
    dataflow_sensor = DataflowJobStatusSensor(
        task_id='check_dataflow_status',
        job_id="{{ task_instance.xcom_pull(task_ids='save_job_id', key='job_id') }}",
        project_id="planet-admin-staging",
        location="us-central1",
        expected_statuses="JOB_STATE_DONE",
        dag=dag
    )


    end_task = EmptyOperator(task_id='end_task', dag=dag)


    start_task >> run_curl_command_operator >> save_job_id_task >> status_check_bash_operator >> dataflow_sensor >> end_task
