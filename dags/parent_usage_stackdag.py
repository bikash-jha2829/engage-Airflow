import pendulum
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

with DAG(
        dag_id="parent_usage_stack_dagrun",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule=None,
        tags=["RunDataflowJob"],
) as dag:
    start_task = EmptyOperator(task_id='start_usage_stack')
    end_task = EmptyOperator(task_id='end_usage_task')

    # Define the TriggerDagRunOperator

    trigger_dag_daily_ordersv2 = TriggerDagRunOperator(
        task_id='trigger_child_daily_ordersv2_job',
        trigger_dag_id='daily_ordersv2_rollup',
        dag=dag,
    )
    trigger_dag_daily_iris_rollup = TriggerDagRunOperator(
        task_id='trigger_child_daily_iris_rollup',
        trigger_dag_id='daily_iris_rollup',
        dag=dag,
    )

    trigger_dag_daily_dapi_rollup = TriggerDagRunOperator(
        task_id='trigger_child_daily_dapi_rollup',
        trigger_dag_id='daily_dapi_rollup',
        dag=dag,
    )
    trigger_dag_daily_usage_adjustments_rollup = TriggerDagRunOperator(
        task_id='trigger_child_daily_usage_adjustments_rollup',
        trigger_dag_id='daily-usage-adjustments-rollup',
        dag=dag,
    )
    trigger_dag_daily_cfd_rollup = TriggerDagRunOperator(
        task_id='trigger_child_daily_cfd_rollup',
        trigger_dag_id='daily_cfd_rollup',
        dag=dag,
    )
    trigger_dag_daily_append_results = TriggerDagRunOperator(
        task_id='trigger_child_daily_append_results',
        trigger_dag_id='daily_append_results',
        dag=dag,
    )
    trigger_dag_daily_aum_rollup = TriggerDagRunOperator(
        task_id='trigger_child_daily_aum_rollup',
        trigger_dag_id='daily_aum_rollup',
        dag=dag,
    )

    trigger_dag_daily_subscription_aum_rollup = TriggerDagRunOperator(
        task_id='trigger_child_daily_subscription_aum_rollup',
        trigger_dag_id='daily_subscription_aum_rollup',
        dag=dag,
    )
    trigger_dag_daily_append_aum_results = TriggerDagRunOperator(
        task_id='trigger_child_daily_append_aum_results',
        trigger_dag_id='daily_append_aum_results',
        dag=dag,
    )

    start_task >> [trigger_dag_daily_ordersv2, trigger_dag_daily_dapi_rollup, trigger_dag_daily_iris_rollup, trigger_dag_daily_usage_adjustments_rollup] >> trigger_dag_daily_cfd_rollup >> [trigger_dag_daily_append_results, trigger_dag_daily_aum_rollup] >> trigger_dag_daily_subscription_aum_rollup >> trigger_dag_daily_append_aum_results >> end_task

    # start_task >> [daily_ordersv2_rollup, daily_dapi_rollup, daily_iris_rollup, daily_usage_adjustments_rollup] >> daily_cfd_rollup >> [daily_append_results, daily_aum_rollup_operator] >> daily_subscription_aum_rollup >> daily_append_results_aum >> save_job_id_task >> dataflow_sensor >> end_task
