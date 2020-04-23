from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago

from emr_step_with_sensor import EmrStepWithSensor

# the job flow step configuration as described here:
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow
step_conf = {}
job_conf = {}

dag = DAG(
    dag_id='spark_job',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1)
    }
)

job = EmrCreateJobFlowOperator(
    task_id='job',
    job_flow_overrides=job_conf,
    dag=dag
)

job_flow_id = "{{ task_instance.xcom_pull(task_ids='job', key='return_value') }}"

step1 = EmrStepWithSensor(
    task_id='step1_and_retry',
    job_flow_id=job_flow_id,
    step=EmrAddStepsOperator(
        task_id='step1',
        job_flow_id='',
        steps=[step_conf]
    ),
    sensor=EmrStepSensor(
        task_id='step1_sensor',
        job_flow_id='',
        step_id=''
    ),
    dag=dag
)

job >> step1
