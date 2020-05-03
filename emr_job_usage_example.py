from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago

from emr_job_flow_with_sensor import EmrJobFlowWithSensor
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

job = EmrJobFlowWithSensor(
    task_id='job_and_retry',
    job_flow=EmrCreateJobFlowOperator(
        task_id='job',
        job_flow_overrides=job_conf
    ),
    sensor=EmrJobFlowSensor(
        task_id='sensor',
        job_flow_id=''
    ),
    dag=dag
)
