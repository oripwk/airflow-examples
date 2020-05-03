from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class EmrJobFlowWithSensor(BaseOperator):
    """
    Takes a job_flow and a sensor and combines them into one
    operator that can be retried atomically.

    :param job_flow: the operator
    :type job_flow: airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator
    :param sensor: the sensor
    :type sensor: airflow.contrib.sensors.emr_job_flow_sensor.EmrJobFlowSensor
    """

    @apply_defaults
    def __init__(self, job_flow, sensor, *args, **kwargs):
        self.job_flow = job_flow
        self.sensor = sensor

        if self.job_flow.has_dag() or self.sensor.has_dag():
            raise AirflowException('The job_flow and sensor operators should not be '
                                   'assigned dags when combined with EmrJobFlowWithSensor')
        super(EmrJobFlowWithSensor, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.job_flow.retries = self.sensor.retries = 0

        job_flow_id = self.job_flow.execute(context)
        self.sensor.job_flow_id = job_flow_id
        self.sensor.execute(context)
        return job_flow_id
