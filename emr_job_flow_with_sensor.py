from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class EmrJobFlowWithSensor(BaseOperator):
    """
    Takes a job_flow and a sensor and combines them into one
    operator that can be retried atomically.

    :param job_flow: the operator
    :type job_flow: EmrCreateJobFlowOperator
    :param sensor: the sensor
    :type sensor: EmrJobFlowSensor
    """
    @apply_defaults
    def __init__(self, job_flow, sensor, *args, **kwargs):
        self.job_flow = job_flow
        self.sensor = sensor
        self.job_flow.retries = 0
        self.sensor.retries = 0
        super(EmrJobFlowWithSensor, self).__init__(*args, **kwargs)

    def execute(self, context):
        job_flow_id = self.job_flow.execute(context)
        self.sensor.job_flow_id = job_flow_id
        self.sensor.execute(context)
