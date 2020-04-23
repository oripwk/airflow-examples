from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class EmrStepWithSensor(BaseOperator):
    """
    Takes a step and a sensor and combines them into one
    operator that can be retried atomically.

    :param step: the operator
    :type step: EmrAddStepsOperator
    :param sensor: the sensor
    :type sensor: EmrJobFlowSensor
    """
    template_fields = ['job_flow_id']

    @apply_defaults
    def __init__(self, step, sensor, job_flow_id, *args, **kwargs):
        self.step = step
        self.sensor = sensor
        self.job_flow_id = job_flow_id
        super(EmrStepWithSensor, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.step.retries = self.sensor.retries = 0
        self.step.job_flow_id = self.sensor.job_flow_id = self.job_flow_id

        step_id = self.step.execute(context)[0]
        self.sensor.step_id = step_id
        self.sensor.execute(context)

        return step_id
