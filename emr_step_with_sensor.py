from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class EmrStepWithSensor(BaseOperator):
    """
    Takes a step and a sensor and combines them into one
    operator that can be retried atomically.

    :param step: the operator
    :type step: airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator
    :param sensor: the sensor
    :type sensor: airflow.contrib.sensors.emr_step_sensor.EmrStepSensor
    :param job_flow_id: the job flow ID
    :type job_flow_id: str
    """
    template_fields = ['job_flow_id']

    @apply_defaults
    def __init__(self, step, sensor, job_flow_id, *args, **kwargs):
        self.step = step
        self.sensor = sensor
        self.job_flow_id = job_flow_id

        if self.step.has_dag() or self.sensor.has_dag():
            raise AirflowException('The step and sensor operators should not be '
                                   'assigned dags when combined with EmrStepWithSensor')
        super(EmrStepWithSensor, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.step.retries = self.sensor.retries = 0
        self.step.job_flow_id = self.sensor.job_flow_id = self.job_flow_id

        step_id = self.step.execute(context)[0]
        self.sensor.step_id = step_id
        self.sensor.execute(context)

        return step_id
