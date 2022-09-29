from time import sleep
import prefect
from prefect import Task
from prefect.utilities.aws import get_boto_client
from arcadeio.containers.simulate.aws import BatchContainer


class TerminateJobsAWSTask(Task):
    """Task for terminating AWS Batch instances."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = get_boto_client("batch")

    def run(self, batches: list[BatchContainer]):
        if prefect.context.flags["terminate"] is False:
            return

        self._terminate_all_jobs(batches)

    def _terminate_all_jobs(self, batches):
        for bat in batches:
            self.client.terminate_job(jobId=bat.job_id, reason="Prefect workflow termination")

        # Pause for 60 seconds for termination.
        sleep(60)
