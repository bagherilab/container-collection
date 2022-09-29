from datetime import timedelta
import prefect
import pendulum
from prefect.utilities.aws import get_boto_client
from prefect.engine.signals import RETRY
from arcadeio.tasks.simulate import CheckJobsTask
from arcadeio.containers.simulate.aws import BatchContainer
from arcadeio.notifications import send_slack_notification


class CheckJobsAWSTask(CheckJobsTask):
    """Task for checking job status on AWS Batch."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = get_boto_client("batch")
        self.log = get_boto_client("logs")
        self.state_handlers = [send_slack_notification]

    def run(self, batches: list[BatchContainer]):
        # Context variables.
        max_retries = prefect.context.max_retries
        retry_delay = prefect.context.retry_delay

        # Set instance variables.
        self.max_retries = max_retries
        self.retry_delay = timedelta(seconds=retry_delay)

        # Get batch container statuses.
        exitcodes = self._get_batch_exitcodes(batches)
        self._update_container_statuses(batches, exitcodes)

        # Raise signals depending on exit codes.
        self._raise_check_signals(exitcodes, batches)

    def _get_batch_exitcodes(self, batches):
        exitcodes = []

        for bat in batches:
            if hasattr(bat, "exitcode") and isinstance(bat.exitcode, int):
                exitcodes.append(bat.exitcode)
                continue

            response = self.client.describe_jobs(jobs=[bat.job_id])["jobs"]

            # Job responses are not immediately available. Retry after 10 seconds.
            if len(response) != 1:
                run_count = prefect.context.get("task_run_count", 1)
                prefect.context.update(task_run_count=run_count - 1)
                start_time = pendulum.now("utc") + timedelta(seconds=10)
                raise RETRY(result=batches, start_time=start_time)

            status = response[0]["status"]

            if status == "RUNNING":
                exitcode = None
                if not hasattr(bat, "log_stream"):
                    bat.log_stream = response[0]["container"]["logStreamName"]
            elif status in ("SUCCEEDED", "FAILED"):
                exitcode = response[0]["attempts"][0]["container"]["exitCode"]
                if not hasattr(bat, "log_stream"):
                    bat.log_stream = response[0]["container"]["logStreamName"]
            else:
                exitcode = status

            exitcodes.append(exitcode)

        return exitcodes

    def _get_log_contents(self, container):
        response = self.log.get_log_events(
            logGroupName="/aws/batch/job", logStreamName=container.log_stream
        )
        return "".join([event["message"] for event in response["events"]])
