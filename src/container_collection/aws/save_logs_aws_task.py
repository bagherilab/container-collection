import time
import prefect
from prefect import Task
from prefect.utilities.aws import get_boto_client
from arcadeio.containers.simulate.aws import BatchContainer
from arcadeio.expressions import LOG_EVENT_FILTER


class SaveLogsAWSTask(Task):
    """Task for saving AWS Batch logs."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = {
            "batch": get_boto_client("batch"),
            "logs": get_boto_client("logs"),
            "s3": get_boto_client("s3"),
        }

    def run(self, batches: list[BatchContainer]):
        if prefect.context.flags["log"] is False:
            return

        # Context variables.
        flow_run_name = prefect.context.flow_run_name
        timestamp = prefect.context.today
        bucket = prefect.context.working_bucket

        log_folder = self._parse_log_folder(flow_run_name, timestamp)
        log_streams = self._get_log_streams(batches)
        log_events = self._filter_log_events(log_streams)
        self._save_log_events(log_events, bucket, log_folder)

    @staticmethod
    def _parse_log_folder(name, timestamp):
        log_folder = name + "/" + timestamp + "/logs"
        return log_folder

    def _get_log_streams(self, batches):
        log_streams = []
        for bat in batches:
            status = self.client["batch"].describe_jobs(jobs=[bat.job_id])["jobs"][0]["status"]
            if status in ("RUNNING", "SUCCEEDED", "FAILED"):
                log_streams.append(bat.log_stream)
        return log_streams

    def _filter_log_events(self, log_streams):
        all_log_events = {}

        for log_stream in log_streams:
            log_events = []

            response = self.client["logs"].filter_log_events(
                logGroupName="/aws/batch/job",
                logStreamNames=[log_stream],
                filterPattern=LOG_EVENT_FILTER,
            )

            while "nextToken" in response:
                log_events = log_events + [event["message"] for event in response["events"]]

                response = self.client["logs"].filter_log_events(
                    logGroupName="/aws/batch/job",
                    logStreamNames=[log_stream],
                    filterPattern=LOG_EVENT_FILTER,
                    nextToken=response["nextToken"]
                )

            all_log_events[log_stream] = log_events

        return all_log_events

    def _save_log_events(self, log_events, bucket, folder):
        for log_stream, log_event in log_events.items():
            log_key = "/".join([folder, log_stream.split("/")[-1] + ".log"])
            body = "\n".join(log_event).encode('ascii')
            self.client["s3"].put_object(Bucket=bucket, Key=log_key, Body=body)
