import boto3
from prefect import task


@task
def get_batch_logs(job_arn: str, log_filter: str) -> str:
    client = boto3.client("batch")
    response = client.describe_jobs(jobs=[job_arn])["jobs"][0]
    log_stream = response["container"]["logStreamName"]

    client = boto3.client("logs")
    log_events: list[str] = []

    response = client.filter_log_events(
        logGroupName="/aws/batch/job",
        logStreamNames=[log_stream],
        filterPattern=log_filter,
    )

    while "nextToken" in response:
        log_events = log_events + [event["message"] for event in response["events"]]

        response = client.filter_log_events(
            logGroupName="/aws/batch/job",
            logStreamNames=[log_stream],
            filterPattern=log_filter,
            nextToken=response["nextToken"],
        )

    return "\n".join(log_events)
