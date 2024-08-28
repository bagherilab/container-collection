import boto3

LOG_GROUP_NAME = "/aws/batch/job"
"""AWS Batch log group name."""


def get_batch_logs(job_arn: str, log_filter: str) -> str:
    """
    Get logs for AWS Batch job.

    Parameters
    ----------
    job_arn
        Job ARN.
    log_filter
        Filter for log events.

    Returns
    -------
    :
        All filtered log events.
    """

    client = boto3.client("batch")
    response = client.describe_jobs(jobs=[job_arn])["jobs"][0]
    log_stream = response["container"]["logStreamName"]

    client = boto3.client("logs")
    log_events: list[str] = []

    response = client.filter_log_events(
        logGroupName=LOG_GROUP_NAME,
        logStreamNames=[log_stream],
        filterPattern=log_filter,
    )

    if response["events"]:
        log_events = log_events + [event["message"] for event in response["events"]]

    while "nextToken" in response:
        response = client.filter_log_events(
            logGroupName="/aws/batch/job",
            logStreamNames=[log_stream],
            filterPattern=log_filter,
            nextToken=response["nextToken"],
        )

        log_events = log_events + [event["message"] for event in response["events"]]

    return "\n".join(log_events)
