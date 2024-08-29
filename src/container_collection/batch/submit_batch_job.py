from typing import Any

import boto3


def submit_batch_job(
    name: str,
    job_definition_arn: str,
    user: str,
    queue: str,
    size: int,
    **kwargs: Any,
) -> list[str]:
    """
    Submit AWS Batch job.

    Parameters
    ----------
    name
        Job name.
    job_definition_arn
        Job definition ARN.
    user
        User name prefix for job name.
    queue
        Job queue.
    size
        Number of jobs in array.
    **kwargs
        Additional parameters for job submission. The keyword arguments are
        passed to `boto3` Batch client method `submit_job`.


    Returns
    -------
    :
        List of job ARNs.
    """

    default_job_submission = {
        "jobName": f"{user}_{name}",
        "jobQueue": queue,
        "jobDefinition": job_definition_arn,
    }

    if size > 1:
        default_job_submission["arrayProperties"] = {"size": size}  # type: ignore

    client = boto3.client("batch")
    job_submission = default_job_submission | kwargs
    response = client.submit_job(**job_submission)

    if size > 1:
        return [f"{response['jobArn']}:{i}" for i in range(size)]

    return [response["jobArn"]]
