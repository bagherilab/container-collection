import boto3


def submit_batch_job(
    name: str, job_definition_arn: str, user: str, queue: str, size: int
) -> list[str]:
    """
    Submit job to AWS Batch.

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

    Returns
    -------
    :
        List of job ARNs.
    """

    job_submission = {
        "jobName": f"{user}_{name}",
        "jobQueue": queue,
        "jobDefinition": job_definition_arn,
    }

    if size > 1:
        job_submission["arrayProperties"] = {"size": size}  # type: ignore

    client = boto3.client("batch")
    response = client.submit_job(**job_submission)

    if size > 1:
        return [f"{response['jobArn']}:{i}" for i in range(size)]

    return [response["jobArn"]]
