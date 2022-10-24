import boto3
from prefect import task


@task
def submit_batch_job(
    name: str, job_definition_arn: str, user: str, queue: str, size: int
) -> list[str]:
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
