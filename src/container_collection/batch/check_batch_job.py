from time import sleep
from typing import Union

import boto3
import prefect
from prefect import task
from prefect.orion.schemas.states import State, Failed

RETRIES_EXCEEDED_EXIT_CODE = 80


@task
def check_batch_job(job_arn: str, max_retries: int) -> Union[int, State]:
    task_run = prefect.context.get_run_context().task_run  # type: ignore

    if task_run.run_count > max_retries:
        return RETRIES_EXCEEDED_EXIT_CODE

    client = boto3.client("batch")
    response = client.describe_jobs(jobs=[job_arn])["jobs"]

    # Job responses are not immediately available. Wait until available.
    while len(response) != 1:
        sleep(10)
        response = client.describe_jobs(jobs=[job_arn])["jobs"]

    status = response[0]["status"]

    # Wait until job is running or completed.
    while status not in ("RUNNING", "SUCCEEDED", "FAILED"):
        sleep(10)
        response = client.describe_jobs(jobs=[job_arn])["jobs"]
        status = response[0]["status"]

    if status == "RUNNING":
        return Failed()

    exitcode = response[0]["attempts"][0]["container"]["exitCode"]
    return exitcode
