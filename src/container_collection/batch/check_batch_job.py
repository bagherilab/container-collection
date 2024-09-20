from __future__ import annotations

from time import sleep

import boto3
from prefect.context import TaskRunContext
from prefect.states import Failed, State

RETRIES_EXCEEDED_EXIT_CODE = 80
"""Exit code used when task run retries exceed the maximum retries."""


def check_batch_job(job_arn: str, max_retries: int) -> int | State | bool:
    """
    Check for exit code of an AWS Batch job.

    If this task is running within a Prefect flow, it will use the task run
    context to get the current run count. While the run count is below the
    maximum number of retries, the task will continue to attempt to get the exit
    code, and can be called with a retry delay to periodically check the status
    of jobs.

    If this task is not running within a Prefect flow, the ``max_retries``
    parameters is ignored. Jobs that are still running will throw an exception.

    Parameters
    ----------
    job_arn
        Job ARN.
    max_retries
        Maximum number of retries.

    Returns
    -------
    :
        Exit code if the job is complete, otherwise throws an exception.
    """

    context = TaskRunContext.get()

    if context is not None and context.task_run.run_count > max_retries:
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

    # For jobs that are running, throw the appropriate exception.
    if context is not None and status == "RUNNING":
        return Failed()
    if status == "RUNNING":
        message = "Job is in RUNNING state and does not have exit code."
        raise RuntimeError(message)

    return response[0]["attempts"][0]["container"]["exitCode"]
