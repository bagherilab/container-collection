from time import sleep
from typing import Union

import boto3
import prefect
from prefect import task
from prefect.orion.schemas.states import State, Failed

RETRIES_EXCEEDED_EXIT_CODE = 80


@task
def check_fargate_task(cluster: str, task_arn: str, max_retries: int) -> Union[int, State]:
    task_run = prefect.context.get_run_context().task_run  # type: ignore

    if task_run.run_count > max_retries:
        return RETRIES_EXCEEDED_EXIT_CODE

    client = boto3.client("ecs")
    response = client.describe_tasks(cluster=cluster, tasks=[task_arn])["tasks"]

    # Task responses are not immediately available. Wait until available.
    while len(response) != 1:
        sleep(10)
        response = client.describe_tasks(cluster=cluster, tasks=[task_arn])["tasks"]

    status = response[0]["lastStatus"]

    # Wait until task is running or stopped.
    while status not in ("RUNNING", "STOPPED"):
        sleep(10)
        response = client.describe_tasks(cluster=cluster, tasks=[task_arn])["tasks"]
        status = response[0]["lastStatus"]

    if status == "RUNNING":
        return Failed()

    exitcode = response[0]["containers"][0]["exitCode"]
    return exitcode
