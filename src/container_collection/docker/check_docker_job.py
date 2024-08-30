from typing import Union
from docker import APIClient
from prefect.states import Failed, State
from prefect.context import TaskRunContext

RETRIES_EXCEEDED_EXIT_CODE = 80
"""Exit code used when task run retries exceed the maximum retries."""


def check_docker_job(api_client: APIClient, container_id: str, max_retries: int) -> Union[int, State]:
    context = TaskRunContext.get()

    if context is not None and context.task_run.run_count > max_retries:
        return RETRIES_EXCEEDED_EXIT_CODE

    status = api_client.containers(all=True, filters={"id": container_id})[0]["State"]

    # For jobs that are running, throw the appropriate exception.
    if context is not None and status == "running":
        return Failed()
    if status == "running":
        raise RuntimeError("Job is in RUNNING state and does not have exit code.")

    exitcode = api_client.wait(container_id, timeout=1)["StatusCode"]
    return exitcode
