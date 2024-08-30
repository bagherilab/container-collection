from typing import Union

from docker import APIClient
from prefect.context import TaskRunContext
from prefect.states import Failed, State

RETRIES_EXCEEDED_EXIT_CODE = 80
"""Exit code used when task run retries exceed the maximum retries."""


def check_docker_job(
    api_client: APIClient, container_id: str, max_retries: int
) -> Union[int, State]:
    """
    Check for exit code of a Docker container.

    If this task is running within a Prefect flow, it will use the task run
    context to get the current run count. While the run count is below the
    maximum number of retries, the task will continue to attempt to get the exit
    code, and can be called with a retry delay to periodically check the status
    of jobs.

    If this task is not running within a Prefect flow, the ``max_retries``
    parameters is ignored. Jobs that are still running will throw an exception.

    Parameters
    ----------
    api_client
        Docker API client.
    container_id
        ID of container to check.
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

    status = api_client.containers(all=True, filters={"id": container_id})[0]["State"]

    # For jobs that are running, throw the appropriate exception.
    if context is not None and status == "running":
        return Failed()
    if status == "running":
        raise RuntimeError("Job is in RUNNING state and does not have exit code.")

    exitcode = api_client.wait(container_id, timeout=1)["StatusCode"]
    return exitcode
