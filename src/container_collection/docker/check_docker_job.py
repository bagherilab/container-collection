from typing import Union

import docker
import prefect
from prefect import task
from prefect.orion.schemas.states import State, Failed

RETRIES_EXCEEDED_EXIT_CODE = 80


@task
def check_docker_job(container_id: str, max_retries: int) -> Union[int, State]:
    task_run = prefect.context.get_run_context().task_run  # type: ignore

    if task_run.run_count > max_retries:
        return RETRIES_EXCEEDED_EXIT_CODE

    client = docker.APIClient(base_url="unix://var/run/docker.sock")
    status = client.containers(all=True, filters={"id": container_id})[0]["State"]

    if status == "running":
        return Failed()

    exitcode = client.wait(container_id, timeout=1)["StatusCode"]
    return exitcode
