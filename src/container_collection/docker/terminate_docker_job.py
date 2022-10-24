import docker
from prefect import task


@task
def terminate_docker_job(container_id: str) -> None:
    client = docker.APIClient(base_url="unix://var/run/docker.sock")
    client.stop(container=container_id, timeout=1)
