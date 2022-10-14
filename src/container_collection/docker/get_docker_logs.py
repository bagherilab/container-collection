import docker
from prefect import task


@task
def get_docker_logs(container_id: str) -> str:
    client = docker.APIClient(base_url="unix://var/run/docker.sock")
    logs = client.logs(container=container_id).decode("utf-8")
    return logs
