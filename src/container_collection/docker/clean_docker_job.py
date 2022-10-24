import docker
from prefect import task


@task
def clean_docker_job(container_id: str) -> None:
    client = docker.APIClient(base_url="unix://var/run/docker.sock")
    status = client.containers(all=True, filters={"id": container_id})[0]["State"]

    if status != "running":
        client.remove_container(container=container_id)
