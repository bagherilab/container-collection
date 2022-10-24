import docker
from prefect import task


@task
def submit_docker_job(job_definition: dict, path: str) -> str:
    client = docker.APIClient(base_url="unix://var/run/docker.sock")
    host_config = client.create_host_config(binds=[path + ":/mnt"])

    container = client.create_container(**job_definition, host_config=host_config)
    container_id = container.get("Id")

    client.start(container=container_id)

    return container_id
