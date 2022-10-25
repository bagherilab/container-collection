import docker
from prefect import task


@task
def run_docker_command(image: str, command: list[str]) -> None:
    client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    container = client.containers.run(image, command, detach=True)
    container.wait()
