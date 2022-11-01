from typing import Optional

import docker
from prefect import task


@task
def run_docker_command(
    image: str,
    command: list[str],
    volume: Optional[docker.models.volumes.Volume] = None,
    environment: Optional[list] = None,
) -> None:
    environment = [] if environment is None else environment
    volumes = {} if volume is None else {volume.name: {"bind": "/mnt", "mode": "rw"}}

    client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    client.containers.run(
        image,
        command,
        environment=environment,
        volumes=volumes,
        auto_remove=True,
    )
