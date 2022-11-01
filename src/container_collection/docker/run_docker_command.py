from typing import Optional

import docker
from prefect import task
from prefect.blocks.system import Secret


@task
def run_docker_command(
    image: str,
    command: list[str],
    volume: Optional[docker.models.volumes.Volume],
    credentials: bool = False,
) -> None:
    client = docker.DockerClient(base_url="unix://var/run/docker.sock")

    if volume is None:
        environment = []

        if credentials:
            aws_access_key_id = Secret.load("aws-access-key-id").get()
            aws_secret_access_key = Secret.load("aws-secret-access-key").get()
            environment.append(f"AWS_ACCESS_KEY_ID={aws_access_key_id}")
            environment.append(f"AWS_SECRET_ACCESS_KEY={aws_secret_access_key}")

        client.containers.run(image, command, environment=environment, auto_remove=True)
    else:
        client.containers.run(
            image, command, volumes={volume.name: {"bind": "/mnt", "mode": "rw"}}, auto_remove=True
        )
