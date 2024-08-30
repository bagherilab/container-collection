from typing import Optional

from docker import DockerClient


def run_docker_command(
    client: DockerClient,
    image: str,
    command: list[str],
    volume_name: Optional[str] = None,
    environment: Optional[list] = None,
    detach: bool = False,
) -> None:
    """
    Run container from image with given command.

    Parameters
    ----------
    api_client
        Docker API client.
    image
        Docker image.
    command
        Command list passed to container.
    volume_name
        Name of the docker volume.
    environment
        List of environment variables as strings.
    detach
        True to start container and immediately return the Container object,
        False otherwise.
    """

    environment = [] if environment is None else environment
    volumes = {} if volume_name is None else {volume_name: {"bind": "/mnt", "mode": "rw"}}

    client.containers.run(
        image,
        command,
        environment=environment,
        volumes=volumes,
        auto_remove=True,
        detach=detach,
    )
