from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from docker import DockerClient


def run_docker_command(
    client: DockerClient,
    image: str,
    command: list[str],
    volume: str | None = None,
    environment: list | None = None,
    *,
    detach: bool,
) -> None:
    """
    Run container from image with given command.

    Parameters
    ----------
    client
        Docker API client.
    image
        Docker image.
    command
        Command list passed to container.
    volume
        Name of the docker volume.
    environment
        List of environment variables as strings.
    detach
        True to start container and immediately return the Container object,
        False otherwise.
    """

    environment = [] if environment is None else environment
    volumes = {} if volume is None else {volume: {"bind": "/mnt", "mode": "rw"}}

    client.containers.run(
        image,
        command,
        environment=environment,
        volumes=volumes,
        auto_remove=True,
        detach=detach,
    )
