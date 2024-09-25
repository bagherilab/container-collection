from docker import APIClient


def remove_docker_volume(api_client: APIClient, volume: str) -> None:
    """
    Remove docker volume.

    Parameters
    ----------
    api_client
        Docker API client.
    volume
        Name of the docker volume.
    """

    api_client.remove_volume(volume)
