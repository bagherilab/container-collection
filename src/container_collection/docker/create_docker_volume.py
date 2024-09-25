from docker import APIClient


def create_docker_volume(api_client: APIClient, path: str) -> str:
    """
    Create a docker volume that copies content to specified path.

    Parameters
    ----------
    api_client
        Docker API client.
    path
        Local path for volume.

    Returns
    -------
    :
        Created volume name.
    """

    return api_client.create_volume(
        driver="local", driver_opts={"type": "none", "device": path, "o": "bind"}
    )["Name"]
