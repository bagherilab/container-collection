from docker import APIClient


def terminate_docker_job(api_client: APIClient, container_id: str) -> None:
    """
    Terminate specific docker container.

    Parameters
    ----------
    api_client
        Docker API client.
    container_id
        ID of container to terminate.
    """

    api_client.stop(container=container_id, timeout=1)
