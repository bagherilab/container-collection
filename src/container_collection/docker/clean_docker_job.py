from docker import APIClient


def clean_docker_job(api_client: APIClient, container_id: str) -> None:
    """
    Clean up container if it is not currently running.

    Parameters
    ----------
    api_client
        Docker API client.
    container_id
        ID of container to remove.
    """

    status = api_client.containers(all=True, filters={"id": container_id})[0]["State"]

    if status != "running":
        api_client.remove_container(container=container_id)
