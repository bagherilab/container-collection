from docker import APIClient


def submit_docker_job(api_client: APIClient, job_definition: dict, volume_name: str) -> str:
    """
    Submit Docker job.

    Parameters
    ----------
    api_client
        Docker API client.
    job_definition
        Docker job definition used to create job container.
    volume_name
        Name of the docker volume.

    Returns
    -------
    :
        Container ID.
    """

    host_config = api_client.create_host_config(binds={volume_name: {"bind": "/mnt", "mode": "rw"}})

    container = api_client.create_container(**job_definition, host_config=host_config)
    container_id = container.get("Id")

    api_client.start(container=container_id)

    return container_id
