from docker import APIClient


def get_docker_logs(api_client: APIClient, container_id: str, log_filter: str) -> str:
    """
    Get logs for Docker job.

    Parameters
    ----------
    api_client
        Docker API client.
    container_id
        Docker container ID.
    log_filter
        Filter for log events (use "-" for exclusion).

    Returns
    -------
    :
        All filtered log events.
    """

    logs = api_client.logs(container=container_id).decode("utf-8")

    log_items = logs.split("\n")
    if "-" in log_filter:
        log_filter = log_filter.replace("-", "")
        log_items = [item for item in log_items if log_filter not in item]
    else:
        log_items = [item for item in log_items if log_filter in item]

    return "\n".join(log_items)
