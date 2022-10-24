import docker
from prefect import task


@task
def get_docker_logs(container_id: str, log_filter: str) -> str:
    client = docker.APIClient(base_url="unix://var/run/docker.sock")
    logs = client.logs(container=container_id).decode("utf-8")

    log_items = logs.split("\n")
    if "-" in log_filter:
        log_filter = log_filter.replace("-", "")
        log_items = [item for item in log_items if log_filter not in item]
    else:
        log_items = [item for item in log_items if log_filter in item]

    return "\n".join(log_items)
