from prefect import task
import docker


@task
def create_docker_volume(path: int) -> docker.models.volumes.Volume:
    client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    volume = client.volumes.create(
        driver="local", driver_opts={"type": "none", "device": path, "o": "bind"}
    )
    return volume
