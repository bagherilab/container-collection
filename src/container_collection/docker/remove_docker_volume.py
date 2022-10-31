from prefect import task
import docker


@task
def remove_docker_volume(volume: docker.models.volumes.Volume) -> None:
    volume.remove()
