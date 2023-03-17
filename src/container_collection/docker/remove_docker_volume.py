import docker


def remove_docker_volume(volume: docker.models.volumes.Volume) -> None:
    volume.remove()
