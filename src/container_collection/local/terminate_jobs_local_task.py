import docker
import prefect
from prefect import Task
from arcadeio.containers.simulate.local import DockerContainer


class TerminateJobsLocalTask(Task):
    """Task for terminating local Docker instances."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = docker.APIClient(base_url="unix://var/run/docker.sock")

    def run(self, dockers: list[DockerContainer]):
        if prefect.context.flags["terminate"] is False:
            return

        self._stop_running_containers(dockers)

    def _stop_running_containers(self, dockers):
        for doc in dockers:
            self.client.stop(container=doc.container, timeout=1)
