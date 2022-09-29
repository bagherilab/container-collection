import os
import docker
import prefect
from prefect import Task
from arcadeio.containers.simulate.local import DockerContainer
from arcadeio.expressions import LOG_EVENT_FILTER


class SaveLogsLocalTask(Task):
    """Task for saving local Docker logs."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = docker.APIClient(base_url="unix://var/run/docker.sock")

    def run(self, dockers: list[DockerContainer]):
        if prefect.context.flags["log"] is False:
            return

        # Context variables.
        flow_run_name = prefect.context.flow_run_name
        timestamp = prefect.context.today
        directory = prefect.context.working_directory

        log_directory = self._make_log_directory(directory, flow_run_name, timestamp)
        self._copy_container_logs(dockers, log_directory)

    @staticmethod
    def _make_log_directory(directory, name, timestamp):
        log_directory = directory + name + "/" + timestamp + "/logs/"
        os.makedirs(log_directory, exist_ok=True)
        return log_directory

    def _copy_container_logs(self, dockers, directory):
        for doc in dockers:
            log = self.client.logs(container=doc.container).decode()
            log = [line for line in log.split("\n") if LOG_EVENT_FILTER in line]
            with open(directory + doc.container + ".log", "w") as file:
                file.write("\n".join(log))
