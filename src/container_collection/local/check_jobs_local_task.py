from datetime import timedelta
import docker
import prefect
from arcadeio.tasks.simulate import CheckJobsTask
from arcadeio.containers.simulate.local import DockerContainer
from arcadeio.notifications import send_slack_notification


class CheckJobsLocalTask(CheckJobsTask):
    """Task for checking job status on local Docker."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = docker.APIClient(base_url="unix://var/run/docker.sock")
        self.state_handlers = [send_slack_notification]

    def run(self, dockers: list[DockerContainer]):
        # Context variables.
        max_retries = prefect.context.max_retries
        retry_delay = prefect.context.retry_delay

        # Set instance variables.
        self.max_retries = max_retries
        self.retry_delay = timedelta(seconds=retry_delay)

        # Get docker container statuses.
        exitcodes = self._get_docker_exitcodes(dockers)
        self._update_container_statuses(dockers, exitcodes)

        # Raise signals depending on exit codes.
        self._raise_check_signals(exitcodes, dockers)

    def _get_docker_exitcodes(self, dockers):
        exitcodes = []

        for doc in dockers:
            if hasattr(doc, "exitcode") and isinstance(doc.exitcode, int):
                exitcodes.append(doc.exitcode)
                continue

            status = self.client.containers(all=True, filters={"id": doc.container})[0]["State"]

            if status == "running":
                exitcode = None
            else:
                exitcode = self.client.wait(doc.container, timeout=1)["StatusCode"]

            exitcodes.append(exitcode)

        return exitcodes

    def _get_log_contents(self, container):
        return self.client.logs(container=container.container).decode()
