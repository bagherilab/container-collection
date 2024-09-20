import secrets
import unittest
from unittest import mock

from docker import APIClient

from container_collection.docker.run_docker_command import run_docker_command


class TestRunDockerCommand(unittest.TestCase):
    def test_run_docker_command_no_optional_parameters(self):
        client = mock.MagicMock(spec=APIClient)
        image = "jobimage:latest"
        command = ["command", "string"]

        run_docker_command(client, image, command, detach=False)

        client.containers.run.assert_called_with(
            image, command, environment=[], volumes={}, auto_remove=True, detach=False
        )

    def test_run_docker_command_with_optional_parameters(self):
        client = mock.MagicMock(spec=APIClient)
        image = "jobimage:latest"
        command = ["command", "string"]
        environment = [
            "ENVIRONMENT_VARIABLE_A=X",
            "ENVIRONMENT_VARIABLE_B=Y",
        ]
        volume_name = secrets.token_hex(32)
        detach = True

        run_docker_command(
            client, image, command, environment=environment, volume_name=volume_name, detach=detach
        )

        client.containers.run.assert_called_with(
            image,
            command,
            environment=environment,
            volumes={volume_name: {"bind": "/mnt", "mode": "rw"}},
            auto_remove=True,
            detach=detach,
        )


if __name__ == "__main__":
    unittest.main()
