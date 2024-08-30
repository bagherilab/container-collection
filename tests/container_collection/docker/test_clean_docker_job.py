import secrets
import unittest
from unittest import mock

from docker import APIClient

from container_collection.docker.clean_docker_job import clean_docker_job


class TestCleanDockerJob(unittest.TestCase):
    def test_clean_docker_job_state_running(self):
        container_id = secrets.token_hex(32)
        client = mock.MagicMock(spec=APIClient)
        client.containers.return_value = [{"State": "running"}]
        client.remove_container = mock.Mock()

        clean_docker_job(client, container_id)

        client.containers.assert_called_with(all=True, filters={"id": container_id})
        client.remove_container.assert_not_called()

    def test_clean_docker_job_state_exited(self):
        container_id = secrets.token_hex(32)
        client = mock.MagicMock(spec=APIClient)
        client.containers.return_value = [{"State": "exited"}]
        client.remove_container = mock.Mock()

        clean_docker_job(client, container_id)

        client.containers.assert_called_with(all=True, filters={"id": container_id})
        client.remove_container.assert_called_with(container=container_id)


if __name__ == "__main__":
    unittest.main()
