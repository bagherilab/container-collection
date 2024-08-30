import unittest
from unittest import mock

from docker import APIClient

from container_collection.docker.terminate_docker_job import terminate_docker_job


class TestTerminateDockerJob(unittest.TestCase):
    def test_terminate_docker_job(self):
        client = mock.MagicMock(spec=APIClient)
        container_id = "container-id"

        terminate_docker_job(client, container_id)

        client.stop.assert_called_with(container=container_id, timeout=1)


if __name__ == "__main__":
    unittest.main()
