import unittest
from unittest import mock

from docker import APIClient

from container_collection.docker.remove_docker_volume import remove_docker_volume


class TestRemoveDockerVolume(unittest.TestCase):
    def test_remove_docker_volume(self):
        client = mock.MagicMock(spec=APIClient)
        volume = "volume-name"

        remove_docker_volume(client, volume)

        client.remove_volume.assert_called_with(volume)


if __name__ == "__main__":
    unittest.main()
