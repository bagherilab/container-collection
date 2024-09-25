import unittest
from unittest import mock

from docker import APIClient

from container_collection.docker.create_docker_volume import create_docker_volume


class TestCreateDockerVolume(unittest.TestCase):
    def test_create_docker_volume(self):
        expected_volume = "volume-name"
        client = mock.MagicMock(spec=APIClient)
        client.create_volume.return_value = {"Name": expected_volume}
        path = "/docker/volume/path"

        volume = create_docker_volume(client, path)

        client.create_volume.assert_called_with(
            driver="local", driver_opts={"type": "none", "device": path, "o": "bind"}
        )
        self.assertEqual(expected_volume, volume)


if __name__ == "__main__":
    unittest.main()
