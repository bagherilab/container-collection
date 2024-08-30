import datetime
import secrets
import unittest
from unittest import mock

from docker import APIClient

from container_collection.docker.create_docker_volume import create_docker_volume


def mock_create_volume(**kwargs):
    name = kwargs.get("name", secrets.token_hex(32))
    return {
        "CreatedAt": datetime.datetime.now().astimezone().replace(microsecond=0).isoformat(),
        "Driver": kwargs.get("driver", "local"),
        "Labels": kwargs.get("labels", {"com.docker.volume.anonymous": ""}),
        "Mountpoint": f"/var/lib/docker/volumes/{name}/_data",
        "Name": name,
        "Options": kwargs.get("driver_opts", None),
        "Scope": "local",
    }


class TestCreateDockerVolume(unittest.TestCase):
    def test_create_docker_volume(self):
        client = mock.MagicMock(spec=APIClient)
        client.create_volume.side_effect = mock_create_volume
        path = "/docker/volume/path"

        volume = create_docker_volume(client, path)

        self.assertEqual(path, volume["Options"]["device"])
        self.assertEqual("none", volume["Options"]["type"])
        self.assertEqual("bind", volume["Options"]["o"])


if __name__ == "__main__":
    unittest.main()
