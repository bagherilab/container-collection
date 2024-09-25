import unittest
from unittest import mock

from docker import APIClient

from container_collection.docker.submit_docker_job import submit_docker_job


def mock_create_host_config(**kwargs):
    host_config = {"NetworkMode": "default"}

    if "binds" in kwargs:
        binds = []
        for key, values in kwargs["binds"].items():
            combined = f"{key}:{values['bind']}:{values['mode']}"
            if "propagation" in values:
                combined = f"{combined},{values['propagation']}"
            binds.append(combined)
        host_config["Binds"] = binds

    return host_config


class TestSubmitDockerJob(unittest.TestCase):
    def test_submit_docker_job(self):
        container_id = "container-id"
        client = mock.MagicMock(spec=APIClient)
        client.create_host_config.side_effect = mock_create_host_config
        client.create_container.return_value = {"Id": container_id, "Warnings": []}

        name = "job-definition-name"
        image = "jobimage:latest"
        volume = "volume-name"

        host_config = {"NetworkMode": "default", "Binds": [f"{volume}:/mnt:rw"]}

        job_definition = {
            "image": image,
            "name": name,
            "volumes": ["/mnt"],
            "environment": [
                "ENVIRONMENT_VARIABLE_A=X",
                "ENVIRONMENT_VARIABLE_B=Y",
            ],
        }

        submit_docker_job(client, job_definition, volume)

        client.create_container.assert_called_with(**job_definition, host_config=host_config)
        client.start.assert_called_with(container=container_id)


if __name__ == "__main__":
    unittest.main()
