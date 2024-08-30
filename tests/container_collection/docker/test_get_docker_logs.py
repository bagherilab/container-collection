import secrets
import unittest
from unittest import mock

from docker import APIClient

from container_collection.docker.get_docker_logs import get_docker_logs


class TestGetDockerLogs(unittest.TestCase):
    def test_get_docker_logs_no_filter(self):
        log_filter = ""
        messages = [f"Event {i}" for i in range(10)]
        expected_logs = "\n".join(messages)

        container_id = secrets.token_hex(32)
        client = mock.MagicMock(spec=APIClient)
        client.logs.return_value = "\n".join(messages).encode("utf-8")

        logs = get_docker_logs(client, container_id, log_filter)
        self.assertEqual(expected_logs, logs)

    def test_get_docker_logs_with_include_filter(self):
        log_filter = "INCLUDE"
        include_messages = [f"Event INCLUDE {i}" for i in range(10)]
        exclude_messages = [f"Event EXCLUDE {i}" for i in range(10)]
        messages = [val for pair in zip(include_messages, exclude_messages) for val in pair]
        expected_logs = "\n".join(include_messages)

        container_id = secrets.token_hex(32)
        client = mock.MagicMock(spec=APIClient)
        client.logs.return_value = "\n".join(messages).encode("utf-8")

        logs = get_docker_logs(client, container_id, log_filter)
        self.assertEqual(expected_logs, logs)

    def test_get_docker_logs_with_exclude_filter(self):
        log_filter = "-EXCLUDE"
        include_messages = [f"Event INCLUDE {i}" for i in range(10)]
        exclude_messages = [f"Event EXCLUDE {i}" for i in range(10)]
        messages = [val for pair in zip(include_messages, exclude_messages) for val in pair]
        expected_logs = "\n".join(include_messages)

        container_id = secrets.token_hex(32)
        client = mock.MagicMock(spec=APIClient)
        client.logs.return_value = "\n".join(messages).encode("utf-8")

        logs = get_docker_logs(client, container_id, log_filter)
        self.assertEqual(expected_logs, logs)


if __name__ == "__main__":
    unittest.main()
