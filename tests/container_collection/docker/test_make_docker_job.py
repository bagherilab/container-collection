import unittest

from container_collection.docker.make_docker_job import make_docker_job


class TestMakeDockerJob(unittest.TestCase):
    def test_make_docker_job_no_environment(self):
        name = "job_name"
        image = "image_name"

        expected_job = {
            "image": image,
            "name": name,
            "volumes": ["/mnt"],
        }

        job = make_docker_job(name, image)

        self.assertDictEqual(expected_job, job)

    def test_make_docker_job_with_environment(self):
        name = "job_name"
        image = "image_name"
        environment = [
            "ENVIRONMENT_VARIABLE_A=X",
            "ENVIRONMENT_VARIABLE_B=Y",
        ]

        expected_job = {
            "image": image,
            "name": name,
            "volumes": ["/mnt"],
            "environment": environment,
        }

        job = make_docker_job(name, image, environment=environment)

        self.assertDictEqual(expected_job, job)


if __name__ == "__main__":
    unittest.main()
