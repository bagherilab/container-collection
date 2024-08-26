import unittest

from container_collection.batch.make_batch_job import make_batch_job


class TestMakeBatchJob(unittest.TestCase):
    def test_make_batch_job_no_optional_properties(self):
        name = "job_name"
        image = "image_name"
        vcpus = 10
        memory = 20

        expected_job = {
            "jobDefinitionName": name,
            "type": "container",
            "containerProperties": {
                "image": image,
                "vcpus": vcpus,
                "memory": memory,
            },
        }

        job = make_batch_job(name, image, vcpus, memory)

        self.assertDictEqual(expected_job, job)

    def test_make_batch_job_with_optional_properties(self):
        name = "job_name"
        image = "image_name"
        vcpus = 10
        memory = 20
        environment = [
            {"name": "ENVIRONMENT_VARIABLE_A", "value": "X"},
            {"name": "ENVIRONMENT_VARIABLE_B", "value": "Y"},
        ]
        job_role_arn = "job_role_arn"

        expected_job = {
            "jobDefinitionName": name,
            "type": "container",
            "containerProperties": {
                "image": image,
                "vcpus": vcpus,
                "memory": memory,
                "environment": environment,
                "jobRoleArn": job_role_arn,
            },
        }

        job = make_batch_job(
            name, image, vcpus, memory, environment=environment, job_role_arn=job_role_arn
        )

        self.assertDictEqual(expected_job, job)


if __name__ == "__main__":
    unittest.main()
