import unittest

from container_collection.fargate.make_fargate_task import make_fargate_task


class TestMakeFargateTask(unittest.TestCase):
    def test_make_fargate_task(self):
        name = "job_name"
        image = "image_name"
        region = "region_name"
        user = "user_name"
        vcpus = 10
        memory = 20
        task_role_arn = "task_role_arn"
        execution_role_arn = "execution_role_arn"

        expected_task = {
            "containerDefinitions": [
                {
                    "name": f"{user}_{name}",
                    "image": image,
                    "essential": True,
                    "portMappings": [],
                    "environment": [],
                    "mountPoints": [],
                    "volumesFrom": [],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": f"/ecs/{user}_{name}",
                            "awslogs-region": region,
                            "awslogs-stream-prefix": "ecs",
                            "awslogs-create-group": "true",
                        },
                    },
                }
            ],
            "family": f"{user}_{name}",
            "networkMode": "awsvpc",
            "requiresCompatibilities": ["FARGATE"],
            "taskRoleArn": task_role_arn,
            "executionRoleArn": execution_role_arn,
            "cpu": str(vcpus),
            "memory": str(memory),
        }

        task = make_fargate_task(
            name, image, region, user, vcpus, memory, task_role_arn, execution_role_arn
        )

        self.assertDictEqual(expected_task, task)


if __name__ == "__main__":
    unittest.main()
