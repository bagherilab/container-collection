from prefect import task
from prefect.blocks.system import Secret


@task
def make_fargate_task(name: str, image: str, user: str, vcpus: int, memory: int) -> dict:
    account = Secret.load("aws-account").get()
    region = Secret.load("aws-region").get()

    return {
        "containerDefinitions": [
            {
                "name": f"{user}_{name}",
                "image": f"{account}.dkr.ecr.{region}.amazonaws.com/{user}/{image}",
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
        "taskRoleArn": f"arn:aws:iam::{account}:role/BatchJobRole",
        "executionRoleArn": f"arn:aws:iam::{account}:role/ecsTaskExecutionRole",
        "cpu": str(vcpus),
        "memory": str(memory),
    }
