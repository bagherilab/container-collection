from prefect import task


@task
def make_fargate_task(
    name: str,
    image: str,
    account: str,
    region: str,
    user: str,
    vcpus: int,
    memory: int,
) -> dict:
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
