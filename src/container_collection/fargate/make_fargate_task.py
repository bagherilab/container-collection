def make_fargate_task(
    name: str,
    image: str,
    region: str,
    user: str,
    vcpus: int,
    memory: int,
    task_role_arn: str,
    execution_role_arn: str,
) -> dict:
    """
    Create Fargate task definition.

    Docker images on the Docker Hub registry are available by default, and can
    be specified using ``image:tag``. Otherwise, use ``repository/image:tag``.

    Parameters
    ----------
    name
        Task definition name.
    image
        Docker image.
    region
        Logging region.
    user
        User name prefix for task name.
    vcpus
        Hard limit of CPU units to present to the task.
    memory
        Hard limit of memory to present to the task.
    task_role_arn
        ARN for IAM role for the task container.
    execution_role_arn : str
        ARN for IAM role for the container agent.

    Returns
    -------
    :
        Task definition.
    """

    return {
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
