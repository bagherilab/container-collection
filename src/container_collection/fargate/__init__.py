from prefect import task

from .check_fargate_task import check_fargate_task
from .make_fargate_task import make_fargate_task
from .register_fargate_task import register_fargate_task
from .submit_fargate_task import submit_fargate_task
from .terminate_fargate_task import terminate_fargate_task

check_fargate_task = task(check_fargate_task)
make_fargate_task = task(make_fargate_task)
register_fargate_task = task(register_fargate_task)
submit_fargate_task = task(submit_fargate_task)
terminate_fargate_task = task(terminate_fargate_task)
