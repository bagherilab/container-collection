from prefect import task

from .check_docker_job import check_docker_job
from .clean_docker_job import clean_docker_job
from .create_docker_volume import create_docker_volume
from .get_docker_logs import get_docker_logs
from .make_docker_job import make_docker_job
from .remove_docker_volume import remove_docker_volume
from .run_docker_command import run_docker_command
from .submit_docker_job import submit_docker_job
from .terminate_docker_job import terminate_docker_job

check_docker_job = task(check_docker_job)
clean_docker_job = task(clean_docker_job)
create_docker_volume = task(create_docker_volume)
get_docker_logs = task(get_docker_logs)
make_docker_job = task(make_docker_job)
remove_docker_volume = task(remove_docker_volume)
run_docker_command = task(run_docker_command)
submit_docker_job = task(submit_docker_job)
terminate_docker_job = task(terminate_docker_job)
