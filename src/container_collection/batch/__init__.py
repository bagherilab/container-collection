from prefect import task

from .check_batch_job import check_batch_job
from .get_batch_logs import get_batch_logs
from .make_batch_job import make_batch_job
from .register_batch_job import register_batch_job
from .submit_batch_job import submit_batch_job
from .terminate_batch_job import terminate_batch_job

check_batch_job = task(check_batch_job)
get_batch_logs = task(get_batch_logs)
make_batch_job = task(make_batch_job)
register_batch_job = task(register_batch_job)
submit_batch_job = task(submit_batch_job)
terminate_batch_job = task(terminate_batch_job)
