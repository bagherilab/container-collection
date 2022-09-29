import re
from datetime import timedelta
import prefect
import pendulum
from prefect import Task
from prefect.engine.signals import SUCCESS, FAIL, RETRY
from arcadeio.expressions import (
    SIMULATION_PERCENT_REGEX,
    FINISHED_SIMULATION_REGEX,
    STARTED_SIMULATION_REGEX,
)


class CheckJobsTask(Task):
    """General methods for tasks that check jobs."""

    def _raise_check_signals(self, exitcodes, result):
        run_count = prefect.context.get("task_run_count", 1)

        if run_count <= self.max_retries:
            prefect.context.update(task_run_count=run_count)
            start_time = pendulum.now("utc") + self.retry_delay

            if None in exitcodes:
                raise RETRY(result=result, start_time=start_time)

        if all(code == 0 for code in exitcodes):
            raise SUCCESS(result=result)

        if any(isinstance(code, str) for code in exitcodes):
            prefect.context.update(task_run_count=run_count - 1)
            start_time = pendulum.now("utc") + timedelta(seconds=60)
            raise RETRY(result=result, start_time=start_time)

        self.max_retries = 0
        raise FAIL(result=result)

    def _update_container_statuses(self, containers, exitcodes):
        for container, exitcode in zip(containers, exitcodes):
            self._update_container_status(container, exitcode)

    def _update_container_status(self, container, exitcode):
        container.exitcode = exitcode

        if exitcode is None:
            log = self._get_log_contents(container)
            container.status = "running"
            container.icon = ":small_orange_diamond:"
            container.note = CheckJobsTask._parse_log_note(log)
        elif isinstance(exitcode, int):
            container.status = "finished"
            container.icon = ":small_blue_diamond:" if exitcode == 0 else ":small_red_triangle:"
            container.note = f"exit code {exitcode}"
        else:
            container.status = "queued"
            container.icon = ":white_small_square:"
            container.note = exitcode

    @staticmethod
    def _parse_log_note(log):
        finished = re.findall(FINISHED_SIMULATION_REGEX, log)
        started = re.findall(STARTED_SIMULATION_REGEX, log)
        percentages = re.findall(SIMULATION_PERCENT_REGEX, log)

        # Remove completed simulations from percentages list.
        percentages_filtered = [(name, perc) for name, perc in percentages if name not in finished]

        # Get latest percentage for unfinished simulation.
        percent = sorted(percentages_filtered)[-1][1] if percentages_filtered else 0

        # Get name of current simulation.
        current = list(set(started) - set(finished))

        if len(current) > 0:
            return f"{current[0]} - {percent} %"

        return " ... "
