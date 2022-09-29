"""NOTIFICATION HANDLERS"""


import prefect
from prefect.client import Secret


def send_slack_notification(task, _old_state, new_state):
    """Sends a Slack notification with jobs status."""

    if prefect.context.flags["notify"] is False:
        return new_state

    import requests

    webhook_url = Secret("SLACK_WEBHOOK_URL").get()

    if new_state.is_retrying():
        jobs = new_state.result
        retry_count = new_state.run_count
        max_retries = task.max_retries
        slack_datetime = format_slack_datetime(new_state.start_time)
        status = {
            "icon": ":large_orange_diamond:",
            "status": "incomplete",
            "note": f"next check {slack_datetime} - {retry_count}/{max_retries}",
        }
    elif new_state.is_failed():
        jobs = new_state.result
        total_jobs = len(jobs)
        unsuccessful_jobs = len([job for job in jobs if job.exitcode != 0])
        status = {
            "icon": ":large_blue_diamond:",
            "status": "complete",
            "note": f"{unsuccessful_jobs}/{total_jobs} jobs unsuccessful",
        }
    elif new_state.is_successful():
        jobs = new_state.result
        status = {
            "icon": ":large_blue_diamond:",
            "status": "complete",
            "note": "all jobs successful",
        }
    else:
        return new_state

    flow_run_name = prefect.context["flow_run_name"]
    message = {
        "author_name": "Prefect",
        "text": f"Update for {flow_run_name}",
        "blocks": [
            make_context_section(format_slack_datetime(prefect.context["date"])),
            make_flow_section(flow_run_name, **status),
            make_job_section(jobs),
        ],
    }

    if new_state.is_successful() or new_state.is_failed():
        message["blocks"].append(make_divider_section())
        message["blocks"].append(make_notes_section())

    requests.post(webhook_url, json=message)
    return new_state


def make_divider_section():
    """Creates a divider block."""
    return {"type": "divider"}


def make_context_section(text):
    """Creates context block."""
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def make_flow_section(name, icon, status, note):
    """Creates message about overall flow."""
    flow_message = f"*{name}* - {icon} *{status}* [ _{note}_ ]"
    return format_slack_section(flow_message)


def make_job_section(jobs):
    """Creates message about individual jobs."""
    try:
        job_message = "\n".join(
            [f"> {job.name} - {job.icon} *{job.status}* [ {job.note} ]" for job in jobs]
        )
    except AttributeError:
        job_message = "\n".join([f"> {job.name} - _undefined_" for job in jobs])

    return format_slack_section(job_message)


def make_notes_section():
    """Creates message with list of notes."""
    notes = []

    terminate_flag = prefect.context.flags["terminate"]
    notes.append(f"running jobs will {'*NOT* ' if terminate_flag is False else ''}be terminated")

    clean_flag = prefect.context.flags["clean"]
    notes.append(f"workspace will {'*NOT* ' if clean_flag is False else ''}be cleaned")

    bullets = [f":white_small_square: {note}" for note in notes]

    return format_slack_section("\n".join(bullets))


def format_slack_datetime(datetime):
    """Converts datetime into Slack datetime format."""
    return f"<!date^{datetime.timestamp():.0f}^{{date_short}} {{time_secs}}|{datetime.isoformat()}>"


def format_slack_section(text):
    """Formats text into Slack message section."""
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}
