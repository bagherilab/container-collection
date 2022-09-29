"""REGULAR EXPRESSIONS"""


import re

TAG_MATCH = r"([A-z0-9]*)"

OPTIONS_MATCH = r"\(([A-z0-9,\.\|\*\[\]]+)\)"

SEED_MATCH = r"[0-9]{4}"

SERIES_NAME_ATTRIBUTE = r'name="([A-z0-9,\_\{\}\|]*)"'

START_SEED_ATTRIBUTE = r'start="([0-9]*)"'

END_SEED_ATTRIBUTE = r'end="([0-9]*)"'

TEMPLATE_PADDING_PATTERN = r"\|([0-9]+),([0-9]+)"

TEMPLATE_NAME_PATTERN = r"{([A-z0-9\_]+)" + TEMPLATE_PADDING_PATTERN + "}"

START_SEED_REGEX = re.compile(START_SEED_ATTRIBUTE)

END_SEED_REGEX = re.compile(END_SEED_ATTRIBUTE)

SIMULATION_PERCENT_REGEX = re.compile(r"\[ ([\w\_\|\s]+) \] tick \s+\d+ \( ([\d\.]+) \% \)")

FINISHED_SIMULATION_REGEX = re.compile(r"\[ ([\w\_\|\s]+) \] finished")

STARTED_SIMULATION_REGEX = re.compile(r"\[ ([\w\_\|\s]+) \] started")

LOG_EVENT_FILTER = "simulation"
