from prefect import task

from .generate_input_contents import generate_input_contents

generate_input_contents = task(generate_input_contents)
