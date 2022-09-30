from prefect import task

from container_collection.template import TemplateContainer


@task
def load_template_file(template_file: str) -> TemplateContainer:
    """Task for loading template file into container."""

    with open(template_file, "r") as file:
        template_contents = file.read()

    template_name = parse_template_name(template_file)

    return TemplateContainer(name=template_name, contents=template_contents)


def parse_template_name(template_file: str) -> str:
    split_template_file = template_file.split("/")
    template_name = split_template_file[-1].split(".")[0]
    return template_name
