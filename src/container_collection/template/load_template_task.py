from prefect import Task
from arcadeio.containers import TemplateContainer


class LoadTemplateTask(Task):
    """Task for loading template file into container."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, template_file: str) -> TemplateContainer:
        # Load template file.
        with open(template_file, "r") as file:
            template_contents = file.read()

        # Parse file name to get template name.
        template_name = LoadTemplateTask._parse_template_name(template_file)

        return TemplateContainer(template_name, template_contents)

    @staticmethod
    def _parse_template_name(template_file):
        split_template_file = template_file.split("/")
        template_name = split_template_file[-1].replace(".xml", "")
        return template_name
