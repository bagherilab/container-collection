from jinja2 import StrictUndefined, Template


def generate_input_contents(template: str, conditions: list[dict]) -> list[str]:
    compiled_template = Template(template, undefined=StrictUndefined)
    rendered_templates = [compiled_template.render(condition) for condition in conditions]
    return rendered_templates
