from jinja2 import StrictUndefined, Template


def generate_input_contents(template: str, conditions: list[dict]) -> list[str]:
    """
    Generate input contents from template and conditions.

    Parameters
    ----------
    template
        Template string.
    conditions
        List of conditions.

    Returns
    -------
    :
        List of rendered templates.
    """

    compiled_template = Template(template, undefined=StrictUndefined)
    rendered_templates = [compiled_template.render(condition) for condition in conditions]
    return rendered_templates
