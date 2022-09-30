class TemplateContainer:
    """Container for parsed template file."""

    def __init__(self, name: str, template: str):
        self.name = name
        self.template = template

    def __str__(self):
        return f" {'TEMPLATE':<10} : {self.name}"


from dataclasses import dataclass


@dataclass
class TemplateContainer:
    """Container for loaded template file."""

    name: str

    contents: str
