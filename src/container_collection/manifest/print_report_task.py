from prefect import Task
from arcadeio.containers import TemplateContainer, SeriesContainer
from arcadeio.containers.summarize import SummaryContainer


class PrintReportTask(Task):
    """Task for printing report on simulation series."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, template: TemplateContainer, series: SeriesContainer, summary: SummaryContainer):
        print()
        print(template)
        print(series)
        print()
        print(summary)
        print()
