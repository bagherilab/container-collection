class DockerContainer:
    """Container for job running on AWS Batch."""

    def __init__(self, name: str, container: str):
        self.name = name
        self.container = container

    def __str__(self):
        strs = [
            f" {'NAME':<10} : {self.name}",
            f" {'CONTAINER':<10} : {self.container}",
        ]

        return "\n".join(strs) + "\n"
