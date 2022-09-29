class ManifestContainer:
    """Container for loaded manifest file."""

    def __init__(self, name: str, file: str, manifest):
        self.name = name
        self.file = file
        self.manifest = manifest

    def __str__(self):
        return self.manifest.to_markdown()
