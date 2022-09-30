from dataclasses import dataclass

import pandas as pd


@dataclass
class ManifestContainer:
    """Container for loaded manifest file."""

    file: str

    contents: pd.DataFrame
