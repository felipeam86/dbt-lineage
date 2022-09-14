import json
from pathlib import Path

__version__ = "0.1.0"


def read_manifest(manifest_filepath):
    return json.loads(Path(manifest_filepath).read_text())
