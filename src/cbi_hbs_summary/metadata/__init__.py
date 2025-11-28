from typing import Literal
from pathlib import Path

import yaml


_METADATA = Literal["table_names", "column_names"]

def load(name: _METADATA) -> dict:
    file_path = Path(__file__).parent.joinpath(f"{name}.yaml")
    with file_path.open(encoding="utf-8") as file:
        metadata = yaml.safe_load(file.read())
    if name == "column_names":
        if "_general" in metadata:
            metadata = _add_general_columns(metadata)
    return metadata


def _add_general_columns(metadata: dict) -> dict:
    general_columns = metadata.pop("_general")
    for columns in metadata.values():
        columns.update(general_columns)
    return metadata
