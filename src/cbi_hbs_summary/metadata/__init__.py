from typing import Literal
from pathlib import Path

import yaml


_METADATA = Literal["table_names", "column_names"]

def load(name: _METADATA) -> dict:
    file_path = Path(__file__).parent.joinpath(f"{name}.yaml")
    with file_path.open(encoding="utf-8") as file:
        metadata = yaml.safe_load(file.read())
    return metadata


def get_rename_dict(table_name: str, year: int | None) -> dict[str, str]:
    metadata = load("column_names")
    table_column_names = metadata[table_name]
    if all(isinstance(k, int) for k in table_column_names.keys()):
        assert year is not None
        version = max(filter(lambda y: y <= year, table_column_names.keys()))
        table_column_names = table_column_names[version]
    if "_general" in metadata:
        table_column_names.update(metadata["_general"])
    rename_dict = {_sanitize_farsi_text(k): v for k, v in table_column_names.items()}
    return rename_dict


def _sanitize_farsi_text(text: str) -> str:
    return (
        text
        .replace(" ", "")
        .replace("،", "")
    )
