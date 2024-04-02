"""Inspect resources on a local filesystem."""

from pathlib import Path
from typing import List

from kukur.inspect import InspectedPath, ResourceType


def inspect_filesystem(path: Path) -> List[InspectedPath]:
    """Inspect a filesystem path.

    Lists all files in the path.
    Tries to determine which files are supported by Kukur and returns them.
    """
    paths = []
    for sub_path in path.glob("*"):
        if sub_path.is_dir():
            if (sub_path / "_delta_log").is_dir():
                paths.append(InspectedPath(ResourceType.DELTA, str(sub_path)))
            else:
                paths.append(InspectedPath(ResourceType.DIRECTORY, str(sub_path)))
        elif sub_path.suffix.lower() == ".parquet":
            paths.append(InspectedPath(ResourceType.PARQUET, str(sub_path)))
        elif sub_path.suffix.lower() in [".arrow", ".feather"]:
            paths.append(InspectedPath(ResourceType.ARROW, str(sub_path)))
        elif sub_path.suffix.lower() in [".arrows"]:
            paths.append(InspectedPath(ResourceType.ARROWS, str(sub_path)))
        elif sub_path.suffix.lower() in [".csv", ".txt"]:
            paths.append(InspectedPath(ResourceType.CSV, str(sub_path)))
    return paths
