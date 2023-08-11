import re
from abc import ABC
from pathlib import Path
from typing import Optional


class Document(ABC):
    def __init__(self, path: Path | str, pattern: Optional[str] = None):
        if isinstance(path, str):
            path = Path(path)

        if path.is_file():
            pass

        if path.is_dir():
            pass


class Folder:
    def __init__(self, path: Path | str):
        if isinstance(path, str):
            path = Path(path)
        self.path = path

        if not self.path.is_dir():
            raise ValueError(f"Expected a path to a folder / directory. Got {self.path}.")

    def __call__(self, filename: str) -> list[Path]:
        if "*" not in filename:
            filename = f"*{filename}*"

        files = [f for f in self.path.rglob(pattern=filename) if f.is_file()]
        return files

