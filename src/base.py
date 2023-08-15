import os.path
import re
from abc import ABC
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd


class Document(ABC):
    def __init__(self, path: Path | str, pattern: Optional[str] = None):
        if isinstance(path, str):
            path = Path(path)

        if path.is_file():
            pass

        if path.is_dir():
            pass


class Folder:
    def __init__(self, folder_path: Path | str):
        if isinstance(folder_path, str):
            folder_path = Path(folder_path)
        self.path = folder_path

        if not self.path.is_dir():
            raise ValueError(f"Expected a path to a folder / directory. Got {self.path}.")
        
    @classmethod
    def open(cls, file_path: str | Path, *args, **kwargs) -> pd.DataFrame:
        if "sheet_name" not in kwargs.keys():
            kwargs["sheet_name"] = 0
        if file_path.suffix == ".xlsx":
            return pd.read_excel(file_path, *args, **kwargs)
        elif file_path.suffix == ".csv":
            return pd.read_csv(file_path, *args, **kwargs)
        elif file_path.suffix == ".json":
            return pd.read_json(file_path, *args, **kwargs)
        else:
            raise KeyError(f"File suffix {file_path.suffix} is an unsupported format.")

    def open_recent(self, filename_pattern: str, with_asterisks: bool = True, recurse: bool = False, *args, **kwargs):
        asterisks_mapping = {
            True: f"{filename_pattern}*",
            False: filename_pattern
        }

        if recurse:
            files = [
                f for f in self.path.rglob(pattern=f"{asterisks_mapping[with_asterisks]}")
                if date.fromtimestamp(f.stat().st_mtime) >= (date.today() - timedelta(days=10000))
                   and not f.name.startswith("~")
            ]
        else:
            files = [
                f for f in self.path.glob(pattern=f"{asterisks_mapping[with_asterisks]}")
                if date.fromtimestamp(f.stat().st_mtime) >= (date.today() - timedelta(days=10000))
                and not f.name.startswith("~")
            ]

        try:
            most_recent_file = Path(sorted(files, key=os.path.getmtime, reverse=True)[0])
            return self.open(most_recent_file, *args, **kwargs)
        except IndexError:
            raise IndexError(
                f"There are no reports saved in the '{self.path.parent.name}/{self.path.name}' in the past 7 days.")

