import os.path
import re
from abc import ABC
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd


class Document(ABC):
    def __init__(self, path: Path | str, pattern: Optional[str] = None):
        ...


class Folder:
    def __init__(self, folder_path: Path | str):
        if isinstance(folder_path, str):
            folder_path = Path(folder_path)
        self.path = folder_path

        if not self.path.is_dir():
            raise ValueError(f"Expected a path to a folder / directory. Got {self.path}.")

    @classmethod
    def open(cls, file_path: str | Path, *args, **kwargs) -> pd.DataFrame:
        """
        Opens a string or pathlib.Path object into a pandas.DataFrame object.
        Currently, reads .xlsx, .csv, .json file extensions, and will raise a
        KeyError for any other file types.

        :param file_path: path to the file you want to open
        :param args: see documentation for pd.DataFrame object
        :param kwargs: see documentation for pd.DataFrame object
        :return: pd.DataFrame object of the file
        """
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

    @classmethod
    def as_map(
            cls,
            file_path: str | Path,
            key_column: str,
            value_column: str, *args, **kwargs) -> dict[str, str]:
        """
        Function for creating a dictionary based on two DataFrame columns

        :param file_path: str or Pathlib.Path to the DataFrame file
        :param key_column: the DataFrame column to be used as the keys for the dict
        :param value_column: the DataFrame column to be used as the values in the dict
        :param args: arguments in the pd.DataFrame.from_csv() or pd.DataFrame.from_excel() functions
        :param kwargs: keyword arguments in the pd.DataFrame.from_csv() or pd.DataFrame.from_excel() functions
        :return: Dictionary of the two columns
        """
        df = cls.open(file_path=file_path, *args, **kwargs)

        if not all([column in df.columns for column in (key_column, value_column)]):
            raise f"Expected key {key_column} and value {value_column}. Missing one or all from the Dataframe."

        mapping = {
            k: v for k, v in zip(df[key_column], df[value_column])
        }
        return mapping

    def open_recent(
            self,
            filename_pattern: str,
            with_asterisks: bool = True,
            recurse: bool = False, *args, **kwargs) -> pd.DataFrame:
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

    def find_and_combine(
            self,
            filename_pattern: str,
            with_asterisks: bool = True,
            recurse: bool = False, *args, **kwargs):

        asterisks_mapping = {
            True: f"{filename_pattern}*",
            False: filename_pattern
        }

        df = None
        if recurse:
            files = [
                f for f in self.path.glob(
                    pattern=f"{asterisks_mapping[with_asterisks]}"
                ) if not f.name.startswith("~") and not f.name.startswith(".")
            ]
        else:
            files = [
                f for f in self.path.rglob(
                    pattern=f"{asterisks_mapping[with_asterisks]}"
                ) if not f.name.startswith("~") and not f.name.startswith(".")
            ]

        files = sorted(files, key=os.path.getmtime, reverse=True)
        for file in files:
            if df is None:
                continue
            temp = self.open(file, *args, **kwargs)
            temp["From"] = file.stem
            df = pd.concat((df, temp))
        return df
