import os.path
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd


class Document:
    def __init__(self, df: pd.DataFrame, path: Path):
        self.dataframe: pd.DataFrame = df
        self.filepath: Path = path

    def check_columns(self, headers: list[str], match_all: bool = True, raise_error: bool = True) -> bool:
        """
        Checks the dataframe to confirm that expected columns are in the data.

        :param headers: the headers to check for
        :param match_all: defaults to confirm every column is in data. When False, confirms if any columns is in data
        :param raise_error: defaults to raising error for missing column. When False, only returns bool
        :return: True if all columns are present. False if any or all columns are missing (param dependent)
        """
        if match_all is True:
            if any([h in self.dataframe.columns for h in headers]) is False:
                if raise_error:
                    raise KeyError(f"Missing on or all expected columns {headers} in {self.filepath.stem} file.")
                return False
            return True

        if any([h in self.dataframe.columns for h in headers]) is True:
            return True
        else:
            if raise_error:
                raise KeyError(f"Missing all expected columns {headers} in {self.filepath.stem} file.")
            return False

    def move_file(self, destination: Path):
        """
        Moves the current file you are working with to another folder or destination.

        :param destination: the full path (including filename) of the Path object
        :return: the new path (also changes state of object in self.filepath)
        """
        try:
            self.filepath = self.filepath.rename(target=destination)
        except Exception as e:
            raise e
        return self.filepath


class Folder:
    def __init__(self, folder_path: Path | str):
        """Object used for processing files through local directories.
        Good for partially built out automated processes that can be done on a single computer.

        Functions:
            - open() : opens a path file and converts it into a dataframe
            - as_map(): creates a dictionary based on two columns from dataframe
            - open_recent(): opens the most recent file as a dataframe based on naming conventions
            - find_and_combine(): finds all files that follow naming conventions and creates a single dataframe
            - index_files(): creates a list of paths based on file_ext or file suffixes (.csv, .xlsx)

        ...

            Attributes:
                - path: the directory path the folder is set to

        :param folder_path: Path to the Folder
        """
        if isinstance(folder_path, str):
            folder_path = Path(folder_path)
        self.path = folder_path

        if not self.path.is_dir():
            raise ValueError(f"Expected a path to a folder / directory. Got {self.path}.")

    def _find_recent_files(
            self,
            filename_pattern: str,
            days: int = 30,
            with_asterisks: bool = True,
            recurse: bool = False) -> Path:
        """
        Base function for finding the most recent file.
        Used in open_recent() and open_recent_as_document()

        :param filename_pattern:
        :param days:
        :param with_asterisks:
        :param recurse:
        :return:
        """

        asterisks_mapping = {
            True: f"{filename_pattern}*",
            False: filename_pattern
        }

        if recurse:
            files = [
                f for f in self.path.rglob(pattern=f"{asterisks_mapping[with_asterisks]}")
                if date.fromtimestamp(f.stat().st_mtime) >= (date.today() - timedelta(days=days))
                and not f.name.startswith("~")
            ]
        else:
            files = [
                f for f in self.path.glob(pattern=f"{asterisks_mapping[with_asterisks]}")
                if date.fromtimestamp(f.stat().st_mtime) >= (date.today() - timedelta(days=days))
                and not f.name.startswith("~")
            ]

        try:
            most_recent_file = Path(sorted(files, key=os.path.getmtime, reverse=True)[0])
            return most_recent_file
        except IndexError:
            raise IndexError(
                f"No reports found in '{self.path.parent.name}/{self.path.name}' directory in the past {days} days.")

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
        if file_path.suffix == ".xlsx":
            if "sheet_name" not in kwargs.keys():
                kwargs["sheet_name"] = 0
            return pd.read_excel(file_path, *args, **kwargs)
        elif file_path.suffix == ".csv":
            return pd.read_csv(file_path, *args, **kwargs)
        elif file_path.suffix == ".json":
            return pd.read_json(file_path, *args, **kwargs)
        else:
            raise KeyError(f"File suffix {file_path.suffix} is an unsupported format.")

    @classmethod
    def open_as_document(cls, file_path: str | Path, *args, **kwargs) -> Document:
        df = cls.open(file_path, *args, **kwargs)
        return Document(df=df, path=file_path)

    def open_recent(
            self,
            filename_pattern: str,
            days: int = 30,
            with_asterisks: bool = True,
            recurse: bool = False, *args, **kwargs) -> pd.DataFrame:
        """
        Finds the most recently modified file in the directory based on a filename pattern.
        Other than this, please read the documentation for the self.open() method for more details.

        :param filename_pattern: the naming convention of the file, will look in the specified directory
        :param days: the number of days the function should look back
        :param with_asterisks: defaults to True, adds the asterisks at the end of the filename_pattern arg
        :param recurse: recursively search through sub-folders for the files in the directory
        :param args: arguments under DataFrame object
        :param kwargs: named arguments for the DataFrame object
        :return: a DataFrame object
        """
        most_recent_file = self._find_recent_files(filename_pattern, days, with_asterisks, recurse)
        return self.open(most_recent_file, *args, **kwargs)

    def open_recent_as_document(
            self,
            filename_pattern: str,
            days: int = 30,
            with_asterisks: bool = True,
            recurse: bool = False, *args, **kwargs) -> Document:
        """
        Please refer to documentation on open_recent().
        The difference is that this function returns a Document object.

        :param filename_pattern: the naming convention for the file you are searching
        :param days: number of days back you would like to search
        :param with_asterisks: whether to append an asterisks at the end of the filename_pattern
        :param recurse: whether to recurse through sub-folders
        :param args: arguments for dataframe function
        :param kwargs: keyword arguments for dataframe function
        :return: returns a Document object
        """
        most_recent_file = self._find_recent_files(filename_pattern, days, with_asterisks, recurse)
        return self.open_as_document(most_recent_file, *args, **kwargs)

    def as_map(
            self,
            file_path: str | Path,
            key_column: str,
            value_column: str, *args, **kwargs) -> dict[str, str]:
        """
        Function for creating a dictionary based on two DataFrame columns

        :param file_path: str or Path to DataFrame file. If str, just give it filename. If Path, give it the full-path.
        :param key_column: the DataFrame column to be used as the keys for the dict
        :param value_column: the DataFrame column to be used as the values in the dict
        :param args: arguments in the pd.DataFrame.from_csv() or pd.DataFrame.from_excel() functions
        :param kwargs: keyword arguments in the pd.DataFrame.from_csv() or pd.DataFrame.from_excel() functions
        :return: Dictionary of the two columns
        """
        try:
            if isinstance(file_path, str):
                file_path = self.path / file_path
        except Exception as e:
            raise e
        df = self.open(file_path=file_path, *args, **kwargs)

        if not all([column in df.columns for column in (key_column, value_column)]):
            raise f"Expected key {key_column} and value {value_column}. Missing one or all from the Dataframe."
        mapping = {
            k: v for k, v in zip(df[key_column], df[value_column])
        }
        return mapping

    def find_and_combine(
            self,
            filename_pattern: str,
            with_asterisks: bool = True,
            recurse: bool = False, *args, **kwargs) -> pd.DataFrame:
        """
        Uses a filename pattern to find all files that follow the naming convention
        and converts the files into a single DataFrame object6

        :param filename_pattern: the naming convention of the files you are searching for
        :param with_asterisks: defaults to True. Determines whether an asterisks are added at the end of the
        filename pattern

        :param recurse: defaults to False. Determines whether to search for sub-folders
        :param args: args used in pd.DataFrame objects
        :param kwargs: keyword args used in pd.DataFrame objects
        :return: a DataFrame object of all the files that share similar naming conventions in a folder
        """

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

    def index_files(
            self,
            file_ext: str,
            filename_convention: Optional[str] = "*",
            recurse: bool = False,
    ) -> list[Path]:
        """Creates a list of Path objects based on the file extension

        :param file_ext: The suffix of the file (.mp4, .xlsx, .csv)
        :param filename_convention: The naming convention of the file. Defaults to every file with extension
        :param recurse: Defaults to False. If True, will recurse through subdirectories
        :return: list of Pathlib.Path objects representing a file
        """
        if recurse:
            files = [
                f for f in self.path.rglob(
                    pattern=f"{filename_convention}{file_ext}"
                ) if not f.name.startswith("~") and not f.name.startswith(".")
            ]
        else:
            files = [
                f for f in self.path.glob(
                    pattern=f"{filename_convention}{file_ext}"
                ) if not f.name.startswith("~") and not f.name.startswith(".")
            ]
        return files
