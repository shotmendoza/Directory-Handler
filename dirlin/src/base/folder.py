"""For the classic Dirlin Folders"""

import os.path
from datetime import date, timedelta
from pathlib import Path, PosixPath
from typing import Optional
from urllib.parse import urlparse
from urllib.error import HTTPError

import pandas as pd
import pandas.errors

import chardet

from dirlin.src.base.document import Document


class Directory:
    def __init__(self, path: str | Path | None = None, initialize_posix_path: bool = True):
        """object used for directory level data wrangling. Can think of directories as a collection
        of Folders, where the Folder object handles different functionality for processing, while the
        Directory object keeps them organized.

        For macOS, we have extra fields we can use - DOWNLOADS, DESKTOP, DOCUMENTS, DEVELOPER
        if available.

        ...

        Attributes:
            - path: the directory path the folder is set to. If left as None, the path will be set to the downloads
            folder for macOS. Windows currently not supported.
            - initialize_posix_path: assumes the script is on a macOS directory

        :param folder: initializes a Folder.folder FolderPath object
        :param initialize_posix_path: initializes default macOS folders
        """
        _curr_folder_directory = Path.cwd().home()

        self.DOWNLOADS: Folder | None = None
        self.DESKTOP: Folder | None = None
        self.DOCUMENTS: Folder | None = None
        self.DEVELOPER: Folder | None = None

        # adding the macOS only directory paths, so we don't have to define these in the future and they are included
        if initialize_posix_path is True:
            if isinstance(_curr_folder_directory, PosixPath):
                print(f"Adding macOS specific default directories...")
                self.DOWNLOADS = Folder(_curr_folder_directory / "Downloads")
                self.DESKTOP = Folder(_curr_folder_directory / "Desktop")
                self.DOCUMENTS = Folder(_curr_folder_directory / "Documents")

                try:
                    self.DEVELOPER = Folder(_curr_folder_directory / "Developer")
                except AttributeError:
                    print(f"Developer folder has not been created yet on this mac.")

        if path is None:
            path = self.DOWNLOADS.path

        self.folder: Folder = Folder(path)
        """argument given by user pointing to a specific directory"""

    def __truediv__(self, other) -> Path:
        return self.folder / other


class Folder:
    def __init__(self, path: Path | str | None = None):
        """

        Dataframe Functions:
            - open() : opens a path file and converts it into a dataframe
            - open_recent(): opens the most recent file as a dataframe based on naming conventions
            - find_and_combine(): finds all files that follow naming conventions and creates a single dataframe
            - as_map(): creates a dictionary based on two columns from dataframe
            - index_files(): creates a list of paths based on file_ext or file suffixes (.csv, .xlsx, etc.)

        ...

        Document Functions:
            - open_as_document(): opens a path file and converts it into a document object
            - open_recent_as_document(): opens the most recent Path file and converts it into a document object

        ...

            Attributes:
                - path: the directory path the folder is set to. If left as None, the path will be set to the downloads
                folder for macOS. Windows currently not supported.
                - initialize_posix_path: assumes the script is on a macOS directory

        :param path: Path to the Folder. This is the directory the functions will use to search files in
        """
        if isinstance(path, str):
            path = Path(path)
        self.path = path

        if not self.path.is_dir():
            raise ValueError(f"Expected a path to a folder / directory. Got {self.path}.")

        self.path_open_recent: Path | None = None
        """stores the most recent path used on self.open_recent or self.open_recent_as_document()"""

    def __repr__(self):
        return f"{self.path}"

    def __str__(self):
        return f"{self.path}"

    def __truediv__(self, other) -> Path:
        return Path(self.path) / other

    def _find_recent_files(
            self,
            filename_pattern: str,
            days: int = 30,
            with_asterisks: bool = True,
            recurse: bool = False) -> Path:
        """
        Base function for finding the most recent file.
        Used in open_recent() and open_recent_as_document()

        :param filename_pattern: the naming convention of the file, will look in the specified directory
        :param days: looks back past x number of days. If looking in 12/31, then days=5 would look back to 12/26 files
        :param with_asterisks: defaults to True, adds the asterisks at the end of the filename_pattern arg
        :param recurse: whether to recurse through sub-folders
        :return: Path object that meets the parameters
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
            raise FileNotFoundError(
                f"No reports found in '{self.path.parent.name}/{self.path.name}' directory in the past {days} days.")

    def open(self, file_path: str | Path, *args, **kwargs) -> pd.DataFrame:
        """
        Opens a string or pathlib.Path object into a pandas.DataFrame object.
        Currently, reads .xlsx, .xls, .csv, .json file extensions, and will raise a
        KeyError for any other file types.

        :param file_path: path to the file you want to open
        :param args: see documentation for pd.DataFrame object
        :param kwargs: see documentation for pd.DataFrame object
        :return: pd.DataFrame object of the file
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)

        if not file_path.exists():
            file_path = self.path / file_path

        # Valid File Types
        _excel_types = (".xlsx", ".xls", ",xlsb")
        _text_types = (".txt", ".csv")

        if file_path.suffix in _excel_types:
            if "sheet_name" not in kwargs.keys():
                kwargs["sheet_name"] = 0
            return pd.read_excel(file_path, *args, **kwargs)

        elif file_path.suffix in _text_types:
            try:
                return pd.read_csv(file_path, *args, **kwargs)
            except pandas.errors.ParserError:
                print("Could not parse in C, attempting to reparse in Python...")
                return pd.read_csv(file_path, engine='python', on_bad_lines='warn', *args, **kwargs)
            except UnicodeDecodeError as uni_error:
                print(f"{uni_error}")
                print(f"reattempting to parse with chardet...")
                with open(file_path, "rb") as f:
                    file_path_encoding = chardet.detect(f.read())
                    return pd.read_csv(file_path, encoding=file_path_encoding['encoding'], *args, **kwargs)
            except pd.errors.DtypeWarning as dt_warning:
                print(dt_warning)
                print("reprocessing with lower_memory arg...")
                return pd.read_csv(file_path, low_memory=False, *args, **kwargs)
        elif file_path.suffix == ".json":
            return pd.read_json(file_path, *args, **kwargs)
        try:
            url = urlparse(str(file_path))
            if url.netloc == "docs.google.com" and "format=csv" in url.query.split("&"):
                return pd.read_csv(file_path, *args, **kwargs)
        except HTTPError as e:
            raise e
        else:
            raise KeyError(f"File suffix {file_path.suffix} is an unsupported format.")

    def open_as_document(self, file_path: str | Path, *args, **kwargs) -> Document:
        """opens the file path as a Document object.

        Document objects have a series of functionality not available to a standard Dataframe.

        Document Functions:
            - open_as_document(): opens a path file and converts it into a document object
            - open_recent_as_document(): opens the most recent Path file and converts it into a document object
            - check_columns(): checks that the columns exist in the DataFrame
            - move_file(): moves the Document to a specified location
            - chunk(): splits a large dataframe into smaller chunk-sized DataFrames, useful if there's an upload limit
            - as_ordered_transaction(): converts a document file into an aggregated dataframe, with cumsum aggregation

        :param file_path: path to the file you want to open as a document
        :param args: see documentation for pd.DataFrame object
        :param kwargs: see documentation for pd.DataFrame object
        :return: a Document object
        """
        df = self.open(file_path, *args, **kwargs)
        return Document(df=df, path=file_path)

    def open_recent(
            self,  
            filename_pattern: str = "",
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
        self.path_open_recent = self._find_recent_files(filename_pattern, days, with_asterisks, recurse)
        return self.open(self.path_open_recent, *args, **kwargs)

    def open_recent_as_document(
            self,
            filename_pattern: str = "",
            days: int = 30,
            with_asterisks: bool = True,
            recurse: bool = False, *args, **kwargs) -> Document:
        """
        Please refer to documentation on open_recent() and open_as_document().
        The difference is that this function returns a Document object.

        :param filename_pattern: the naming convention for the file you are searching
        :param days: number of days back you would like to search
        :param with_asterisks: whether to append an asterisks at the end of the filename_pattern
        :param recurse: whether to recurse through sub-folders
        :param args: arguments for dataframe function
        :param kwargs: keyword arguments for dataframe function
        :return: returns a Document object
        """
        self.path_open_recent = self._find_recent_files(filename_pattern, days, with_asterisks, recurse)
        return self.open_as_document(self.path_open_recent, *args, **kwargs)

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
            filename_pattern: str = "",
            with_asterisks: bool = True,
            recurse: bool = False,
            only_first_x: int | None = None, *args, **kwargs) -> pd.DataFrame:
        """
        Uses a filename pattern to find all files that follow the naming convention
        and converts the files into a single DataFrame object6

        :param filename_pattern: the naming convention of the files you are searching for
        :param with_asterisks: defaults to True. Determines whether an asterisks are added at the end of the
        filename pattern

        :param recurse: defaults to False. Determines whether to search for sub-folders
        :param only_first_x: defaults to None. Determines whether to only look at the first x files to combine.
        don't use this parameter if you are unsure of the number of files in the folder
        :param args: args used in pd.DataFrame objects
        :param kwargs: keyword args used in pd.DataFrame objects
        :return: a DataFrame object of all the files that share similar naming conventions in a folder
        """
        # formatting the filename pattern convention and handling asterisks
        asterisks_mapping = {
            True: f"{filename_pattern}*",
            False: filename_pattern
        }
        if not with_asterisks and filename_pattern == "":
            raise ValueError(f"filename_pattern cannot be left as default if with_asterisks parameter is set to False")

        # handling finding the files with recursion or not
        df = None
        if recurse:
            files = [
                f for f in self.path.rglob(
                    pattern=f"{asterisks_mapping[with_asterisks]}"
                ) if not f.name.startswith("~") and not f.name.startswith(".")
            ]
        else:
            files = [
                f for f in self.path.glob(
                    pattern=f"{asterisks_mapping[with_asterisks]}"
                ) if not f.name.startswith("~") and not f.name.startswith(".")
            ]

        # handling the only_first_x param
        files = sorted(files, key=os.path.getmtime, reverse=True)
        if only_first_x is not None:
            try:
                files = files[: only_first_x]
            except IndexError:
                print(f"only_first_x arg  was too large ({only_first_x}) compared to found files ({len(files)})")
                print(f"keeping the length of the files")
            else:
                if only_first_x < 0:
                    raise ValueError(f"The value of only_first_x must be a positive integer. Got ({only_first_x}).")

        # handling the combining of files
        for file in files:
            if file.is_dir():
                """
                This is used for when the recurse is True so that it doesn't raise an error
                """
                continue
            temp = self.open(file, *args, **kwargs)
            temp["From"] = file.stem
            if df is None:
                df = temp
                continue
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
                f for f in self.path.glob(pattern=f"{filename_convention}{file_ext}")
                if not f.name.startswith("~") and not f.name.startswith(".")
            ]
        return files
