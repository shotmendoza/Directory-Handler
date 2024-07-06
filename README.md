# Dirlin Project
Dirlin is a python script that has some handy tools for dealing with local files.

Some of the use cases include:
1. Getting the most recently downloaded files into a DataFrame
2. Finding files with the same naming conventions into a Dataframe
3. Creating mappings for fast column index-matching

# Installation
You can install this project via pip:

```bash
pip install dirlin
```

# Uses
This will make it easier to pull files from your local directory to work on different projects.

1. Import Path and Folder Objects
2. Define the Helper Object
3. Create the folder attributes
4. Define a function to create new folders

```python
# object.py
from dirlin import Path, Folder  # 1 - import path and folder objects
class LocalHelper:  # 2 - defining the Helper Object
    _base_path = Path("path to directory")
    
    DOWNLOADS = Folder(_base_path / "Folder1")  # 3 - creating the folder attributes
    DESKTOP = Folder(_base_path / "Folder2")  # 3 - defining the folder attributes

    @classmethod
    def new_folder(cls, folder: str | Path):  # 4 - defining a function to create new folders
        if isinstance(folder, str):
            folder = Path(folder)
        return Folder(folder)
```
## Getting the most recently downloaded file
```python
from dirlin import Folder, Path

def get_most_recent(filename):
    _base_path = Path("path to directory")
    folder = Folder(_base_path / "Folder1")
    
    df = folder.open_recent(filename)  # returns a dataframe of the most recent file with that pattern
    return df
```

## Combining Multiple Excel documents into a single file
```python
from dirlin import Folder, Path

def get_most_recent(filename):
    _base_path = Path("path to directory")
    folder = Folder(_base_path / "Folder1")
    
    combined_df = folder.find_and_combine(filename_pattern=filename)  # combines documents 
    return combined_df

