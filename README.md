# Dirlin Project
Dirlin is a python script that has some handy tools for dealing with local files.

## Dirlin Folders

Some of the use cases include:
1. Getting the most recently downloaded files into a DataFrame
2. Finding files with the same naming conventions into a Dataframe
3. Creating mappings for fast column index-matching

## Dirlin Pipelines
Dirlin now also offers a quick way to create pipelines from local folders and files.

Some use cases include:
1. Validating different fields based on logic you set up
2. Creating error logs for any records that don't follow pre-defined rules
3. Doing exploratory analysis

# Installation
You can install this project via pip:

```bash
pip install dirlin
```

# Quickstart
This will make it easier to pull files from your local directory to work on different projects.

1. Import Path and Folder Objects
2. Define the Helper Object
3. Create the folder attributes
4. Define a function to create new folders

## Setting up a Folder for your project

```python
# object.py
from dirlin import Path, Folder  # 1 - import path and folder objects
class LocalHelper:  # 2 - define the Helper Object
    _base_path = Path("path to directory")
    
    DOWNLOADS = Folder(_base_path / "Folder1")  # 3 - create the folders
    DESKTOP = Folder(_base_path / "Folder2")

    @classmethod
    def new_folder(cls, folder: str | Path):  # 4 - create a function to create new folders
        if isinstance(folder, str):
            folder = Path(folder)
        return Folder(folder)
```
### Getting the most recently downloaded file

```python
filename = "naming convention.xlsx"

# returns a dataframe of the most recent file with that pattern
df = LocalHelper.DOWNLOADS.open_recent(filename)  
```

### Combining Multiple Excel documents into a single file
```python
from dirlin import Folder, Path

def get_most_recent(filename):
    _base_path = Path("path to directory")
    folder = Folder(_base_path / "Folder1")
    
    combined_df = folder.find_and_combine(filename_pattern=filename)  # combines documents 
    return combined_df
```

## Using Dirlin Pipelines

Using pipelines will allow you to complete data quality and data exploratory tasks a lot easier.

The Pipeline uses 4 different objects:
1. `Pipeline` - responsible for the creation and keeping context of the folder and the file
2. `Report` - defines how the report should be formatted, which includes the column names
3. `Check` - defines the checks you want to run, ensuring that each field follows  some rule you defined
4. `Validation` - defines all the checks you want to run in the pipeline

### Setting up a Report
```python
from dirlin.pipeline import Report
report = Report(
    name_convention="ohlcv",  # name of the file you are looking for
    field_mapping={'High': 'high', 'Low': 'low'},  # the name you want to update the fields to
    column_type_cash=['high', 'low']  # the fields that need to be formatted as a `cash` type
)
```

That's all it takes to set up  a Report for you to use for your data analysis.

### Setting up a Check
Let's say that you want to add a check to make sure that values in `low` is never higher
than the values in `high`. We'll walk through how to quickly set that up.

```python
from dirlin.pipeline import Check

def low_never_higher_than_high(low: float, high: float):
    if  high <= low:
        return False
    return True

new_check = Check(low_never_higher_than_high)
```

> The function can also be defined with `pd.Series` types as parameters as well.

###  Setting up Validation
...

### Bringing it Together
```python
from dirlin.pipeline import Pipeline, Report

f = "folder path"
pipeline =  Pipeline(f)

report = Report("ohlcv", {"High": "high", "Low": "low"})
df = pipeline.get_worksheet("ohlcv", report=report)

```

Needed reset from bug
