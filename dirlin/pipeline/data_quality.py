"""Still a work in progress to get this portion set up"""


import pandas as pd
from typing import Callable


class Check:
    def __init__(self, check_function: Callable[..., tuple[bool, float]]):
        self._check_function = check_function

    def __annotations__(self):
        return self.__dict__.__annotations__

    def validate(self, data: pd.DataFrame):
        ...


class Validation:
    def __init__(self, check: Check | list[Check]):
        if isinstance(check, Check):
            check = [check,]

        self._check_list = [c for c in check]  # Redundant?

    def run(self, df: pd.DataFrame):
        for check in self._check_list:
            check.validate(df)
