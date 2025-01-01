from typing import Any

import numpy as np
import pandas as pd


class TestSharedParamSingleField:
    """this class aims to test use cases where a test function has a shared parameter,
    but the field in the DataFrame only has one field that is under the shared parameter.

    For example, if a function has parameters(id, based_shared_field, static_field), then
    we would expect that a dataframe that only had one column under that shared parameter (because
    currently, the logic dictates that if no matching name is found, look at the base
    of the word `shared_field`) would be a static field.

    An example that got caught: `check_function_net_income(stock_gross_income, stock_expenses)`.
    This was passed to a DataFrame that only had the following fields: `tsm_gross_income`, `tsm_expenses`.
    Don't remember the error code but would want to catch these types of errors.
    """

    @staticmethod
    def create_example_df() -> pd.DataFrame:
        data = {
            "id": [0, 1, 2, 3, 4, 5],
            "tsm_gross_income": [150, 160, 170, 180, 190, 200],
            "tsm_expenses": [25, 20, 40, 30, 35, 45],
        }
        return pd.DataFrame.from_dict(data)

    @staticmethod
    def check_function(stock_gross_income: Any, stock_expenses: Any) -> bool:
        expected = 140
        net_income = stock_gross_income - stock_expenses
        if expected != net_income:
            return False
        return True
