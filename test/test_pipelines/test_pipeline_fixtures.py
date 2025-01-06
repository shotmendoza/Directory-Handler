from typing import Any

import pandas as pd
import pytest

"""testing class aims to test use cases where a test function has a shared parameter,
but the field in the DataFrame only has one field that is under the shared parameter.

For example, if a function has parameters(id, based_shared_field, static_field), then
we would expect that a dataframe that only had one column under that shared parameter (because
currently, the logic dictates that if no matching name is found, look at the base
of the word `shared_field`) would be a static field.

An example that got caught: `check_function_net_income(stock_gross_income, stock_expenses)`.
This was passed to a DataFrame that only had the following fields: `tsm_gross_income`, `tsm_expenses`.
Don't remember the error code but would want to catch these types of errors.
"""


def single_stock_df() -> pd.DataFrame:
    """a pretty standard dataframe for our use case."""
    data = {
        "id": [0, 1, 2, 3, 4, 5],
        "tsm_gross_income": [150, 160, 170, 180, 190, 200],
        "tsm_expenses": [25, 20, 40, 30, 35, 45],
    }
    return pd.DataFrame.from_dict(data).copy()


def single_stock_df_b() -> pd.DataFrame:
    """a pretty standard dataframe for our use case."""
    data = {
        "id": [0, 1, 2, 3, 4, 5],
        "appl_gross_income": [112, 160, 563, 345, 543, 235],
        "appl_expenses": [25, 20, 40, 30, 35, 45],
    }
    return pd.DataFrame.from_dict(data).copy()


def two_stock_df() -> pd.DataFrame:
    data = {
        "id": [0, 1, 2, 3, 4, 5],
        "tsm_gross_income": [150, 160, 170, 180, 190, 200],
        "tsm_expenses": [25, 20, 40, 30, 35, 45],
        "vt_gross_income": [100, 110, 111, 113, 115, 117],
        "vt_expenses": [25, 20, 40, 30, 35, 45],
    }
    return pd.DataFrame.from_dict(data)


def similar_field_name_df() -> pd.DataFrame:
    data = {
        "id": [0, 1, 2, 3, 4, 5],
        "tsm_gross_income": [150, 160, 170, 180, 190, 200],
        "total_gross_income": [25, 20, 40, 30, 35, 45],
    }
    return pd.DataFrame.from_dict(data)


def std_check_function(stock_gross_income: Any, stock_expenses: Any) -> bool:
    """"""
    expected = 140
    net_income = stock_gross_income - stock_expenses
    if expected != net_income:
        return False
    return True


def series_check_function(stock_gross_income: pd.Series, stock_expenses: pd.Series) -> pd.Series:
    """trying to recreate some of the issues

    :return:
    """
    results = []
    for gi, e in zip(stock_gross_income, stock_expenses):
        if gi - e == 140:
            results.append(True)
            continue
        results.append(False)
    return pd.Series(results)


def same_name_check(total_gross_income: pd.Series, carrier_gross_income: pd.Series):
    results = []
    for gi, e in zip(total_gross_income, carrier_gross_income):
        if gi - e == 140:
            results.append(True)
            continue
        results.append(False)
    return pd.Series(results)
