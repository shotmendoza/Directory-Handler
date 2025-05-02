import pandas as pd

from dirlin import Directory, BaseValidation


def test_init_on_mac():
    """added new functionality for creating MacOS folders

    """
    d = Directory()
    print(d.folder)


def function_1(foo: int, bar: int, foobar: int) -> int:
    return foo + bar + foobar


def example_df() -> pd.DataFrame:
    temp = {"bee": [1, 2, 3], "bar": [4, 5, 6], "foobar": [7, 8, 9]}
    return pd.DataFrame.from_dict(temp)


def example_two_df() -> pd.DataFrame:
    temp = {"bee": [1, 2, 3], "brr": [4, 5, 6], "far": [7, 8, 9]}
    return pd.DataFrame.from_dict(temp)


class TestBaseValidation(BaseValidation):
    first_function = function_1
    alias_mapping = {"foo": ["bee"]}


class TestSecondLayer(TestBaseValidation):
    first_function = function_1
    alias_mapping = {"bar": ["brr"], "foobar": ["far"]}


def test_class_works_as_expected():
    t_bv = TestBaseValidation()
    t_df = example_df()
    t_bv.run_validation(t_df)


def test_second_layer_works():
    t_bv = TestSecondLayer()
    t_df = example_two_df()
    t_bv.run_validation(t_df)

    print(t_bv.alias_mapping)
