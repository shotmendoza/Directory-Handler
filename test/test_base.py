import pandas as pd

from dirlin import Directory, BaseValidation


def test_init_on_mac():
    """added new functionality for creating MacOS folders

    """
    d = Directory()
    print(d.folder)


###############################
# DATAFRAME for Testing
###############################
def example_df() -> pd.DataFrame:
    temp = {"bee": [1, 2, 3], "bar": [4, 5, 6], "foobar": [7, 8, 9]}
    return pd.DataFrame.from_dict(temp)


def example_two_df() -> pd.DataFrame:
    temp = {"bee": [1, 2, 3], "brr": [4, 5, 6], "far": [7, 8, 9]}
    return pd.DataFrame.from_dict(temp)


def example_three_df() -> pd.DataFrame:
    temp = {"glee": [1, 2, 3], "brr": [4, 5, 6], "far": [7, 8, 9]}
    return pd.DataFrame.from_dict(temp)


###############################
# CLASSES used for Testing
###############################

# === validation functions ===
def function_1(foo: int, bar: int, foobar: int) -> int:
    return foo + bar + foobar


# === the classes ===
class TestBaseValidation(BaseValidation):
    first_function = function_1
    alias_mapping = {"foo": ["bee"]}


class TestSecondLayer(TestBaseValidation):
    """a Layer ontop of the BaseValidation class

    alias_mapping does not overlap with the core class
    """
    first_function = function_1
    alias_mapping = {"bar": ["brr"], "foobar": ["far"]}


# 2025.05.05 Testing for Alias Mapping Overlap
class TestSecondLayerWithOverlap(TestBaseValidation):
    first_function = function_1
    alias_mapping = {"foo": ["glee"], "bar": ["brr"], "foobar": ["far"]}


###############################
# TESTS
###############################
def test_class_works_as_expected():
    """Validation class, Dataframe, both run properly on the most
    basic use cases
    """
    t_bv = TestBaseValidation()
    t_df = example_df()
    t_bv._run_validation(t_df)


def test_second_layer_works():
    """a class that inherits from BaseValidation can inherit from said class, and
    runs properly on the most basic use cases
    """
    t_bv = TestSecondLayer()
    t_df = example_two_df()
    t_bv._run_validation(t_df)

    print(t_bv.alias_mapping)


def test_run_summary_on_second_layer():
    """a second-level core validation class can run a Summary and output correctly
    """
    t_bv = TestSecondLayer()
    t_df = example_two_df()
    t_bv.run_summary(t_df)
    print(t_bv.run_summary(t_df))


def test_run_summary_on_second_layer_with_overlap():
    """testing to see behavior of an alias_mapping that has overlapped values"""
    base_validation = TestSecondLayer()
    overlap_validation = TestSecondLayerWithOverlap()

    print(base_validation._get_all_alias_mapping_in_class())
    print(overlap_validation._get_all_alias_mapping_in_class())
