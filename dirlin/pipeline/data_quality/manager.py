import inspect
from typing import TypeVar, Any, Callable

import pandas as pd

from dirlin.pipeline.data_quality.check import Check, CheckType


# 2025.02.26 - creating new architecture for the quick pipeline
# we're doing this to make setup easier since it got a little complicated
# and time-consuming when onboarding new clients
class BaseValidation:
    """object used for Validation objects in order to help in reusing the same set of checks
    over different reports.

    The class should be inherited by a base set of classes `BaseChecks(BaseValidation)`, where
    the `BaseChecks` class has a defined set of checks.

    The object inheriting BaseValidation class then uses the check function, `example_function = defined function`
    """

    alias_mapping: dict[str, list | str] | None = None
    """used to define columns that don't exact-match a parameter in the object,
    but we want to use as an argument in the parameter.
    
    Is a key-value pair of {`parameter name`: [`associated columns`]}, and will tie into
    the function. The error code for `_verify_column_ties_to_parameter` will also notify
    you to add missing parameters into this variable as a dict.
    """

    @classmethod
    def run_validation(cls, df: pd.DataFrame) -> dict[str, dict]:
        # STEP 1: VALIDATE
        # We want to validate that the class was set up correctly
        # Also want to validate that the dataframe we are taking in is usable
        cls._verify_alias_mapping(df=df)  # validates alias mapping is set up correctly
        cls._verify_column_ties_to_parameter(df=df)  # validates that all parameters are accounted for
        cls._verify_function_params_match()  # verifies that parameter types are uniform

        # STEP 2: GET THE MAPS WE NEED TO RUN THROUGH THE FUNCTIONS
        # now we want to iterate through the checks, making sure we have the args we need
        # we want to store the results in a flexible data type so that we can have many types of deliverables
        function_name_to_args_mapping = cls._map_function_to_args(df)  # gives me the func_name and (param and arg) tup
        function_mapping = cls._get_all_functions_in_class()
        function_to_function_type_map = cls._map_function_to_function_type()

        # STEP 3: RUN THE CHECKS
        results = cls._process_function_with_args(
            df,
            function_map=function_mapping,
            function_args=function_name_to_args_mapping,
            function_type_map=function_to_function_type_map,
            )
        return results

    @classmethod
    def _process_function_with_args(
            cls,
            df: pd.DataFrame,
            *,
            function_map: dict,
            function_type_map: dict[str, bool],  # true is series
            function_args: dict[str, list[tuple[str, dict]]],

    ) -> dict:
        """identifies the type of function we are dealing with, and will run the
        function and its arguments according to its needs. For example, a parameter of type pd.Series will
        run differently than a function running based on float.

        """
        # in the future, this may be under the Check class
        results = {}
        for function_name, function in function_map.items():
            param_args_list = function_args[function_name]
            match function_type_map[function_name]:
                case True:
                    temp = cls._process_function_as_series_function(function, df, param_args_list)
                case _:
                    temp = cls._process_function_as_scalar_function(function, df, param_args_list)
            results = results | temp
        return results

    @classmethod
    def _process_function_as_series_function(
            cls,
            function: Callable,
            df: pd.DataFrame,
            args_list: list[tuple[str, dict]]
    ) -> dict:
        """processes the class function assuming every parameter has a pd.Series as the param type

        :param function: function to run the args on
        :param df: the dataframe to run the args on
        :param args_list: a list of argument params

        :return: dict
        """
        # We need this because arg list only gives you the field name and doesn't tie it to an existing DF
        deliverable = {}
        new_keys = {
            args_tuple[0]: {
                param: df[column] for param, column in args_tuple[1].items()
            } for args_tuple in args_list
        }

        for ref_name, args in new_keys:
            results = function(**args)
            deliverable[ref_name] = results
        return deliverable

    @classmethod
    def _process_function_as_scalar_function(
            cls,
            function: Callable,
            df: pd.DataFrame,
            args_list: list[tuple[str, dict]]
    ) -> dict:
        """processes the class function assuming every parameter has a scalar as the param type

        :param function: function to run the args on
        :param df: the dataframe to run the args on
        :param args_list: a list of argument params

        :return: dict
        """
        deliverable = {}
        for args in args_list:
            temp = df[list(args[1].values())].copy()  # should be column names
            results = function(**temp.to_dict(orient="series"))
            deliverable[args[0]] = results
        return deliverable

    @classmethod
    def _format_flatten_parameters(cls, one_to_many_param: dict) -> list[dict]:
        """used when ONE parameter has MANY columns associated with it, this function will convert
        a nested list inside a dictionary, into a list of dictionaries with a key-value pair of
        `parameter: column_name`

        """
        flattened_params_combo = [
            dict(zip(one_to_many_param.keys(), values)) for values in zip(*one_to_many_param.values())
        ]
        return flattened_params_combo

    @classmethod
    def _format_args_reference_names(cls, arg_set: dict[str, str]) -> str:
        """need this function in order to accept the argument from `map_function_to_args`.
        This function allows us to reference the different arguments when we use the `run_validation` function.
        Having a reference point allows us to create different variations of a final deliverable.
        For example, we want to show check_a_v1: 100 errors, check_a_v2: 10 errors. This function creates the
        `check_a_v1` and `check_a_v2` reference names.

        :param arg_set: the argument set we are going to use for a given function, likely comes from the
        `map_function_to_args` function.

        :return: a formatted reference name
        """
        cleaned_string = "_".join((cls._format_column_names(column_name) for column_name in arg_set.values()))
        return cleaned_string

    @classmethod
    def _format_column_names(cls, name: str) -> str:
        """function for cleaning a column name. Can add onto this to cover more edge cases in the future.

        :param name: the column name or the string we want to format to make it Python friendly
        :return: a cleaned column name
        """
        name = name.strip("-").lower().replace(" ", "_")
        return name

    @classmethod
    def _map_function_to_args(cls, df: pd.DataFrame) -> dict[str, list[tuple[str, dict]]]:
        """identifies whether a param has a one-to-one or a one-to-many relationship with a column.
        Once identified, will flatten fields with a one-to-many, and will create a list of
        function args. Keeps in context of the check that the parameters are under to ensure
        that the arg dicts we made tie out correctly to the function. This is important so that the
        function argument list does not have a parameter that is not associated with the function.

        :return: a dictionary with key-value pairs of {`check`: [(`ref1`, {p1: c1, p2: c2, ...}),], ...}
        """
        check_mapping = cls._get_function_param_and_type()
        param_mapping = cls._map_param_to_columns(df)
        deliverable = {}

        for check, param_set in check_mapping.items():
            # STEP 1 is to categorize whether the param has a one-to-one or one-to-many relationship
            # We do this by comparing the length of the field list we have from the `get param column mapping` func
            one_to_one = {}
            one_to_many = {}
            for param, fields in param_mapping.items():
                if param in param_set:  # if this param is part of the check
                    if len(fields) == 1:  # only one field associated with the param. Previously known as static
                        one_to_one[param] = fields[0]
                    elif len(fields) > 1:
                        one_to_many[param] = fields
                    # ZERO param args should be caught in `verify_column_ties_to_param`

            # STEP 2 is to flatten out the parameter-column mapping for one-to-many relationships
            # we do this so that we can iterate on the parameters that can take in different columns
            # if we have stock_a_price and stock_b_price with stock_price as the parameter
            # we want to be able to use both stock_a and stock_b with the other parameters in the function

            # NOTE: realized that if I want to reference the params being used in the final deliverable,
            # NOTE cont: I need to be able to create a dict instead of a list to so that I can reference
            # NOTE cont: the changed parameter names
            flat_one_to_many = cls._format_flatten_parameters(one_to_many)
            arg_sets: list[dict] = [one_to_many_args | one_to_one for one_to_many_args in flat_one_to_many]  # the args
            deliverable[check] = [
                (cls._format_args_reference_names(args), args) for args in arg_sets
            ]
        return deliverable

    @classmethod
    def _map_param_to_columns(cls, df: pd.DataFrame) -> dict[str, Any]:
        """creates the actual mapping between the parameter and the columns associated with it,
        `_get_all_params_in_class` ties out the alias to the parameters. This class then goes
        one step further by adding any columns that match the name of the parameter.

        :param df: the dataframe we want to use
        :return: dictionary with key-value pair of `{parameter: [column, alias]}`
        """
        params = cls._get_all_params_in_class()
        param_column_mapping = {
            param: [column,] if alias_names is None and param == column
            else alias_names.append(column) if param == column and isinstance(alias_names, list)
            else alias_names
            for param, alias_names in params.items() for column in df.columns.values
        }
        return param_column_mapping

    @classmethod
    def _map_function_to_function_type(cls) -> dict[str, bool]:
        """creates a mapping so that we can tell the type of function we are dealing with.
        For example, currently, we are trying to figure out if the function uses a pd.Series type params
        or if it uses scalar values (float, int, etc.) as arg types. We can't have both.

        {`check`: `True` if series-based, `False` if scalar}

        """
        deliverable = {}
        for check, args in cls._get_function_param_and_type().items():
            _has_series_type = False
            _has_scalar_type = False
            for param, p_type in args.items():
                if isinstance(p_type, pd.Series):
                    _has_series_type = True
                elif isinstance(p_type, pd.DataFrame):
                    raise NotImplementedError(
                        f"We currently do not support Dataframes as a check argument."
                        f" Please update the check function to include either on pd.Series, list-like, or scalar values"
                    )
                else:  # we're going to assume single values for now and not lists. Those will error out for now.
                    _has_scalar_type = True
            if _has_series_type and _has_scalar_type:
                raise ValueError(f"The function cannot have both a scalar parameter and a series parameter."
                                 f" Please update the function to include one or the other, but not both")
            if _has_scalar_type:
                deliverable[check] = False
            else:
                deliverable[check] = True
        return deliverable

    @classmethod
    def _get_function_param_and_type(cls) -> dict:
        """private helper function used to get the class function's parameters and its associated type

        :return: a dictionary of `check: {parameter: Type, ...}` for all functions defined in the subclass.
        """
        _function_params = {
            check_name: {
                parameter: t for parameter, t in inspect.get_annotations(function) if parameter not in ('return',)
            } for check_name, function in cls._get_all_functions_in_class().items()
        }
        return _function_params

    @classmethod
    def _get_all_functions_in_class(cls) -> dict:
        """private helper function used to get a mapping of the check function name and the check function.
        Helpful when iterating through all the check functions in the class.

        The idea is to iterate through this dictionary in order to run all the defined functions in the
        super class.

        :return: a dictionary key-value pair of `check_name: check_function`
        """
        base_functions = {
            check: function for check, function in cls.__base__.__dict__.items() if inspect.isfunction(function)
        }
        curr_functions = {
            check: function for check, function in cls.__dict__.items() if inspect.isfunction(function)
        }
        return base_functions | curr_functions

    @classmethod
    def _get_all_params_in_class(cls) -> dict:
        """private helper function used to flatten the `get_function_param()` dictionary
        and return a dictionary of all parameters in the class. Helps to map columns to
        a parameter that's in the class

        This function flattens out all the parameters used in the super object, meaning, each
        parameter from all the functions are pulled into one dictionary.

        This will likely be used to tie out the parameters to different columns. Includes alias mapping.

        :return: a dictionary with a key-value pair of `parameter: list[column] | None` for all parameters in the class,
        taking into account all the functions in the class
        """
        all_params = {}
        for check_name, params in cls._get_function_param_and_type().items():
            for param in params:
                if param not in all_params:  # don't want to overwrite previous param
                    all_params[param] = None
                    # can delete if it's adding a layer of dependency we don't want
                    if param in cls.alias_mapping:  # adding the user defined param-column pairing
                        all_params[param] = cls.alias_mapping[param]
        return all_params

    @classmethod
    def _verify_column_ties_to_parameter(cls, df: pd.DataFrame) -> bool:
        """verifies that all the parameters in the super class has atleast one associated column
        tied to it.

        If that check fails, it will raise an error, and will notify the user to add the column
        to the `alias_mapping` variable so that is ties to the parameter it's missing,
        or to remove the function altogether if it's irrelevant.
        """
        column_mapping = {column: False for column in df.columns.to_list()}
        params = cls._get_all_params_in_class()

        # Tying Out the Columns
        missing_params = []
        for param, args in params.items():
            try:
                column_mapping[param] = True
            except KeyError:
                if args is None:  # because `get_all_params_in_class` will make the value a list from alias or None
                    missing_params.append(param)
        if missing_params:
            raise KeyError(
                f"Missing columns: {missing_params}. Declare the columns associated with the missing parameters"
                f" inside the `alias_mapping variable`."
            )
        return True

    @classmethod
    def _verify_alias_mapping(cls, df: pd.DataFrame) -> bool:
        """verifies that alias mapping was properly defined and usable.

        :param df: the dataframe we are using to tie to the Validation object
        :return: True if the alias mapping was properly defined and usable, raises an error otherwise
        """
        columns = {column: False for column in df.columns.to_list()}

        invalid_field_names = []
        for param in cls.alias_mapping:
            param_results = []
            for arg in cls.alias_mapping[param]:
                try:
                    columns[arg] = True
                    param_results.append(True)
                except KeyError:
                    param_results.append(False)
            if not any((result for result in param_results)):  # because none of the fields we defined are in the df
                invalid_field_names.append(param)

        if invalid_field_names:
            raise KeyError(
                f"{invalid_field_names} were not in the dataframe. "
                f"Please update the `alias_mapping` variable with the correct field names."
            )
        return True

    @classmethod
    def _verify_function_params_match(cls) -> bool:
        """want to make sure if one param is of type series, then another param matches, and should not be a
        scalar value

        :return:
        """
        for check, check_args in cls._get_function_param_and_type().items():
            _has_series_type = False
            _has_scalar_type = False
            for param, p_type in check_args.items():
                if isinstance(p_type, pd.Series):
                    _has_series_type = True
                elif isinstance(p_type, pd.DataFrame):
                    raise NotImplementedError(
                        f"We currently do not support Dataframes as a check argument."
                        f" Please update the check function to include either on pd.Series, list-like, or scalar values"
                    )
                else:  # we're going to assume single values for now and not lists. Those will error out for now.
                    _has_scalar_type = True
            if _has_series_type and _has_scalar_type:
                raise ValueError(f"The function cannot have both a scalar parameter and a series parameter."
                                 f" Please update the function to include one or the other, but not both")
        return True
