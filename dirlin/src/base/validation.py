"""Part of the Dirlin Pipeline 2.0 (2025.04.30)"""

import dataclasses
import inspect
from typing import Any, Callable

import pandas as pd

from dirlin.src.base.util import DirlinFormatter


@dataclasses.dataclass
class _ResultWrapper:
    """used as a wrapper for the results coming from either a scalar function
    or a Series type function.

    Fields:
        - `result`: a pd.Series type variable that holds the results from running the function
        - `check_name`: a str type variable that holds the string name of the check that will be put in the deliverable
    """
    result: pd.Series
    """stores the results of the validation in the BaseValidation object"""

    check_name: str
    """stores the name of the check that will be put in the deliverable"""


class _BaseValidationVerifier:
    def __init__(
            self,
            df: pd.DataFrame,
            function_return_type: dict,
            function_param_and_type: dict,
            params_in_class: dict,
            alias_mapping: dict,
    ):
        """used for running verification on the base class to ensure that the formatting
        and organization of the dataframe is correct, and made in a way that the BaseValidation
        object can handle

        :param df: the dataframe we are verifying that follows the formatting for BaseValidation
        :param function_return_type: dictionary of {`check`: `return_type`}. Pulled from `_get_function_return_type`
        :param function_param_and_type: dictionary of `check: {parameter: Type}`. From `_get_function_param_and_type`
        :param params_in_class: dictionary of `parameter: list[column] | None`. From `_get_all_params_in_class`
        :param alias_mapping: {`parameter name`: [`associated columns`]}
        """
        self.df = df
        self.function_return_type = function_return_type
        self.function_param_and_type = function_param_and_type
        self.params_in_class = params_in_class
        self.alias_mapping = alias_mapping

    def _verify_function_param_return_match(self) -> bool:
        """verifies that the param types (pd.Series, scalar) matches the return type on a function.

        The context for this is when we are deciding to process as a Series function or a Scalar function,
        we need to add this cap in order to understand how to save the results.

        :return: True / False
        """
        missing_return_value = [
            function_name for function_name, return_type in self.function_return_type.items()
            if return_type is None
        ]
        if missing_return_value:
            raise ValueError(f"Functions {missing_return_value} is missing a return type.")

        for check, parameter in self.function_param_and_type.items():
            _has_series_type = False
            _has_scalar_type = False
            if all((isinstance(arg, pd.Series) for arg in parameter)):
                if not isinstance(self.function_return_type[check], pd.Series):
                    raise TypeError(f"The return type of a function must match the parameter types")
            else:
                _has_scalar_type = True
                # TODO: Not sure what to do right now with this part of the function
        return True

    def _verify_function_params_match(self) -> bool:
        """want to make sure if one param is of type series, then another param matches, and should not be a
        scalar value.

        May be deprecated in the future in favor of making sure that the return and parameter of
        the majority matches

        :return:
        """
        for check, check_args in self.function_param_and_type.items():
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

    def _verify_alias_mapping(self) -> bool:
        """verifies that alias mapping was properly defined and usable.

        :return: True if the alias mapping was properly defined and usable, raises an error otherwise
        """
        columns = {column: False for column in self.df.columns.to_list()}

        invalid_field_names = []
        if self.alias_mapping:
            for param in self.alias_mapping:
                param_results = []
                for arg in self.alias_mapping[param]:
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

    def _verify_column_ties_to_parameter(self) -> bool:
        """verifies that all the parameters in the super class has atleast one associated column
        tied to it.

        If that check fails, it will raise an error, and will notify the user to add the column
        to the `alias_mapping` variable so that is ties to the parameter it's missing,
        or to remove the function altogether if it's irrelevant.
        """
        column_mapping = {column: False for column in self.df.columns.to_list()}
        params = self.params_in_class

        # Tying Out the Columns
        missing_params = []
        for param, args in params.items():
            try:
                column_mapping[param]
            except KeyError:
                if args is None:  # because `get_all_params_in_class` will make the value a list from alias or None
                    missing_params.append(param)
        if missing_params:
            raise KeyError(
                f"Missing columns: {missing_params}. Declare the columns associated with the missing parameters"
                f" inside the `alias_mapping` variable in the class."
            )
        return True

    def check_all(self) -> bool:
        """we'll hard code all the checks for now, but this can be made more dynamic in the future.
        Will run all the validations we have set up so far to ensure proper formatting

        Will raise an error if any of the functions fail

        :return: True / False
        """
        self._verify_alias_mapping()  # validates alias mapping is set up correctly
        self._verify_column_ties_to_parameter()  # validates that all parameters are accounted for
        self._verify_function_params_match()  # verifies that parameter types are uniform
        self._verify_function_param_return_match()  # need input for the type of func param

        return True


# 2025.02.26 - creating new architecture for the quick pipeline
# we're doing this to make setup easier since it got a little complicated
# and time-consuming when onboarding new clients
class BaseValidation:
    """object used for Validation objects in order to help in reusing the same set of checks
    over different reports.

    ...

    The class should be inherited by a base set of classes `MyValidationSet(BaseValidation)`, where
    the `MyValidationSet` has defined checks as we will go over below. You could then layer these checks by
    inheriting the previous class SecondLayerValidation(MyValidationSet) with its own set of checks.

    The object inheriting BaseValidation class then uses the check function, `function_name = defined_function`

    ...

    For any columns that don't match directly with the parameter name, you can use `alias_mapping` to tie the parameter
    to the field name. Ex: `{price: [Total Price,], loss: [Total Loss,]}`

    We could even treat the alias_mapping as a dictionary,
    defining it as `alias_mapping[price] = [Total Price,]` and repeating for each parameter we want to define.
    """

    alias_mapping: dict[str, list | str] | None = dict()
    """used to define columns that don't exact-match a parameter in the object,
    but we want to use as an argument in the parameter.
    
    For example, if we have a column `Total Price` but our test function uses `price`
    as the parameter of the function, we would add `Total Price` as the value under
    `price` in the alias_mapping key-value pair. This would look like this:
    `{"price": ["Total Price]"}`.
    
    Is a key-value pair of {`parameter name`: [`associated columns`]}, and will tie into
    the function. The error code for `_verify_column_ties_to_parameter` will also notify
    you to add missing parameters into this variable as a dict.
    """

    _validator: _BaseValidationVerifier = _BaseValidationVerifier
    _formatter: DirlinFormatter = DirlinFormatter()
    """class level utility functions for parsing strings, numbers, and pd.Series"""

    @classmethod
    def run_validation(cls, df: pd.DataFrame) -> dict[str, dict]:
        # STEP 1: VALIDATE to validate that the class was set up correctly and is usable (GLOBAL)
        _verify = cls._validator(
            df,
            cls._get_function_return_type(),
            cls._get_function_param_and_type(),
            cls._get_all_params_in_class(),
            cls.alias_mapping,
        ).check_all()

        # STEP 2: INITIALIZE to create all the maps, to prepare for running the checks
        function_name_to_args_mapping = cls._map_function_to_args(df)  # gives me the func_name and (param and arg) tup
        """function used to pull the check functions, which includes the name, params, and args.
        This creates associations between the checks we have with the report columns we want to use as arguments.
        """
        function_mapping = cls._get_all_functions_in_class()  # gives me each function to iterate through
        """creates a mapping of the check_name, and the actual function code that goes with it.
        Helpful for creating an iterable for the next few steps
        """
        function_to_function_type_map = cls._map_function_to_function_type()  # gives me the function type (param?)
        """current limitation of the pipeline, but this function determines the parameter types, and ensures that
        we return the current type. This is needed because we can't return scalar values when the parameters are
        expecting a Series type.
        """

        # STEP 3: RUN THE CHECKS
        results = cls._process_function_with_args(
            df,
            function_map=function_mapping,
            function_args=function_name_to_args_mapping,
            function_type_map=function_to_function_type_map
        )
        return results

    @classmethod
    def _process_function_with_args(
            cls,
            df: pd.DataFrame,
            *,
            function_map: dict,
            function_type_map: dict[str, bool],  # true is series
            function_args: dict[str, list[dict]],

    ) -> dict:
        """identifies the type of function we are dealing with, and will run the
        function and its arguments according to its needs. For example, a parameter of type pd.Series will
        run differently than a function running based on float.

        Parameters:
            - function_args: gives you a list of parameter to column pairs. Each member of list
            has the same set of parameter keys, but different column values.

        """
        # in the future, this may be under the Check class
        results = {}
        for function_name, function in function_map.items():
            # function_args has the same key as function_map
            param_args_list = function_args[function_name]  # should be list of {param: col}
            match function_type_map[function_name]:
                case True:
                    result = cls._process_function_as_series_function(function, df, param_args_list)
                case _:
                    result = cls._process_function_as_scalar_function(function, df, param_args_list)
            temp = {r.check_name: r.result for r in result}  # todo only needs to be a list from result
            results = results | temp
        return results

    @classmethod
    def _process_function_as_series_function(
            cls,
            function: Callable,
            df: pd.DataFrame,
            args_list: list[dict[str, str]]
    ) -> list[_ResultWrapper]:
        """processes the class function assuming every parameter has a pd.Series as the param type, and also
        returns a pd.Series type

        :param function: function to run the args on
        :param df: the dataframe to run the args on
        :param args_list: a list of `param: column` pairs

        :return: _ResultWrapper
        """
        # We need this because arg list only gives you the field name and doesn't tie it to an existing DF
        deliverable = []
        new_keys = {
            cls._formatter.convert_dict_to_ref_names(args_pair, "series"): {
                param: df[column] for param, column in args_pair.items()
            } for args_pair in args_list
        }  # this should look like `check_name`: `{param: pd.Series}` aka args_pair

        # list comprehension to get all the results as a list
        deliverable = [_ResultWrapper(function(**args), name) for name, args in new_keys.items()]
        return deliverable

    @classmethod
    def _process_function_as_scalar_function(
            cls,
            function: Callable,
            df: pd.DataFrame,
            args_list: list[dict[str, str]]
    ) -> list[_ResultWrapper]:
        """processes the class function assuming every parameter has a scalar as the param type, and returns
        a single scalar type as well

        :param function: the callable function we will be validating the Dataframe with
        :param df: the dataframe to run the args on
        :param args_list: a list of `param: column` pairs

        Note that when you iterate through the args_list, you will get a different combination of columns.

        arg_list is from a higher level, and gives you a mapping of the columns involved with the function
        you are using.

        :return: list of ResultWrappers
        """
        deliverable = []
        for args_pair in args_list:
            # used for renaming the column to parameter names so that we can unpack as args
            reversed_param_column_dict = {column: param for param, column in args_pair.items()}
            temp: pd.DataFrame = df[list(reversed_param_column_dict)].copy()  # filter to keep only function context
            temp = temp.rename(columns=reversed_param_column_dict)  # rename for arg unpacking
            list_of_temp_args = temp.to_dict(orient='records')  # now we have a list of `param: args` pair
            # we can use this list to run through the scalar functions since this is a records dict

            # We need to iterate through these new results we just received
            results = [function(**args) for args in list_of_temp_args]
            check_name = cls._formatter.convert_dict_to_ref_names(args_pair)
            deliverable.append(_ResultWrapper(result=pd.Series(results), check_name=check_name))
        return deliverable

    @classmethod
    def _map_function_to_args(cls, df: pd.DataFrame) -> dict[str, list[dict]]:
        """identifies whether a param has a one-to-one or a one-to-many relationship with a column.
        Once identified, will flatten fields with a one-to-many, and will create a list of
        function args. Keeps in context of the check that the parameters are under to ensure
        that the arg dicts we made tie out correctly to the function. This is important so that the
        function argument list does not have a parameter that is not associated with the function.

        :return: a dictionary with key-value pairs of {`check`: [{p1: c1, p2: c2, ...}], ...}
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
            flat_one_to_many = cls._formatter.convert_dict_to_records(one_to_many)
            arg_sets: list[dict] = [one_to_one | one_to_many_args for one_to_many_args in flat_one_to_many]  # the args

            # 2025.03.21 previous way added too much complexity. Fixed to keep it clean.
            deliverable[check] = [args for args in arg_sets]
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
                if p_type == pd.Series:
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
                parameter: t
                for parameter, t in inspect.get_annotations(function).items() if parameter not in ('return',)
            } for check_name, function in cls._get_all_functions_in_class().items()
        }
        return _function_params

    @classmethod
    def _get_function_return_type(cls) -> dict:
        """private helper function that gets the class function's return

        :return: dictionary of {`check`: `return_type`}
        """
        check_return_annotation = {
            check_name: inspect.get_annotations(function)
            for check_name, function in cls._get_all_functions_in_class().items()
        }

        return_type_mapping = {
            check: param_pairs["return"] if "return" in param_pairs else None
            for check, param_pairs in check_return_annotation.items()
        }
        return return_type_mapping

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
                    if cls.alias_mapping is not None:
                        if param in cls.alias_mapping:  # adding the user defined param-column pairing
                            all_params[param] = cls.alias_mapping[param]
        return all_params
