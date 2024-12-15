"""Still a work in progress to get this portion set up"""
import inspect

import pandas as pd
from typing import Callable, Any


class Check:
    def __init__(
            self, check_function: Callable[..., ...],
    ):
        self._check_function = check_function
        self._exempt_keywords = ["option"]
        """used for parameters in functions that aren't in a DataFrame but we need as args
        
        Denoting parameters with keywords (that start with a special keyword in the beginning
        of the parameter name) so the function can identify them in the function call.
        
        Exempt Keywords:
            `option`: any string that starts with `option` will be removed from keywords used in the
            dataframe checks. For example, `option_variance_cap` will be marked as being an option
            and not a parameter in the check itself
        """

        self._instance_param = ('self', 'cls')
        """used for when a function is part of a class or class method
        
        we'll add the given signature as part of the function call
        """

        self.__annotations__: dict[str, Any] = inspect.get_annotations(self._check_function)

        # Adding context variables
        self._required_params: dict[str, Any] | None = None
        self._exempt_keywords_used: list[str] | None = None
        self._expected_return: type | None = None
        self._class_sig: str | None = None

        # name of the function
        self.name: str = self._check_function.__name__

    def _identify_needed_params(
            self,
            *,
            fields_available: pd.Index | list[str] | None,
            keep_exempt_keywords_used: bool = True,
            keep_required_field_context: bool = True
    ) -> dict[str, Any]:
        """identifies the parameters that are involved in the function that this object wraps around

        :param fields_available: the columns available in the dataframe

        :param keep_exempt_keywords_used: keeps the state of exempt parameters if True. Will not if False.
        defaults to True.

        :param keep_required_field_context: keeps the state of required parameters if True. Will not if False.
        defaults to True.

        """
        requested_func_param = self.__annotations__.copy()

        # Running through the exemption logic
        exempt_fields = []
        for param in requested_func_param:
            # split on _ and look in first half for exempt keywords
            if 'option' in str(param).split('_', 2)[0]:
                exempt_fields.append(param)
            # adding the used exempt keyword back to the context
            if keep_exempt_keywords_used:
                self._exempt_keywords_used = exempt_fields

        # Removing unnecessary field names like 'return' from __annotations__
        if 'return' in requested_func_param:
            del requested_func_param['return']

        # Checking the naming convention when available headers are given
        if fields_available is not None:
            missing_fields = []
            for field in requested_func_param:
                if field not in fields_available:
                    if field not in exempt_fields:
                        missing_fields.append(field)
                if missing_fields:
                    _missing_label = ", ".join(missing_fields)
                    raise KeyError(f"Missing fields {_missing_label} from available fields.")

        # Keeping the context when run
        if keep_required_field_context:
            self._required_params = requested_func_param
        return requested_func_param

    def _handle_non_series_validation(self, args_map: dict[str, Any]):
        """Handles dict unwrapping for Non-Series type of check function params

        :param args_map: dict with params names as keys and arguments as values
        """
        # Possibly sensitive to ordering issues
        results = []
        if self._class_sig is None:
            for holder in zip(*args_map.values()):
                result = self._check_function(*holder)
                results.append(result)
            return results
        for holder in zip(*args_map.values()):
            result = self._check_function(self._class_sig, *holder)
            results.append(result)
        return results

    def validate(self, data: pd.DataFrame, **kwargs) -> pd.Series | list[bool]:
        """maps fields to function kwargs and runs the validation function

        :param data: the data to validate. Usually in the form of pd.DataFrame
        :param kwargs: the kwargs to pass to the validation function
        :return: the validation result. list[bool] or pd.Series depending on the function
        """
        if not self._required_params:
            _required_fields_kw = {
                k: w for k, w in kwargs.items() if k in ('keep_exempt_keywords_used', 'keep_required_field_context')
            }
            self._identify_needed_params(fields_available=data.columns, **_required_fields_kw)

        is_series_type = False
        """whether the parameters use pd.Series as argument types"""
        if (
            any([v is pd.Series and k not in self._exempt_keywords_used
                for k, v in self._required_params.items()])
        ):
            is_series_type = True
        """
        We should work to refine this section so that a range of args can be used
        Currently we assume that if the first arg is a pd.Series, then the rest
        should follow that same format
        """
        args_map = dict()
        class_sig: str | None = None

        # Checking for class signatures like self, cls
        sig = list(inspect.signature(self._check_function).parameters)
        if sig[0] in self._instance_param:
            class_sig = sig[0]
        self._class_sig = class_sig

        for k, v in self._required_params.items():
            if k not in self._exempt_keywords_used:
                args_map[k] = data[k]
                if not is_series_type:
                    args_map[k] = data[k].astype(v)

            # checks for the option keywords
            # adds to the arg dict if it is, leaves blank if not
        for exempt_kw in self._exempt_keywords_used:
            if exempt_kw in kwargs:
                args_map[exempt_kw] = kwargs[exempt_kw]

        if is_series_type:
            if class_sig is None:
                return self._check_function(**args_map)
            result = self._check_function(class_sig, **args_map)
            return result
        return self._handle_non_series_validation(args_map=args_map)


class Validation:
    def __init__(
            self, check: Check | list[Check],
    ):

        """class for handling all the various checks

        :param check: the checks you want to run in the pipeline
        :param temp_formatting: an optional formatting in case the dataframe doesn't follow proper naming conventions
        """
        if isinstance(check, Check):
            check = [check,]
        if not isinstance(check, list):
            raise TypeError(f"Expected a list or Check object, but got {type(check)}")

        self._checklist = check

    def run(
            self,
            df: pd.DataFrame,
            field_mapping: dict[str, str] | None = None
    ) -> pd.DataFrame:
        if field_mapping is not None:
            df = df.rename(columns=field_mapping)

        for check in self._checklist:
            result = check.validate(df)
            df[check.name] = result
        return df
