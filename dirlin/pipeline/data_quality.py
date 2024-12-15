"""Still a work in progress to get this portion set up"""
import inspect

import pandas as pd
from typing import Callable, Any


class Check:
    def __init__(self, check_function: Callable[..., ...]):
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

        self.__annotations__: dict[str, Any] = inspect.get_annotations(self._check_function)

        # Adding context variables
        self._required_fields: dict[str, Any] | None = None
        self._exempt_keywords_used: list[str] | None = None

    def _identify_needed_fields(
            self,
            *,
            available_fields: pd.Index | list[str] | None,
            keep_exempt_keywords_used: bool = True,
            keep_required_field_context: bool = True
    ) -> dict[str, Any]:
        """

        """
        requested_fields = self.__annotations__.copy()

        # Running through the exemption logic
        exempt_fields = []
        for field in requested_fields:
            # split on _ and look in first half for exempt keywords
            if 'option' in str(field).split('_', 2)[0]:
                exempt_fields.append(field)

            # adding the used exempt keyword back to the context
            if keep_exempt_keywords_used:
                self.exempt_keywords_used = exempt_fields

        # Removing unnecessary field names like 'return' from __annotations__
        del requested_fields['return']

        # Checking the naming convention when available headers are given
        if available_fields is not None:
            missing_fields = []
            for field in requested_fields:
                if field not in available_fields:
                    if field not in exempt_fields:
                        missing_fields.append(field)
                if missing_fields:
                    _missing_label = ", ".join(missing_fields)
                    raise KeyError(f"Missing fields {_missing_label} from available fields.")

        # Keeping the context when run
        if keep_required_field_context:
            self._required_fields = requested_fields
        return requested_fields

    def validate(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame | Any:
        """maps fields to function kwargs and runs the validation function

        Still a work in progress (12/12/2024)

        """
        if not self._required_fields:
            self._identify_needed_fields(available_fields=data.columns)

        is_series_type = False
        """whether the parameters use pd.Series as argument types"""
        if (
            any(
                [
                    isinstance(v, pd.Series) and k not in self.exempt_keywords_used
                    for k, v in self._required_fields.items()
                ]
            )
        ):
            is_series_type = True
        """
        We should work to refine this section so that a range of args can be used
        Currently we assume that if the first arg is a pd.Series, then the rest
        should follow that same format
        """
        args_map = dict()
        for k, v in self._required_fields.items():
            if k not in self.exempt_keywords_used:
                args_map[k] = data[k].astype(v)

            # checks for the option keywords
            # adds to the arg dict if it is, leaves blank if not
        for exempt_kw in self.exempt_keywords_used:
            if exempt_kw in kwargs:
                args_map[exempt_kw] = kwargs[exempt_kw]

        if is_series_type:
            result = self._check_function(args_map)
            return result
        return self._handle_non_series_validation(args_map=args_map)

    def _handle_non_series_validation(self, args_map: dict[str, Any]):
        results = []
        for holder in zip(*args_map.values()):
            result = self._check_function(*holder)
            results.append(result)
        return results


class Validation:
    def __init__(self, check: Check | list[Check]):
        if isinstance(check, Check):
            check = [check,]

        self._check_list = [c for c in check]  # Redundant?

    def run(self, df: pd.DataFrame):
        for check in self._check_list:
            check.validate(df)
