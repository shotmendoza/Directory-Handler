import inspect
from typing import Callable, Any, TypeVar

import pandas as pd


class Check:
    def __init__(
            self,
            check_function: Callable[..., ...],
            *,
            fix_function: Callable[..., Any] | None = None,
            option_keywords: list[str] | str = "option"
    ):
        ############################
        # HANDLING THE FUNCTION
        ############################
        # I think the fastest way to complete this change would be to create a mapping based on the info we need
        # Check needs to know exactly what parameters it needs

        # ==== function level info ====
        self.check_function = check_function
        """function wrapper. This function is used to perform boolean checks."""

        self.fix_function = fix_function
        """function wrapper. Going to start off by separating the fix from checking, but will end up combining logic"""

        self.name: str = self.check_function.__name__
        """name of the function"""

        # ==== parameter level info ====
        # The mapping needs to have a set or dict that I can quickly reference multiple times
        # - parameter dictionaries could be built off this by using the keys
        self.__annotations__: dict[str, Any] = inspect.get_annotations(self.check_function)
        """all parameters of the function. `param`: `Type(arg)`, key-value pair"""

        # used for quick parameter name checks and class signatures
        self._param_signatures = list(inspect.signature(self.check_function).parameters)
        """similar to __annotations__ but discloses only name and use of self or cls as an argument (param instance)"""

        # class signatures (cls, self)
        self._arg_class_signature: str | None = self._handle_class_signatures()
        """referenced when determining whether the function calls is part of a class or instantiated object"""

        # option signatures ('option', user input)
        self._arg_option_signature: list = self._handle_string_to_list_conversion(option_keywords)
        """defaults to `option`. Marks params with option as a prefix. Denotes params not tied to specific fields."""

        # ==== API level info ====
        # this section is for what the check needs to request or respond with to other requests
        # - For example, we need to know what parameters we are expecting from outside sources
        self.expected_arguments: dict[str, Any] = {
            param: ptype for param, ptype in self.__annotations__.items()
            if param not in self._arg_option_signature and param != "return"
        }
        """returns the expected parameters of the check function

        For example, if a function has the parameters `def function(arg1, arg2, arg3)`,
        the function will return a dictionary of `{arg1: type(arg1), arg2: type(arg2), arg3: type(arg3)}`.
        
        """

    def _handle_class_signatures(self) -> str | None:
        """identifies whether the check function has class arguments (self, cls) as its first argument

        Example of a function without a signature:
            - def function_without_signature(param1: type1, param2: type2)

        Example of a function with a signature:
            - def function_with_signature(self, param1: type1, param2: type2)

        Returns the class argument itself (self, cls) if the function has a class signature, or None
        if there is no class signature in the function.
        """
        _example_param_instances = ('self', 'cls')
        """Represents the arguments used for class functions or instantiated object functions"""

        if self._param_signatures:
            first_arg = self._param_signatures[0]
            first_arg_is_class_signature = first_arg in _example_param_instances
        else:
            raise KeyError(f"{self.name} has no parameters!")
        if not first_arg_is_class_signature:
            return None
        return first_arg

    def _handle_string_to_list_conversion(self, multi_type_param: str | list[str]) -> list[str] | list:
        """identifies the argument type, and converts it into a list of string

        Specifically used for parameters in the class that requires str types to be converted to lists
        to make it easier to loop through without considering scalar values.

        Returns empty list `[]` if the argument is not given.

        """
        if isinstance(multi_type_param, str):
            return [multi_type_param]
        elif isinstance(multi_type_param, list):
            return multi_type_param
        elif multi_type_param is None:
            return []
        else:
            raise TypeError(f"{self.name} can only handle string or list")

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
        print()
        if fields_available is not None:
            missing_fields = []
            for field in requested_func_param:
                try:
                    _split_field = field.split('_', maxsplit=1)[1]  # for use for shared fields
                except IndexError:
                    _split_field = ""
                if field not in fields_available:
                    if _split_field not in self.shared_param_base:  # Checking to see if it's a shared param
                        if field not in exempt_fields:
                            missing_fields.append(field)
                if missing_fields:
                    _missing_label = ", ".join(missing_fields)
                    raise KeyError(f"Missing fields {_missing_label} from available fields.")

        # Keeping the context when run
        if keep_required_field_context:
            self._required_params = requested_func_param
        return requested_func_param

    def _handle_non_series_validation(self, args_map: dict[str, Any] | list[dict[str, Any]]) -> list:
        """Handles dict unwrapping for Non-Series type of check function params

        :param args_map: dict with params names as keys and arguments as values
        """
        # Possibly sensitive to ordering issues
        results = []
        if isinstance(args_map, dict):
            args_map = [args_map]

        for param in args_map:
            if self._class_sig is None:
                for holder in zip(*param.values()):
                    result = self.check_function(*holder)
                    results.append(result)
                return results
            for holder in zip(*param.values()):
                result = self.check_function(self._class_sig, *holder)
                results.append(result)
        return results

    def validate(self, data: pd.DataFrame, **kwargs) -> pd.Series | list[bool] | pd.DataFrame:
        """maps fields to function kwargs and runs the validation function

        :param data: the data to validate. Usually in the form of pd.DataFrame
        :param kwargs: the kwargs to pass to the validation function
        :return: the validation result. list[bool], pd.Series, pd.Dataframe depending on the function
        and if there are shared parameters or not
        """
        if not self._required_params:
            _required_fields_kw = {
                k: w for k, w in kwargs.items() if k in ('keep_exempt_keywords_used', 'keep_required_field_context')
            }
            self._identify_needed_params(fields_available=data.columns, **_required_fields_kw)

        # Validating that the shared param fields given matches with the available function params
        not_a_parameter = []
        for param in self.given_shared_params:
            if param not in self._required_params:
                not_a_parameter.append(param)
        if not_a_parameter:
            raise KeyError(f"Parameter {not_a_parameter} is not a parameter in the function.")

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
        sig = list(inspect.signature(self.check_function).parameters)
        if sig[0] in self._param_instances:
            class_sig = sig[0]
        self._class_sig = class_sig

        # Going to split to start off, so it's easier for me to conceptualize
        for k, v in self._required_params.items():
            if k not in self.given_shared_params:
                if k not in self._exempt_keywords_used:
                    if not is_series_type:
                        args_map[k] = data[k].astype(v)
                    args_map[k] = data[k]

        # This portion is for if there's a shared param (split out to make it easier to conceptualize)
        shared_param_column_mapping = dict()
        """parameter name: set(df[column], column) key-value. (team_qb: [raiders_qb, chiefs_qb])"""
        for k, v in self._required_params.items():
            if k in self._shared_param_base_map:
                shared_param_column_mapping[k] = list()
                curr_shared_param_base = self._shared_param_base_map[k]
                for column in data.columns.values.tolist():
                    args_shares_param_base = column.endswith(curr_shared_param_base)
                    arg_is_not_base = column != curr_shared_param_base
                    arg_kw_is_excluded = any((column.startswith(ep) for ep in self._excluded_prefixes))
                    if args_shares_param_base and not arg_kw_is_excluded and arg_is_not_base is True:
                        try:
                            shared_param_column_mapping[k].append((data[column], column))
                        except IndexError:
                            print("WE ERRORED")

        # ERROR CHECK TO CONFIRM THAT SHARED PARAMS ALL ALIGN CORRECTLY
        # IN THE FUTURE, WE CAN ALSO MAKE SURE THAT THE KEYWORDS ALIGN AS WELL
        if shared_param_column_mapping:
            _size_mapping = {c: len(shared_param_column_mapping[c]) for c in shared_param_column_mapping}
            _curr_max_param_size = max(_size_mapping.values())
            _set_size_errors = []
            for p, sets in shared_param_column_mapping.items():
                if len(sets) > _curr_max_param_size:
                    _set_size_errors.append(p)
                    raise IndexError(
                        f"Size mismatches, expected to equal {_size_mapping}"
                        f"Update `excluded_keywords`"
                    )

        # checks for the option keywords
        # adds to the arg dict if it is, leaves blank if not
        for exempt_kw in self._exempt_keywords_used:
            if exempt_kw in kwargs:
                args_map[exempt_kw] = kwargs[exempt_kw]

        if not self.given_shared_params:
            if is_series_type:
                if class_sig is None:
                    return self.check_function(**args_map)
                result = self.check_function(class_sig, **args_map)
                return result
            return self._handle_non_series_validation(args_map=args_map)

        # Since there are shared parameters, we'll be returning a DataFrame with all
        sub_results = {}
        go_on = True
        counter = 0
        while go_on is True:
            temp_args_map = args_map.copy()
            _max_size = max([len(shared_param_column_mapping[k]) for k in shared_param_column_mapping.keys()])
            curr_column: str | None = None
            """stores column name"""

            for idx, arg in enumerate(self.given_shared_params):  # shows args in shared_params (team_qb, curr_net)
                temp_args_map[arg] = shared_param_column_mapping[arg][counter][0]
                curr_column = shared_param_column_mapping[arg][counter][1]

            if is_series_type:
                if class_sig is None:
                    result = self.check_function(**temp_args_map)
                else:
                    result = self.check_function(class_sig, **temp_args_map)
            else:
                result = self._handle_non_series_validation(args_map=temp_args_map)

            sub_results[curr_column] = result
            counter += 1
            if (counter + 1) > _max_size:
                go_on = False

        # Creating a copy of the dataframe and adding new data
        return_data = data.copy()
        for column, value in sub_results.items():
            func_name = str(self.check_function.__name__)
            func_name = func_name.lstrip("_")
            return_data[f"{func_name}_{column}"] = pd.Series(value)
        return return_data


CheckType = TypeVar('CheckType', bound=Check)
"""Object type Check"""
