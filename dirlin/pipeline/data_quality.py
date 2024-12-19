"""Still a work in progress to get this portion set up"""
import inspect
import pandas as pd
from typing import Callable, Any


class Check:
    def __init__(
            self,
            check_function: Callable[..., ...],
            *,
            shared_params: str | list[str] | None = None,
            excluded_prefixes: list[str] | None = None
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

        # Adding the Shared Params
        self.shared_param_keyword: list[str] = list()
        """Keeps a list of the first keyword used in the parameter. Similar to `option` in `option_variance_cap`"""
        self.shared_param_base: list[str] = list()
        """Keeps a list of the base word used in the parameter. Similar to `variance_cap` in `option_variance_cap`"""
        self.given_shared_params: list[str] = list()
        """Keeps the actual shared params in question"""
        self._shared_param_base_map = dict()
        """Shared Parameter Name: Parameter Base, key-value pairs"""

        # Excluded Prefix related variables (which ties in with Shared Params)
        self._excluded_prefixes: list[str] = list()
        """ties to shared_param_keyword. If keyword is in this list, then it's an excluded keyword prefix
        
        The idea is that if we have a base `profit` and have `q1_profit`, `q2_profit`, and `total_profit`
        as fields but don't want to include `total_profit` in our validation, we would exclude the
        `total_profit` field by excluding the `total` keyword. 
        """

        if excluded_prefixes is not None:
            self._excluded_prefixes = [kw for kw in excluded_prefixes]
        # Handles and updates the shared params
        if shared_params is not None:
            self.add_shared_param_(shared_params)

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

    def _handle_shared_params(self, shared_params: str | list[str]) -> None:
        if isinstance(shared_params, str):
            kw, base = shared_params.split('_', maxsplit=1)
            self.shared_param_keyword.append(list(kw))
            self.shared_param_base.append(list(base))
            self.given_shared_params.append(shared_params)

            self._shared_param_base_map[shared_params] = base

        elif isinstance(shared_params, list):
            self.shared_param_keyword = [param.split("_", 1)[0] for param in shared_params]
            self.shared_param_base = [param.split("_", 1)[1] for param in shared_params]
            self.given_shared_params = [param for param in shared_params]

            for base, kw in zip(self.shared_param_base, self.given_shared_params):
                self._shared_param_base_map[kw] = base
        else:
            raise NotImplementedError(f"Did not recognize shared_params: {shared_params}")

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
                    result = self._check_function(*holder)
                    results.append(result)
                return results
            for holder in zip(*param.values()):
                result = self._check_function(self._class_sig, *holder)
                results.append(result)
        return results

    def add_shared_param_(self, shared_param: str) -> None:
        """adds a keyword that a higher level object wants to use as a keyword for any shared parameter

        For example, if the use when setting up the check has `team_qb` as a parameter in a function
        but expects columns like `raiders_qb` or `chiefs_qb` fields to also work, they would add
        `team` as an argument in `shared_param_keyword` or on the object instantiation.

        """
        # Can probably add a validation step above
        self._handle_shared_params(shared_param)

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
        sig = list(inspect.signature(self._check_function).parameters)
        if sig[0] in self._instance_param:
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
                    return self._check_function(**args_map)
                result = self._check_function(class_sig, **args_map)
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
                    result = self._check_function(**temp_args_map)
                else:
                    result = self._check_function(class_sig, **temp_args_map)
            else:
                result = self._handle_non_series_validation(args_map=temp_args_map)

            sub_results[curr_column] = result
            counter += 1
            if (counter + 1) > _max_size:
                go_on = False

        # Creating a copy of the dataframe and adding new data
        return_data = data.copy()
        for column, value in sub_results.items():
            func_name = str(self._check_function.__name__)
            func_name = func_name.lstrip("_")
            return_data[f"{func_name}_{column}"] = pd.Series(value)
        return return_data


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
        """function that handles running the Dataframe and the various checks

        Still in the very early stages, but will likely expand to handle different errors and checks

        """
        if field_mapping is not None:
            # renaming the report based on the checks you would like to run
            df = df.rename(columns=field_mapping)

        for check in self._checklist:
            result = check.validate(df)
            if isinstance(result, pd.DataFrame):
                for column in result.columns.values.tolist():
                    if column not in df.columns:
                        df[column] = result[column]
            else:
                df[check.name] = result
        return df
