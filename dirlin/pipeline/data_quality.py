"""Still a work in progress to get this portion set up"""
import inspect
import pandas as pd
from typing import Callable, Any


class Check:
    def __init__(
            self,
            check_function: Callable[..., ...],
            *,
            option_keywords: list[str] | str = "option"
    ):
        ############################
        # HANDLING THE FUNCTION
        ############################
        # I think the fastest way to complete this change would be to create a mapping based on the info we need
        # Check needs to know exactly what parameters it needs

        # ==== function level info ====
        self._check_function = check_function
        """function wrapper. This function is used to perform boolean checks."""

        self.name: str = self._check_function.__name__
        """name of the function"""

        # ==== parameter level info ====
        # The mapping needs to have a set or dict that I can quickly reference multiple times
        # - parameter dictionaries could be built off this by using the keys
        self.__annotations__: dict[str, Any] = inspect.get_annotations(self._check_function)
        """all parameters of the function. `param`: `Type(arg)`, key-value pair"""

        # used for quick parameter name checks and class signatures
        self._param_signatures = list(inspect.signature(self._check_function).parameters)
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
                    result = self._check_function(*holder)
                    results.append(result)
                return results
            for holder in zip(*param.values()):
                result = self._check_function(self._class_sig, *holder)
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
        sig = list(inspect.signature(self._check_function).parameters)
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
            self,
            check: Check | list[Check],
            shared_param: list[str] | str | None = None,
    ):

        """class for handling all the various checks

        :param check: the checks you want to run in the pipeline
        :param shared_param: can denote parameters in the functions you expect are shared among multiple columns.
        An example is if a check (function) takes `team_qb` as a parameter, but the dataframe has columns
        like `raiders_qb` and `chiefs_qb` as columns. Argument `team_qb` would pull both columns as an arg.
        """
        if isinstance(check, Check):
            check = [check,]
        if not isinstance(check, list):
            raise TypeError(f"Expected a list or Check object, but got {type(check)}")

        #####################################
        # check collection layer api (for pipeline return)
        #####################################

        # ==== All ====
        self.checks_performed = check  # confirmed
        """List of all checks to perform for the validation"""

        # ==== Used to Create Arguments ====
        self.shared_param_column_map: dict[str, list] = dict()  # confirmed
        """`{parameter: list[column]}` key-value pairs of all the checks with shared parameters.
        
        To go one step further, these are the parameters that are "on deck" for the entire 
        set of checks that we are running. We'll only need to use the ones that are in the check we are using.
        """

        self.arg_map: dict[str, str] = dict()  # confirmed
        """The static parameters with only one column that matches the parameter.
        
        The idea is that if the column and parameter have a one-to-one relationships, then
        the arguments can be reused and extended with the `shared_param_column_map`.
        
        `{parameter: column}` key-value pairs with single column pairs. These are one-to-one relationships.
        """

        #############################
        # runtime variable layer (class functions)
        #############################

        # ==== All ====
        self._df_columns: pd.Index | list[str] | None = None
        """an index or list of column names in the given dataframe"""

        # ==== properties from `infer_param_class` ====
        # properties should hold information regarding the check
        self._column_base_name_map: dict[str, str] | None = None
        """used for finding shared parameters"""

        self._param_base_name_map: dict[str, str] | None = None
        """used for finding shared parameters"""

        self.shared_params: list[str] | list = shared_param if shared_param is not None else list()
        """list of params given by the validation class when it knows which column is a shared param"""

        self._flag_infer_shared_params: bool = False
        """will be set to True if arg is given on function call"""

        # ==== properties from `generate_arg_map` ====
        self._arg_map: dict[str, list[Any]] = dict()  # NOT CONFIRMED
        """`{check_name: list[args]}` key-value pairs"""

        # ==== properties from 'run_check_validation` ====
        self.results: dict[str, list] = dict()
        """Final dictionary that represents the results of the checks"""

    def run(
            self,
            df: pd.DataFrame,
            *,
            key_column: pd.Series | None = None,
            field_mapping: dict[str, str] | None = None,
            infer_shared: bool = False
    ) -> pd.DataFrame:
        """function that handles running the Dataframe and the various checks

        Still in the very early stages, but will likely expand to handle different errors and checks

        """
        # ===== handling function arguments and flags ====
        if field_mapping is not None:
            df = df.rename(columns=field_mapping)
        if infer_shared:
            self._flag_infer_shared_params = True
        if self._df_columns is None:
            self._df_columns = df.columns

        # ==== collection check function data ====
        for check in self.checks_performed:
            self._infer_param_class(check)  # map parameter to column relationships  (creates shared, static params)
            self._generate_arg_map(check)  # creates the set of arguments each function needs
            self._align_parameters(check, df)  # ties the parameter to the columns and runs the checks

        # ==== Creating Final Deliverable from Run ====
        if key_column is not None:
            temp_key_dict = {f"{key_column.name}": key_column}
            self.results = self.results | temp_key_dict
        temp_df = pd.DataFrame.from_dict(self.results)
        return temp_df

    def _generate_arg_map(self, check: Check):
        """creates individual param args based on what check returns

        populates the `self._arg_map` attribute that ties checks to their params.
        [['high', 'bar_low', 'foo_close'], ['high', 'foo_low', 'bar_close']]

        These are a list of arguments (parameters) that the function would accept, and includes shared params.
        """

        # (2) create a temporary list of the static args
        temp_static_arg_map = [param for param in self.arg_map if param in check.expected_arguments]

        # (1) first set up the key-value pairs of the params in this specific check. Creates temp mapping.
        if self.shared_param_column_map:
            temp_shared_arg_map = {}
            for arg in check.expected_arguments:
                if arg in self.shared_param_column_map:
                    temp_shared_arg_map[arg] = self.shared_param_column_map[arg]

            # (3) create the combination of possible arguments we will use when we get a tuple
            process_list = []
            """list[list[parameter] involved with the check"""
            for shared in zip(*(temp_shared_arg_map[key] for key in temp_shared_arg_map)):
                process_list.append(temp_static_arg_map + list(shared))
            self._arg_map[check.name] = process_list

        else:
            self._arg_map[check.name] = temp_static_arg_map

    def _infer_param_class(self, check: Check) -> None:
        """determines whether a parameter is a shared parameter or a single parameter.

        Adds `shared parameter` to self.shared_param_column_map and
        `non-shared parameter` to self.non_shared_param_column_map.

        A `shared parameter` has a one-to-many relationship between a single parameter
        and a DataFrame column.

        `Single parameters` have a one-to-one relationship between column names and
        parameter names.
        """
        # ==== Pre-Check to see if we have given shared params ====
        # Initialize the variables if base map was never initialized
        _found_shared_param = None
        """used for checking if a column is a shared parameter column"""

        if self.shared_params:
            _found_shared_param = any((p in self.shared_params for p in check.expected_arguments))

        if _found_shared_param or self._flag_infer_shared_params:
            if self._param_base_name_map is None and self._column_base_name_map is None:
                self._param_base_name_map = dict()
                self._column_base_name_map = dict()

                for column in self._df_columns:
                    base_name = self._generate_base_name(column)
                    # if base_name is not None:
                    self._column_base_name_map[column] = base_name

                for arg in check.expected_arguments:
                    if arg in self.shared_params or self._flag_infer_shared_params:
                        param_base_name = self._generate_base_name(arg)
                        # if param_base_name is not None:
                        self._param_base_name_map[arg] = param_base_name

        # ==== categorize the params between shared and non-shared ====
        # shared_param: {parameter_name: list[column names that share the parameter]}
        # non_shared: {parameter_name: column name}
        _missing_parameters = []
        for param in check.expected_arguments:
            match param in self._df_columns:
                case True:
                    if param not in self.arg_map:
                        self.arg_map[param] = param
                case False:  # is a shared parameter field
                    if self._flag_infer_shared_params or param in self.shared_params:
                        for column in self._df_columns:
                            # parameter nfl_qb matches column raiders_qb (qb == qb)
                            if self._column_base_name_map[column] == self._param_base_name_map[param]:
                                if param not in self.shared_param_column_map:
                                    self.shared_param_column_map[param] = list()
                                    if not _found_shared_param:
                                        _found_shared_param = True
                                if column not in self.shared_param_column_map[param]:
                                    self.shared_param_column_map[param].append(column)
                    else:
                        _missing_parameters.append(param)
        if _missing_parameters:
            raise ValueError(f"Couldn't find matching column for parameters: {_missing_parameters}")
        if _found_shared_param:
            _max_shared_size = max([len(self.shared_param_column_map[k]) for k in self.shared_param_column_map.keys()])
            for k, v in self.shared_param_column_map.items():
                if len(v) != _max_shared_size:
                    raise IndexError(
                        f"Length of shared parameter `{k}` ({len(v)}) != "
                        f"{_max_shared_size}. Size of shared columns must match."
                    )
        return None

    @staticmethod
    def _generate_base_name(field: str, kw_or_base: bool = False) -> str | None:
        """used for generating the string base to match for shared params

        If kw_or_base is set to `True`, will return the keyword. If set to
        `False`, will return the base string. Defaults to `False`.

        Returns None if column name is not able to be split. This would make it
        ineligible to be a shared parameter column name.

        """
        try:
            kw, base_name = str(field).split('_', maxsplit=1)
        except ValueError:
            return None
        if kw_or_base is False:
            return base_name
        return kw

    def _align_parameters(self, check: Check, df: pd.DataFrame):
        """Ties the `parameter: column` mapping (self.arg_map + self.shared_param_column_map) and runs the checks
        on the pd.Series by aligning the parameter to the pd.Series argument

        """
        # ==== creates the 'parameter: column' mapping ====
        static_args = {kw: self.arg_map[kw] for kw in check.expected_arguments if kw in self.arg_map}
        shared_args = {
            kw: self.shared_param_column_map[kw]
            for kw in check.expected_arguments if kw in self.shared_param_column_map
        }

        # ==== creates the `parameter: column` mapping for shared parameters ====
        # Further breaks down the shared parameter in a list of dictionary "sets" by splitting
        # We do this by splitting the shared args into different combinations
        shared_param_combos = [dict(zip(shared_args.keys(), values)) for values in zip(*shared_args.values())]
        combined_parameter_sets = [shared_combo | static_args for shared_combo in shared_param_combos]

        result = {}
        for parameter_set in combined_parameter_sets:
            match_mapping = {param: df[column] for param, column in parameter_set.items()}
            kw = self._generate_base_name(list(parameter_set.values())[0], kw_or_base=True)
            full_name = f"{check.name.strip('_')}_{kw}"

            # might need to confirm whether the return type is a pd.Series to determine formatting
            temp_result = check._check_function(**match_mapping)
            result[full_name] = temp_result
        self.results = self.results | result
