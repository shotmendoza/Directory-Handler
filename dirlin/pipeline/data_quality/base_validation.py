from typing import Any, TypeVar

import pandas as pd

from dirlin.pipeline.data_quality.base_check import Check, CheckType


class Validation:
    def __init__(
            self,
            check: CheckType | list[CheckType],
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
        self._shared_param_column_map: dict[str, list] = dict()  # confirmed
        """`{parameter: list[column]}` key-value pairs of all the checks with shared parameters.
        
        To go one step further, these are the parameters that are "on deck" for the entire 
        set of checks that we are running. We'll only need to use the ones that are in the check we are using.
        """

        self._arg_map: dict[str, str] = dict()  # confirmed
        """The static parameters with only one column that matches the parameter.
        
        The idea is that if the column and parameter have a one-to-one relationships, then
        the arguments can be reused and extended with the `shared_param_column_map`.
        
        `{parameter: column}` key-value pairs with single column pairs. These are one-to-one relationships.
        """

        self._option_map: dict[str, str] = dict()
        """the parameters for options with the prefix `option` which denotes a setting in the check"""

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

        self._shared_params: list[str] | list = shared_param if shared_param is not None else list()
        """list of params given by the validation class when it knows which column is a shared param"""

        self._flag_infer_shared_params: bool = False
        """will be set to True if arg is given on function call or found"""

        # ==== properties from 'run_check_validation` ====
        self.results: dict[str, list] = dict()
        """Final dictionary that represents the results of the checks"""

        self.flag_run_processed: bool = False
        """whether Validation.run() was ran. Marked True once it has run at  least once."""

        self.key_column: str | None = None
        """Used to store the key column if one was given"""

    def run(
            self,
            df: pd.DataFrame,
            *,
            key_column: str | None = None,
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
            self._align_parameters(check, df)  # ties the parameter to the columns and runs the checks

        # ==== Creating Final Deliverable from Run ====
        if key_column is not None:
            self.key_column = key_column
            temp_key_dict = {key_column: df[key_column]}
            self.results = self.results | temp_key_dict
        temp_df = pd.DataFrame.from_dict(self.results)
        self.flag_run_processed = True
        return temp_df

    def generate_base_results(self, key_column: str | None = None):
        """"""
        self._confirm_run_was_ran()

    def generate_error_log(self):
        """creates an error log based off the results of the Validation.run()

        Will have the number of lines the check has validated, and the number of
        errors that the check has found.

        Results:
                                              total_checked  errors
            check
            series_type_check_function_foobar             21       0
            series_type_check_function_foo                21       0
            series_type_check_function_bar                21       0


        :return: The DataFrame of the Error Log
        """
        self._confirm_run_was_ran()

        error_log = dict()
        for check, result in self.results.items():
            _error_count = len(result) - sum(result)
            _total_validated = len(result)
            error_log[check] = [_total_validated, _error_count]

        error_log_df = pd.DataFrame.from_dict(error_log, orient='index')
        error_log_df.columns = ['total_checked', 'errors']
        error_log_df.index.names = ['check']
        return error_log_df

    def generate_fix_file(self):
        """"""
        self._confirm_run_was_ran()

    def _confirm_run_was_ran(self):
        if self.flag_run_processed is False:
            raise RuntimeError(
                f"Expected Validation.run() method to be run once. "
                f"Please ensure it is before attempting to run the `generate` functions"
            )

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

        if self._shared_params:
            _found_shared_param = any((p in self._shared_params for p in check.expected_arguments))

        if _found_shared_param or self._flag_infer_shared_params:
            if self._param_base_name_map is None and self._column_base_name_map is None:
                self._param_base_name_map = dict()
                self._column_base_name_map = dict()

            for column in self._df_columns:
                base_name = self._generate_base_name(column)
                self._column_base_name_map[column] = base_name

            for arg in check.expected_arguments:
                if arg in self._shared_params or self._flag_infer_shared_params:
                    if arg not in self._param_base_name_map:
                        param_base_name = self._generate_base_name(arg)
                        self._param_base_name_map[arg] = param_base_name

        # ==== categorize the params between shared and non-shared ====
        # shared_param: {parameter_name: list[column names that share the parameter]}
        # non_shared: {parameter_name: column name}
        _missing_parameters = []
        for param in check.expected_arguments:
            match param in self._df_columns:
                case True:
                    if param not in self._arg_map:
                        self._arg_map[param] = param
                case False:  # is a shared parameter field / 2024.12.26 or an Options parameter
                    if self._flag_infer_shared_params or param in self._shared_params:
                        for column in self._df_columns:
                            # parameter nfl_qb matches column raiders_qb (qb == qb)
                            if self._column_base_name_map[column] == self._param_base_name_map[param]:
                                if param not in self._shared_param_column_map:
                                    self._shared_param_column_map[param] = list()
                                    if not _found_shared_param:
                                        _found_shared_param = True
                                if column not in self._shared_param_column_map[param]:
                                    self._shared_param_column_map[param].append(column)
                    elif str(param).startswith('option_'):
                        self._option_map[param] = param # need to figure out how to handle these
                    else:
                        _missing_parameters.append(param)
        if _missing_parameters:
            error = f"Couldn't find matching column for parameters: {_missing_parameters}."
            if self._flag_infer_shared_params is False:
                suggestion = f"Consider making setting `infer_shared` to `True`."
                error = f"{error} {suggestion}"
            raise ValueError(error)
        if _found_shared_param:
            _max_shared_size = max([len(self._shared_param_column_map[k]) for k in self._shared_param_column_map.keys()])
            _max_param = [
                param for param in self._shared_param_column_map.keys()
                if len(self._shared_param_column_map[param]) == _max_shared_size
            ][0]
            for k, v in self._shared_param_column_map.items():
                if len(v) != _max_shared_size:
                    _max_kw = set(
                        self._generate_base_name(column, kw_or_base=True)
                        for column in self._shared_param_column_map[_max_param]
                    )
                    _short_kw = [self._generate_base_name(column, kw_or_base=True) for column in v]
                    _mismatched_parameters = [kw for kw in _max_kw if kw not in _short_kw]

                    raise IndexError(
                        f"Length of shared parameter `{k}` ({len(v)}) != "
                        f"{_max_shared_size}. Size of shared columns must match. "
                        f"Missing Columns: {_mismatched_parameters}"
                    )
        return None

    def _align_parameters(self, check: Check, df: pd.DataFrame):
        """Ties the `parameter: column` mapping (self.arg_map + self.shared_param_column_map) and runs the checks
        on the pd.Series by aligning the parameter to the pd.Series argument

        """
        # ==== creates the 'parameter: column' mapping ====
        static_args = {kw: self._arg_map[kw] for kw in check.expected_arguments if kw in self._arg_map}
        shared_args = {
            kw: self._shared_param_column_map[kw]
            for kw in check.expected_arguments if kw in self._shared_param_column_map
        }

        # ==== creates the `parameter: column` mapping for shared parameters ====
        # Further breaks down the shared parameter in a list of dictionary "sets" by splitting
        # We do this by splitting the shared args into different combinations
        shared_param_combos = [dict(zip(shared_args.keys(), values)) for values in zip(*shared_args.values())]
        combined_parameter_sets = [shared_combo | static_args for shared_combo in shared_param_combos]

        result = {}
        if shared_args:
            for parameter_set in combined_parameter_sets:
                kw = self._generate_base_name(list(parameter_set.values())[0], kw_or_base=True)
                name = f"{check.name.strip('_')}_{kw}"

                reverse_parameter_set = {v: k for k, v in parameter_set.items()}
                temp_df = df.rename(columns=reverse_parameter_set)
                r = temp_df[parameter_set.keys()].apply(
                    lambda row: check.check_function(**row), axis=1
                )
                result[name] = r[0]  # confirm this works, seems a little shakey
        else:
            check_name = check.name.strip('_')
            result[check_name] = df[static_args.keys()].apply(lambda row: check.check_function(**row), axis=1)

        self.results = self.results | result

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


ValidationType = TypeVar('ValidationType', bound=Validation)
"""Object type Validation
"""
