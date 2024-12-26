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
        # (!) we don't need to run this if there are NO SHARED PARAMETERS. Can check for it
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
            error = f"Couldn't find matching column for parameters: {_missing_parameters}."
            if self._flag_infer_shared_params is False:
                suggestion = f"Consider making setting `infer_shared` to `True`."
                error = f"{error} {suggestion}"
            raise ValueError(error)
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
        # Temp fix until we get this thing solved
        # The issue is that we'll eventually need to find a way to determine functions with
        # shared parameters and those without shared parameters and run them differently
        if shared_args:
            for parameter_set in combined_parameter_sets:
                match_mapping = {param: df[column] for param, column in parameter_set.items()}
                kw = self._generate_base_name(list(parameter_set.values())[0], kw_or_base=True)
                full_name = f"{check.name.strip('_')}_{kw}"

                # might need to confirm whether the return type is a pd.Series to determine formatting
                temp_result = check._check_function(**match_mapping)
                result[full_name] = temp_result

        # WE might want to add this under another function for handling value only functions
        else:
            check_name = check.name.strip('_')
            result[check_name] = df[static_args.keys()].apply(lambda row: check._check_function(**row), axis=1)

        self.results = self.results | result


ValidationType = TypeVar('ValidationType', bound=Validation)
"""Object type Validation
"""
