import logging

import pandas as pd
from tqdm import tqdm


class DirlinFormatter:
    """utility object used for string formatting and series formatting
    """
    @classmethod
    def convert_string_to_python_readable(cls, name: str) -> str:
        """function for cleaning a column name. Can add onto this to cover more edge cases in the future.

        The function will allow you to transform a standard column `California Taxes` to a python
        usable format of `california_taxes`

        :param name: the column name or the string we want to format to make it Python friendly
        :return: a cleaned column name
        """
        name = name.strip("-").lower().replace(" ", "_")
        return name

    # todo should be convert string to number, with int or float as the param
    @classmethod
    def convert_string_to_integer(
            cls,
            key_field: pd.Series,
            balance_field: pd.Series | None = None
    ) -> pd.Series:
        """may be used for something like limits in coverages, but will parse and format a pd.Series of strings
        in a pd.Series of integers

        :param key_field: a pd.Series of strings that we want to convert to a series of integers
        :param balance_field: when given, will use the signs on the column to normalize against. For example,
        a negative number on the balance field will convert the key_field to a negative as well

        :return: a formatted pd.Series of integers
        """
        # (1) First stab at converting the key column. Assuming simplest case.
        try:
            key_field = key_field.fillna(0)
            key_field = key_field.astype(float).astype(int)
        except ValueError:
            key_field = key_field.astype(str).fillna("0")

            # formatting the string to clean up bad values
            key_field = key_field.str.strip()
            key_field = key_field.str.rstrip("_")
            key_field = key_field.str.lower().replace("nan", "0")
            key_field = key_field.str.replace("$", "")  # handle $
            key_field = key_field.str.replace(",", "")  # handles commas
            key_field = key_field.str.replace("_", "")
            key_field = key_field.str.replace(' ', "")  # handles spaces
            key_field = key_field.str.replace('no', "")  # handles no
            key_field = key_field.str.replace('none', "")  # handles none

        # (2) Second stab after trying to clean the column with any special values
        try:
            key_field = key_field.astype(str).fillna("0")
            key_field = key_field.astype(float).astype(int)
        except ValueError:
            key_field = pd.to_numeric(key_field, errors='coerce', downcast='integer')
            key_field = key_field.fillna(0)
            key_field = key_field.astype(float).astype(int)
        except Exception as e:
            # catch all for now to future-proof any errors we might see
            raise e

        # (3) the optional parameter to normalize against the balance field
        if isinstance(balance_field, pd.Series):
            key_field = pd.Series(
                [abs(amount) if b >= 0 else abs(amount) * -1 for amount, b in zip(key_field, balance_field)]
            )
        return key_field

    @classmethod
    def convert_dict_to_records(cls, one_to_many_param: dict) -> list[dict]:
        """used when ONE key parameter in a dictionary has MANY column values associated with it.
        this function will convert a nested list inside a dictionary, into a list of dictionaries
        with a key-value pair of `parameter: column_name`

        FROM one_to_many_param TO list of dictionaries:
        FROM `{A: [1, 2, 3], B: [A, B, C]}` TO `[{A: 1, B: A}, {A: 2, B: B}, {A: 3, B: C}]`

        Similar to a record in pd.Dataframe.to_dict(records)
        """
        flattened_params_combo = [
            dict(zip(one_to_many_param.keys(), values)) for values in zip(*one_to_many_param.values())
        ]
        if not flattened_params_combo:
            flattened_params_combo = [dict()]
        return flattened_params_combo

    @classmethod
    def convert_dict_to_ref_names(
            cls,
            arg_pair: dict[str, str],
            use_keys: bool = False,
            prefix: str | int | None = None
    ) -> str:
        """need this function in order to accept the argument from `map_function_to_args`.
        This function allows us to reference the different arguments when we use the `run_validation` function.

        Having a reference point allows us to create different variations of a final deliverable.
        For example, we want to show check_a_v1: 100 errors, check_a_v2: 10 errors. This function creates the
        `check_a_v1` and `check_a_v2` reference names.

        :param use_keys: when True, will use the key values of the dictionary. Defaults to False.
        :param prefix: adds a string to the front of the text
        :param arg_pair: the argument set we are going to use for a given function, likely comes from the
        `map_function_to_args` function.

        :return: a formatted reference name
        """
        columns = arg_pair.values()
        if use_keys is True:
            columns = arg_pair.keys()

        cleaned_string = "_".join(
            (cls.convert_string_to_python_readable(column_name) for column_name in columns)
        )
        if prefix is not None:
            cleaned_string = f"instance_{prefix}_{cleaned_string}"
        return cleaned_string

    @classmethod
    def convert_string_to_percentage(cls, percentage_field: pd.Series) -> pd.Series:
        """formats a percentage column with two decimal places and a percentage symbol

        :param percentage_field: the string field with the percentage values
        """
        percentage_field = percentage_field.fillna("0").str.rstrip('%').astype(float)
        return pd.Series([f"{round(float(value)): .2f}" for value in percentage_field])

    @classmethod
    def convert_percentage_to_float(cls, percentage_field: pd.Series) -> pd.Series:
        """formats a percentage column into a float column
        """
        try:
            percentage_field = percentage_field.fillna("0").str.rstrip('%').astype(float) / 100  # todo cond. on %
        except AttributeError:
            percentage_field = percentage_field.fillna(0).astype(float) / 100
        return pd.Series(percentage_field)

    @classmethod
    def flip_signature(cls, signature_column: pd.Series) -> pd.Series:
        """flips the signature of all the columns in the signature_column argument

        The function assumes that Report.format() has been run at least once so that
        Report._df is populated.

        This function is used any time a 'reversal' file needs to be created.
        If you need to back out the original amount, or 'zero-out' a column
        so that two DataFrames equal 0, then this function is helpful.

        :param signature_column: the column to flip through
        :return: DataFrame with the required fields having flipped signatures
        """
        signature_column = signature_column.fillna(0)
        signature_column = signature_column * -1
        return signature_column

    @classmethod
    def format_zip_fields(cls, zip_field: pd.Series) -> pd.Series:
        """retains the leading 0 in a zip code string field

        :param zip_field: the field with the Zip Code values
        """
        return zip_field.astype(str).str.extract('(\d+)', expand=False).str.zfill(5)


class TqdmLoggingHandler(logging.Handler):
    """handles logging for TQDM messages and errors on the console
    """
    def __init__(self, level=logging.INFO):
        super().__init__(level)

        # [1.0] set formatting
        _default_format = "(%(asctime)s | %(levelname)s | %(message)s)"
        self.setFormatter(logging.Formatter(_default_format))

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        tqdm.write(msg)
