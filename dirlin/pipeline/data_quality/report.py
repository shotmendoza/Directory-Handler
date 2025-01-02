from typing import TypeVar

import pandas as pd


class Report:
    """creates a dataframe 'report' usable by the Dirlin Pipeline

    Runs proper formatting and checks on the values, applies back-fill logic
    to missing data in order to ensure quality in the data.

    """
    def __init__(
            self,
            name_convention: str | None = None,
            field_mapping: dict[str, str] | None = None,
            *,
            df: pd.DataFrame | None = None,
            column_type_floats: list[str] | None = None,
            column_type_ints: list[str] | None = None,
            column_type_dates: list[str] | None = None,
            column_type_cash: list[str] | None = None,
            key_cash_column: str | None = None,
    ):
        """sets up a Report to prep for formatting the report

        :param name_convention:
        :param field_mapping: used for renaming the columns in the report
        :param column_type_floats:
        :param column_type_ints:
        :param column_type_dates:
        :param column_type_cash:
        :param key_cash_column:
        """
        # Naming convention for the Report you want to pull
        self.name_convention = name_convention

        # Categories for base formatting rules
        self.field_mapping = field_mapping
        self._float_columns = column_type_floats
        self._int_columns = column_type_ints
        self._date_columns = column_type_dates
        self._cash_columns = column_type_cash
        self._key_cash_column = key_cash_column  # used in conjunction with cash columns to tie out signature

        if field_mapping is None:
            self.field_mapping = {}

        self.df = df

    def format(
            self,
            df: pd.DataFrame | None = None,
            *,
            normalize_cash_columns: bool = False,
            drop_duplicated_columns: bool = False
    ) -> pd.DataFrame:
        """formats the report by updating the field names and doing elementary data quality checks on the report

        :param df: the dataframe we are parsing and wrangling
        :param normalize_cash_columns: matches all columns to the signature of the key_cash_column
        :param drop_duplicated_columns: whether to drop duplicated columns
        :return: formatted dataframe
        """
        if df is None:
            if self.df is None:
                raise ValueError(f"No dataframe reference. Must be given in the class init or parameter in function.")
            df = self.df.copy()

        # Normalizing the column names for data cleaning later in the formatting process
        working_df = df.rename(columns=self.field_mapping).copy()

        # Everything under here is about cleaning specific column types. Can be expanded later on
        if self._float_columns is not None:
            for field in self._float_columns:
                try:
                    working_df[field] = working_df[field].fillna(0.0).astype(float)
                except KeyError:
                    print(f"Field `{field}` not in dataframe columns")
                except ValueError:
                    working_df[field] = self._clean_number_column(working_df[field])
                    working_df[field] = working_df[field].astype(float).round(5)

        if self._int_columns:
            for field in self._int_columns:
                try:
                    working_df[field] = working_df[field].fillna(0).astype(int)
                except KeyError:
                    print(f"Field `{field}` not in dataframe columns")
                except ValueError:
                    working_df[field] = self._clean_number_column(working_df[field])
                    working_df[field] = working_df[field].astype(int)

        if self._date_columns:
            for field in self._date_columns:
                try:
                    working_df[field] = pd.to_datetime(working_df[field], errors='ignore')
                except KeyError:
                    print(f"Field `{field}` not in dataframe columns")

        if self._cash_columns:
            for field in self._cash_columns:
                try:
                    working_df[field] = working_df[field].fillna(0.0).astype(float)
                except KeyError:
                    print(f"Field `{field}` not in dataframe columns")

        # Special Flags for more formatting of the report
        if normalize_cash_columns:
            """
            need to add logic for normalizing the cash columns. This logic should match the signature of key column
            """
            # ==== requires the key_cash_column and cash_columns to be specified on class call ====
            # we'll check if one or both are missing
            _check_missing = []
            if self._key_cash_column is None:
                _check_missing.append("key_cash_column")
            if self._cash_columns is None:
                _check_missing.append("cash_columns")
            if _check_missing:
                raise ValueError(
                    f"{''.join(_check_missing)} missing from class call. The parameters need to be specified"
                )

            for column in self._cash_columns:
                working_df[column] = pd.Series([
                    abs(column_val) * -1 if key_val < 0 else abs(column_val)
                    for column_val, key_val in zip(working_df[column], working_df[self._key_cash_column])
                ])

        if drop_duplicated_columns:
            """Need to add the logic
            Should drop the column if duplicate name or naming convention is found in the report
            """

        self.df = working_df.copy()
        return working_df

    def flip_signature(self, signature_columns: list[str]) -> pd.DataFrame:
        """flips the signature of all the columns in the signature_column argument

        The function assumes that Report.format() has been run at least once so that
        Report._df is populated.

        This function is used any time a 'reversal' file needs to be created.
        If you need to back out the original amount, or 'zero-out' a column
        so that two DataFrames equal 0, then this function is helpful.

        :param signature_columns: will go through the list and flip the signatures in this column
        :return: DataFrame with the required fields having flipped signatures
        """
        if self.df is None:
            raise ValueError(
                "Report.format() has not been run yet. Please run the method once to initialize self._df")
        df = self.df.copy()

        for column in signature_columns:
            df[column] = df[column] * -1
        return df

    @staticmethod
    def _clean_number_column(field: pd.Series) -> pd.Series:
        """cleans up any messy number columns by removing unnecessary string characters

        :param field: pd.Series of the number column
        :return: cleaned and formatted number column
        """
        field = (
            field.fillna(0).astype(str)
            .str.lower()
            .str.replace("nan", "0")
            .str.strip("$")
            .str.strip("")
            .str.replace(",", "")
            .str.replace("none", "0")
            .str.replace("no", "0")
        )
        field = pd.Series(float(str(c).strip('%')) / 100 for c in field if '%' in c)
        return field


ReportType = TypeVar('ReportType', bound=Report)
"""Object type Report
"""
