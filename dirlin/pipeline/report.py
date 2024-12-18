import pandas as pd


class Report:
    def __init__(
            self,
            name_convention: str,
            field_mapping: dict[str, str],
            *,
            column_type_floats: list[str] | None = None,
            column_type_ints: list[str] | None = None,
            column_type_dates: list[str] | None = None,
            column_type_cash: list[str] | None = None,
            key_cash_column: str | None = None,
    ):
        # Naming convention for the Report you want to pull
        self.name_convention = name_convention

        # Categories for base formatting rules
        self.field_mapping = field_mapping
        self._float_columns = column_type_floats
        self._int_columns = column_type_ints
        self._date_columns = column_type_dates
        self._cash_columns = column_type_cash
        self._key_cash_column = key_cash_column  # used in conjunction with cash columns to tie out signature

        self._df: pd.DataFrame | None = None

    def format(
            self,
            df: pd.DataFrame,
            *,
            normalize_cash_columns: bool = False,
            drop_duplicated_columns: bool = False
    ) -> pd.DataFrame:

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
            ...

        if drop_duplicated_columns:
            """Need to add the logic
            Should drop the column if duplicate name or naming convention is found in the report
            """
            ...

        if not self._df:
            self._df = working_df.copy()
        return working_df

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
