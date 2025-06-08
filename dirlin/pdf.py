from pathlib import Path
from typing import Literal

import pandas as pd
import pdfplumber

from dirlin.core.api import DirlinFormatter


class _PDFParseMixin(DirlinFormatter):
    """handles the complicated portion of parsing a PDF and handling tables
    """
    @classmethod
    def _handle_table_parsing(
            cls,
            file_path: Path,
            skip_first_row: Literal['page', 'pdf'] | None = None,
            table_settings: dict | None = None,
            debug_mode: bool = False,
    ) -> list[list]:
        """handles the table extraction logic for a PDF. Will attempt to parse a PDF
        with the easiest way first.
        """
        if table_settings is None:
            table_settings = {}

        with pdfplumber.open(file_path) as pdf:
            found_tables = []
            errored_pages = []  # list for keeping any pages that didn't pull correctly
            for idx, page in enumerate(pdf.pages):
                y = page.extract_text()  # check to see if we were able to handle this PDF
                if y == "" and debug_mode is True:
                    debug_path = file_path.parent / f"debug_table{idx}.png"
                    table_image = page.to_image().debug_tablefinder()  # Visualize tables
                    table_image.save(debug_path)

                    # todo running find_and_combine on pdfplumber is not thread safe so we'll need another fn to handle
                temp_tbl = page.extract_table(table_settings)  # gives me a list of records I can parse through

                # first check, we're checking again for pdf level once all tables are found
                if skip_first_row == "page":
                    temp_tbl = temp_tbl[1:]
                found_tables.extend(temp_tbl)

        # second check in case they want to only skip it on the PDF level
        if skip_first_row == "pdf":
            found_tables = found_tables[1:]
        print(found_tables)
        return found_tables


class PDFHandler(_PDFParseMixin):
    """handles PDFs and parsing it into Dataframes to start.
    """
    @classmethod
    def read(
            cls,
            file_path: Path,
            field_names: list[str] | None = None,
            skip_first_row: Literal['page', 'pdf'] | None = None,
            table_settings: dict | None = None,
            remove_repeated_keywords: str | None = None,
            add_source: bool = True,
    ):
        """converts a PDF file into a Dataframe

        ...

        # Below are some notes on some of the use-cases of the parameters in this function.

        `skip_first_row` is useful when there's a mini-table in the table you parsed:
            - in > [["Bad Field", None, None], ["Column A", "Column B", "Column C"], [1, 2, 3]]
            - out > [["Column A", "Column B", "Column C"], [1, 2, 3]]

        ...

        `remove_repeated_keywords` can be used when there's a persistent keyword you want to remove:
            - in > [["Total", ...], ..., ["Total", ...], ..., ["Total", ...]]
            - out > [...]

        ...

        `add_source` adding a column to the dataframe
            - in > df.columns > ["foo", "bar", "foobar", ...]
            - out > df.columns > ["foo", "bar", "foobar", ... , "source"]

        ...

        :param file_path: path to the PDF file
        :param field_names: column names you want to process the dataframe with
        :param table_settings: for `pdfplumber` settings for parsing the PDF table
        :param remove_repeated_keywords: determines whether to remove a row based on a keyword
        :param skip_first_row: determines whether to skip the first row
        :param add_source: determines whether to add the `source` field when opening the file

        """
        # ===Part 1=== Parse the PDF
        records = cls._handle_table_parsing(file_path, skip_first_row, table_settings)

        # ===Part 2=== Format the Dataframe
        # If Field Name is None, we automatically try to parse it
        if field_names is None:
            try:
                # Attempts to take the first row as the column names
                field_names = [
                    cls.convert_string_to_python_readable(f, True) for f in records[0]
                ]
                del records[0]
            except TypeError as te:
                # the function in the try block will raise a type error if None comes in as the name
                # similar to attribute error, this happens when the PDF gets parsed as an empty table
                print(f"Table unable to be pulled from PDF ({file_path.stem}) (TE)")
                print(f"Hint: try using `skip_the_first_row` argument (`page`, `pdf`).")
                print(f"{te}")
                return pd.DataFrame()
            except AttributeError as ae:
                # This happens when the PDF table that gets parsed is empty.
                # I think we're going to return an empty DataFrame so columns don't get affected.
                print(f"Table unable to be pulled from PDF ({file_path.stem}) (AE): {ae}")
                return pd.DataFrame()
        df = pd.DataFrame(records, columns=field_names)
        df = cls.clean_table(df, remove_keyword_records=remove_repeated_keywords)

        # Adds Source at the end
        if add_source is True:
            df["source"] = file_path.stem
        return df
