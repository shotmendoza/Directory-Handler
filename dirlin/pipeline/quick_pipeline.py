"""This is the quick pipeline"""
import pandas as pd

from dirlin import Folder
from dirlin.pipeline.report import ReportType


class Pipeline:
    def __init__(
            self,
            folder: Folder | str
    ):
        """an object that allows for quick ETL and EDA process setups

        """
        # Should add more high level checks to these
        self._folder = folder
        if isinstance(folder, str):
            self._folder = Folder(folder)

        # Adding some context on the report that have been run in the pipeline
        # Updates when get_worksheet() gets updated and run
        self._df: pd.DataFrame | None = None
        """adds context so that the pipeline can pull again without having to rerun the function
        
        value gets updated when `get_worksheet()` parameter `keep_df_context` is set to True
        """

        self._report: ReportType | None = None
        """keeps the context of the last Report that went through the pipeline
        
        value gets updated when `get_worksheet()` parameter `keep_report_context` is set to True
        """

    def __repr__(self):
        return f"Pipeline<folder: {self._folder.path}>"

    def get_worksheet(
            self,
            worksheet_name: str,
            *,
            report: ReportType | None = None,
            keep_report_context: bool = True,
            keep_df_context: bool = True
    ) -> pd.DataFrame:
        """creates a dataframe with the file that follows the `worksheet_name` naming convention

        The function searches for the file in the `self.folder`, where you set on the class instantiation.
        Feel free to update the parameters to make it more customizable in the future, like making
        use of the recursion.

        Formats the report based on the `format()` function inside TypeReport. If no `report` argument
        is given, then the function will keep the raw format

        Parameters:
            :param worksheet_name: naming convention of the worksheet you want to pull into the pipeline

            :param report: (TypeReport) has the report information and metadata for formatting

            :param keep_report_context: (bool) set to True to save the `report` argument in the object

            :param keep_df_context: (bool) set to True to save the `df` argument in the object
        """
        # Probably needs wrapping in the future
        df = self._folder.open_recent(worksheet_name)
        if self._df:
            df = self._df.copy()

        # Handling if report argument was given
        if report:
            df = report.format(df)
            if keep_report_context:
                self._report = report

        # Handling the context if keep_df_context is set
        if keep_df_context:
            self._df = df

        return df
