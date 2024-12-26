"""This is the quick pipeline"""
import pandas as pd

from dirlin import Folder
from dirlin.pipeline.data_quality.base_validation import ValidationType
from dirlin.pipeline.report import ReportType


class Pipeline:
    def __init__(
            self,
            folder: Folder | str,
            *,
            report: ReportType | None = None,
            validation: ValidationType | None = None,
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

        self._report: ReportType | None = report
        """keeps the context of the last Report that went through the pipeline
        
        value gets updated when `get_worksheet()` parameter `keep_report_context` is set to True
        """

        self._validation: ValidationType | None = validation
        """keeps context of the last Validation that went through the pipeline
        
        """

    def __repr__(self):
        return f"Pipeline<folder: {self._folder.path}>"

    def get_worksheet(
            self,
            worksheet_name: str | None = None,
            df: pd.DataFrame | None = None,
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

            :param df: an optional dataframe if you want to give the pipeline a DF without using keywords

            :param report: (TypeReport) has the report information and metadata for formatting

            :param keep_report_context: (bool) set to True to save the `report` argument in the object

            :param keep_df_context: (bool) set to True to save the `df` argument in the object
        """
        if worksheet_name is None:
            if df is not None:
                df = df.copy()
            elif report is not None:
                df = self._folder.open_recent(report.name_convention)
            elif self._report is not None:
                df = self._folder.open_recent(self._report.name_convention)
            elif self._df is not None:
                df = self._df.copy()
            else:
                raise ValueError("Need a worksheet_name if report not given on Pipeline init.")
        else:  # worksheet_name is given
            df = self._folder.open_recent(worksheet_name)

        # Handling if report argument was given
        if report is not None:  # report arg is given
            if keep_report_context:
                self._report = report
        else:  # report is arg not given
            if self._report is not None:  # previous context was kept
                report = self._report

        if report is not None:
            df = report.format(df)

        # Handling the context if keep_df_context is set
        if keep_df_context:
            self._df = df
        return df

    def run_validation(
            self,
            validation: ValidationType,
    ):
        try:
            df = self._df.copy()
        except AttributeError:
            if self._report is None:
                raise AttributeError(
                    f"Will need to run Pipeline.get_worksheet() or "
                    f"use a Report as an argument in Pipeline init."
                )
            # Rerunning with report context
            df = self.get_worksheet(
                self._report.name_convention,
                report=self._report,
                keep_df_context=False,
                keep_report_context=True
            )
        return validation.run(df)
