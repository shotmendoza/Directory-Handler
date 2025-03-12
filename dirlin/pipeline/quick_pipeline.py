"""This is the quick pipeline"""
import pandas as pd

from dirlin import Folder, Path
from dirlin.pipeline.data_quality.report import ReportType


class Pipeline:
    """an object that allows for quick ETL and EDA process setups"""
    def __init__(
            self,
            folder: Folder | str | None = None,
    ):
        # ==== setting up the data pull from directory ====
        if folder is None:
            folder = (Path.home() / 'Downloads')
            if not folder.exists():
                raise FileNotFoundError(f"{folder} is not a folder. Please give an argument for `folder`.")
            folder = Folder(folder)
        elif isinstance(folder, str):
            folder = Folder(folder)
            if not folder.path.exists():
                raise FileNotFoundError(f"`{folder}` does not exist. Please make sure you enter a valid path.")
        # a regular folder type would be okay - might need to add a final check to make it more robust
        self._folder = folder
        """The folder path where the Pipeline will search"""

        self.report_name_validation_pairs: dict[str: ValidationType] = dict()
        """`Report Name: Validation` key-value pairs used for running the checks and keeping the context
        of which report to run with which validation. These have a one-to-one relationship, even
        if some of the reports have a common validation set.
        
        Used when you want to run functions available in the Validation object like Error Logs.
        """

        self.report_name_results_pairs: dict[str: pd.DataFrame] = dict()
        """`Report Name: Validation Results (dataframe)` key-value pairs used for storing the results of the 
        validation checks. These also have a one-to-one relationships, where one Report is tied to a
        single results Dataframe.
        
        Used when you want to get a DataFrame with True / False and a key (if Key Column was given)
        """

        self.report_name_report_pairs: dict[str: ReportType] = dict()
        """`Report Name: Report` key-value pairs used for storing the initialized Report object.
         These also have a one-to-one relationships, where one Report is tied to a single Report object.
         
        Used when you want to used Report object functions, like reversals or the formatted report. 
        """

        self._df: pd.DataFrame | None = None
        """adds context so that the pipeline can pull again without having to rerun the function
        
        value gets updated when `get_worksheet()` parameter `keep_df_context` is set to True
        """

    def __repr__(self):
        return f"Pipeline<folder: {self._folder.path}>"

    def add_report_set(
            self,
            report: ReportType,
            validation: ValidationType,
            *,
            normalize_cash_columns: bool = False,
            drop_duplicate_columns: bool = False,
            key_column: str | None = None,
            field_mapping: dict[str, str] | None = None,
            infer_shared: bool = True,
    ):
        # ==== i) initialize the report object and run the checks if True ====
        # we currently need to do this because Report requires that the values be formalized
        if report.df is None:
            try:
                _df = self._folder.open_recent(report.name_convention)
            except FileNotFoundError:
                raise FileNotFoundError(f"No reports under `{report.name_convention}` within given timeframe.")
        else:
            if not isinstance(report.df, pd.DataFrame):
                raise ValueError(f"`report.df` must be a Pandas dataframe. Got `{type(report.df)}`")
            _df = report.df.copy()

        # ticket 11: this runs the formatting and stores it in the report.df property
        report.format(
            df=_df,
            normalize_cash_columns=normalize_cash_columns,
            drop_duplicated_columns=drop_duplicate_columns
        )

        # ==== ii) get the results, which also inits the Validation object ====
        # ticket 11: stores the formatted report with report.df
        _results = validation.run(
            df=report.df,
            key_column=key_column,
            field_mapping=field_mapping,
            infer_shared=infer_shared
        )

        # ==== iii) add to current context after init ====
        self.report_name_validation_pairs[report.name_convention] = validation
        self.report_name_results_pairs[report.name_convention] = _results

    def run_error_log(
            self,
            report_name: str | None = None,
    ):
        """runs the error log from the Validation object

        :param report_name: if given, will only return the Error Log of that report. All results as a list if None.
        :return:
        """
        if not self.report_name_validation_pairs:
            raise IndexError(
                f"No report-validation pairs were given. Please initialize with the `add_report_set` method."
            )

        # ==== run the Error Log function from the Validation Dictionary ====
        # I'm thinking I'm going to concat these, let's see how they turn out
        if report_name is None:
            _results = None
            for report, validation in self.report_name_validation_pairs.items():
                if _results is None:
                    _results = validation.generate_error_log()
                    continue
                _result = validation.generate_error_log()
                _results = pd.concat((_results, _result))
            return _results
        if report_name not in self.report_name_validation_pairs:
            raise KeyError(
                f"Report name `{report_name}` does not exist. Please make sure you enter a valid report name."
            )
        return self.report_name_validation_pairs[report_name].generate_error_log()
