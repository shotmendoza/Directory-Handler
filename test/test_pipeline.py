import pandas as pd
import pytest

from dirlin import Folder
from dirlin.pipeline import Pipeline, Report, Check, Validation


class TestPipeLine:
    f = Folder(folder_path="/Volumes/Sho's SSD/trading")

    report_for_formatting = Report(
        name_convention="ohlcv",
        field_mapping={"High": "high", "Low": "low"},
        column_type_cash=["high", "low"]
    )

    def _series_type_check_function(self, low: pd.Series, high: pd.Series) -> pd.Series:
        return pd.Series(low < high)

    new_check = Check(_series_type_check_function)
    validation = Validation(new_check)

    # df = qp.get_worksheet(report_for_formatting.name_convention, report=report_for_formatting)

    def test_pipeline_init(self):
        qp = Pipeline(self.f)

    def test_pipeline_run(self):
        qp = Pipeline(self.f)
        df = qp.get_worksheet("balance_sheet")
        assert isinstance(df, pd.DataFrame)

    def test_pipeline_run_with_reports(self):
        qp = Pipeline(self.f)
        r = Report(
            name_convention="ohlcv",
            field_mapping={},
            column_type_cash=["open"]
        )
        df = qp.get_worksheet(r.name_convention, report=r)
        assert isinstance(df, pd.DataFrame)

    def test_pipeline_run_with_validation(self):
        qp = Pipeline(self.f, report=self.report_for_formatting)
        qp.run_validation(self.validation)

    def test_pipeline_run_without_report_error(self):
        with pytest.raises(AttributeError) as ae:
            qp = Pipeline(self.f)
            qp.run_validation(validation=self.validation)
        assert (
                str(ae.value) == "Will need to run Pipeline.get_worksheet() "
                                 "or use a Report as an argument in Pipeline init."
        )


class TestCheckProcess:
    f = Folder(folder_path="/Volumes/Sho's SSD/trading")
    qp = Pipeline(f)

    report_for_formatting = Report(
        name_convention="ohlcv",
        field_mapping={"High": "high", "Low": "low"},
        column_type_cash=["high", "low"]
    )

    df = qp.get_worksheet(report_for_formatting.name_convention, report=report_for_formatting)

    def _series_type_check_function(self, low: pd.Series, high: pd.Series) -> pd.Series:
        return pd.Series(low < high)

    def non_series_check_function(self, low: float, high: float) -> bool:
        if high <= low:
            return False
        return True

    def test_create_check(self):
        new_check = Check(self._series_type_check_function)  # this is a check you would want to add
        result = new_check.validate(self.df)
        assert isinstance(result, pd.Series)

        non_series_check = Check(self.non_series_check_function)
        result = non_series_check.validate(self.df)
        assert isinstance(result, list)

    def test_validation_run(self):
        new_check = Check(self._series_type_check_function)
        validation = Validation(new_check)
        result = validation.run(self.df)
        print(result.info())
        print(result)
