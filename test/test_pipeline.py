import pandas as pd

from dirlin import Folder
from dirlin.pipeline import Pipeline, Report


class TestPipeLine:
    f = Folder(folder_path="/Volumes/Sho's SSD/trading")

    def test_pipeline_init(self):
        qp = Pipeline(self.f)
        print(qp)

    def test_pipeline_run(self):
        qp = Pipeline(self.f)
        df = qp.get_worksheet("balance_sheet")

        assert isinstance(df, pd.DataFrame)
        print(df)

    def test_pipeline_run_with_reports(self):
        qp = Pipeline(self.f)
        r = Report(
            name_convention="ohlcv",
            field_mapping={},
            column_type_cash=["open"]
        )
        df = qp.get_worksheet(r.name_convention, report=r)
        assert isinstance(df, pd.DataFrame)
        print(df)
