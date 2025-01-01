import pytest

from dirlin.pipeline import Check, Validation
from test.test_pipelines.test_pipeline_fixtures import TestSharedParamSingleField

_test_cases = [
    TestSharedParamSingleField,
]


@pytest.mark.parametrize("validation_use_case", _test_cases)
def test_pipeline_validation_workflow(validation_use_case):
    # i) setting up the test
    current_test_case = validation_use_case()

    # ii) creating the pipeline
    check = Check(current_test_case.check_function)
    validation = Validation(check)
    df = current_test_case.create_example_df()

    # iii) Currently running it through pipeline since Pipeline is a little messed up
    result = validation.run(df, infer_shared=True)
    assert result is not None
