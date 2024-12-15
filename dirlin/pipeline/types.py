from typing import TypeVar

from dirlin.pipeline import Report, Validation

ReportType = TypeVar('ReportType', bound=Report)
"""Object type Report
"""

ValidationType = TypeVar('ValidationType', bound=Validation)
"""Object type Validation
"""
