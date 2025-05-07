"""Dirlin (Directory Handler), set of tools to help process local files."""

__version__ = "0.3.6"

from .src.base.folder import (
    Folder,
    Document,
    Directory,
    Path
)

from .src.base.validation import BaseValidation
from .src.base.util import DirlinFormatter
