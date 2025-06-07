from datetime import date

import pandas as pd

from dirlin import Folder, Path
from dirlin.pdf import PDFHandler


def test_problem_pdf():
    """PDF 2024/06 has been giving me some issues so this is testing that
    """
    path = Path("/Volumes/USB321FD/Commission Statements/Connie LiuPeng Statement Commissions 2024.06.pdf")
    pdf = PDFHandler()

    df = pdf.read(
        file_path=path,
        skip_first_row='page'
    )
    print(df)


def test_mass_writing_pdf():
    folder = Folder("/Volumes/USB321FD/Commission Statements")
    files = folder.index_files(".pdf")

    pdf = PDFHandler()

    results = []
    for f in files:
        temp = pdf.read(f, skip_first_row="page", remove_repeated_keywords="Total Commissions")
        results.append(temp)
    df = pd.concat(results)
    df.insert(len(df.columns) - 1, "source", df.pop("source"))
    print(df.info())
    df.to_csv(folder / f"Connie LiuPeng Statement Commissions - All - {date.today()}.csv")
