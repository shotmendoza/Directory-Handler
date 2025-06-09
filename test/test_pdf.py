from dirlin import Folder
from dirlin.pdf import PDFFile


def test_pdf_open():
    f = Folder("/Volumes/USB321FD/Commission Statements")
    path = f / f"Connie LiuPeng Statement Commissions 2024.03.pdf"

    pdf = PDFFile(path)
    df = pdf.to_dataframe(skip_first_per_page=True)
    print(df)
