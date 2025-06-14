from abc import ABC
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from sqlalchemy import Engine, create_engine

from dirlin.db.utils import database_exists


class Base(DeclarativeBase):
    """used for creating different SqlAlchemy tables. Tables will usually inherit from this class.

    Example:
        class SampleTable(Base):
            __tablename__ = "bank_statements"
            id: Mapped[int] = mapped_column(primary_key=True)
            transaction_id: Mapped[str] = mapped_column(unique=True)
    """
    ...


@dataclass
class SqlSetup(ABC):
    """used for metadata and setting up tables and databases in the app

    Attributes:
        url: the database URL
        engine: the database engine generated from create_engine()
        session: the session context used to communicate with the database

    """
    url: str
    Base: DeclarativeBase
    engine: Engine | None = None
    session: Session | sessionmaker | None = None

    def __post_init__(self):
        if self.engine is None:
            self.engine = self.generate_engine()

        if self.session is None:
            self.session = self.create_session_factory()

    def generate_engine(self) -> Engine:
        """creates an engine based off the init `url` property.
        """
        return create_engine(self.url, echo=True)

    def create_session_factory(self):
        """generates a session factory for the database
        """
        return sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    @classmethod
    def _resolve_db_path(cls):
        """will be made cleaner in the future, but use this function
        to get the DB URL from wherever you are calling it
        """
        url = _BASE_DIR = Path(__file__).resolve().parent.parent.parent
        print(url)
        return url

    def create_if_not_exist(self) -> None:
        if not database_exists(self.url):
            self.Base.metadata.create_all(self.engine)
