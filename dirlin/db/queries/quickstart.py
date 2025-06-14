from pydantic import BaseModel
from sqlalchemy import select, update

from dirlin.db.queries.query import Query
from dirlin.db.setup import Base, SqlSetup


class CreateOrUpdateRecord(Query):
    def __init__(self, setup: SqlSetup):
        """creates a new record in the table or updates existing record

        Assumes that tables has an `iid` column used as the primary key.
        """
        self.setup = setup

    def execute(self, table: type[Base], model: BaseModel) -> None:
        with self.setup.session.begin() as sesh:
            # (!) probably need to check whether the table has transaction_id
            query = sesh.execute(
                select(table)
                .where(table.iid == model.iid)  # type:ignore
            )
            if query.one_or_none() is None:
                new_record = table(**model.model_dump(exclude_none=True))
                sesh.add(new_record)
            else:
                sesh.execute(
                    update(table),
                    model.model_dump(exclude={"transaction_id": True}, exclude_none=True)
                )


class ReadRecordWithTransactionID(Query):
    def __init__(self, setup: SqlSetup):
        """gets an existing record from the database

        """
        self.setup = setup

    def execute(
            self,
            table: Base,
            model: BaseModel,
            transaction_id: str
    ) -> BaseModel:
        with self.setup.session.begin() as sesh:
            query = sesh.execute(
                select(table).where(table.transaction_id == transaction_id)  # type:ignore
            )
            return model.model_validate(query.scalar_one())


__all__ = ['CreateOrUpdateRecord', 'ReadRecordWithTransactionID']
