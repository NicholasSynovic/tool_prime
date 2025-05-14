from pathlib import Path

import pandas
from pandas import DataFrame
from pydantic import BaseModel
from sqlalchemy import (
    Column,
    DateTime,
    Engine,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.exc import IntegrityError

from src.api.types import validate_df


class DB:
    def __init__(self, db_path: Path):
        self.dbPath: Path = db_path
        self.engine: Engine = create_engine(url=f"sqlite:///{self.dbPath}")
        self.metadata: MetaData = MetaData()

        self.create_tables()

    def create_tables(self) -> None:
        _: Table = Table(
            "commit_hashes",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("commit_hash", String),
        )

        _: Table = Table(
            "authors",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("author", String),
            Column("author_email", String),
        )

        _: Table = Table(
            "committers",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("committer", String),
            Column("committer_email", String),
        )

        _: Table = Table(
            "commit_logs",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("commit_hash_id", Integer),
            Column("author_id", Integer),
            Column("committer_id", Integer),
            Column("co_author_ids", String),
            Column("parent_hash_ids", String),
            Column("authored_datetime", DateTime),
            Column("committed_datetime", DateTime),
            Column("encoding", String),
            Column("message", String),
            Column("gpgsign", String),
            ForeignKeyConstraint(
                ["commit_hash_id"],
                ["commit_hashes.id"],
            ),
            ForeignKeyConstraint(["author_id"], ["authors.id"]),
            ForeignKeyConstraint(["committer_id"], ["committers.id"]),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def write_df(self, df: DataFrame, table: str, model: BaseModel) -> bool:
        validate_df(model=model, df=df)

        try:
            df.to_sql(
                name=table,
                con=self.engine,
                if_exists="append",
                index=True,
                index_label="id",
            )
        except IntegrityError:
            return False

        return True

    def read_table(self, table: str, model: BaseModel) -> DataFrame:
        df: DataFrame = pandas.read_sql_table(
            table_name=table,
            con=self.engine,
            index_col="id",
        )

        validate_df(model=model, df=df)

        return df


if __name__ == "__main__":
    db: DB = DB(db_path=":memory:")
    db.create_tables()
    print(db.metadata.tables)
