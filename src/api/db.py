from pathlib import Path

import pandas
from pandas import DataFrame
from sqlalchemy import (
    Column,
    Engine,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)


class DB:
    def __init__(self, db_path: Path):
        self.dbPath: Path = db_path
        self.engine: Engine = create_engine(url=f"sqlite:///{self.dbPath}")
        self.metadata: MetaData = MetaData()

    def create_tables(self) -> None:
        _: Table = Table(
            "commit_hashes",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("commit_hash", String, nullable=False),
        )

        _: Table = Table(
            "authors",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("author", String, nullable=False),
            Column("author_email", String, nullable=False),
        )

        _: Table = Table(
            "committers",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("committer", String, nullable=False),
            Column("committer_email", String, nullable=False),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def write_df(self, df: DataFrame, table: str) -> None:
        df.to_sql(
            name=table,
            con=self.engine,
            if_exists="append",
            index=True,
            index_label="id",
        )

    def read_table(self, table: str) -> DataFrame:
        return pandas.read_sql_table(
            table_name=table,
            con=self.engine,
            index_col="id",
        )


if __name__ == "__main__":
    db: DB = DB(db_path=":memory:")
    db.create_tables()
    print(db.metadata.tables)
