from pathlib import Path

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
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)


if __name__ == "__main__":
    db: DB = DB(db_path=":memory:")
    db.create_tables()
    print(db.metadata.tables)
