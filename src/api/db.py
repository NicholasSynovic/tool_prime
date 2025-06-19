"""
DB executor and interface.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from pathlib import Path

import pandas as pd
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
    """
    DB executor and interface.

    Provides functionality to interact with a SQLite database using SQLAlchemy.
    This class manages the creation of tables, and facilitates reading from and
    writing to the database using pandas DataFrames. It ensures data integrity
    through validation against Pydantic models.

    Attributes:
        dbPath (Path): The file path to the SQLite database.
        engine (Engine): SQLAlchemy engine for database connection and operations.
        metadata (MetaData): SQLAlchemy MetaData object for schema management.

    Methods:
        create_tables(): Defines and creates tables for storing commit-related
            information.
        write_df(df, table, model): Writes a DataFrame to a SQL table with validation.
        read_table(table, model): Reads data from a SQL table into a DataFrame
            with validation.

    """

    def __init__(self, db_path: Path) -> None:
        """
        Initialize the database connection and metadata.

        Sets up a connection to a SQLite database using SQLAlchemy, with the database
        path provided during instantiation. Initializes the metadata object for managing
        the database schema and calls the method to create tables.

        Args:
            db_path (Path): The file path to the SQLite database.

        Attributes:
            dbPath (Path): Stores the path to the SQLite database.
            engine (Engine): SQLAlchemy engine for database connection and operations.
            metadata (MetaData): SQLAlchemy MetaData object for schema management.

        """
        self.dbPath: Path = db_path.resolve()
        self.engine: Engine = create_engine(url=f"sqlite:///{self.dbPath}")
        self.metadata: MetaData = MetaData()

        self.create_tables()

    def create_tables(self) -> None:
        """
        Create database tables for storing commit-related information.

        Defines the schema for several tables using SQLAlchemy's `Table` and `Column`
        constructs. These tables are designed to store metadata related to Git commits,
        including commit hashes, releases, authors, committers, commit logs, and file
        size information. Foreign key constraints are used to establish relationships
        between tables, ensuring referential integrity.

        """
        _: Table = Table(
            "commit_hashes",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("commit_hash", String),
        )

        _: Table = Table(
            "releases",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("commit_hash_id", String),
            ForeignKeyConstraint(
                ["commit_hash_id"],
                ["commit_hashes.id"],
            ),
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

        _: Table = Table(
            "file_size_per_commit",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("commit_hash_id", Integer),
            Column("language", String),
            Column("provider", String),
            Column("lines", Integer),
            Column("code", Integer),
            Column("comments", Integer),
            Column("blanks", Integer),
            Column("bytes", Integer),
            ForeignKeyConstraint(
                ["commit_hash_id"],
                ["commit_hashes.id"],
            ),
        )

        _: Table = Table(
            "issue_ids",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("issue_id", String),
        )

        _: Table = Table(
            "issues",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("issue_id_key", Integer),
            Column("created_at", DateTime),
            Column("closed_at", DateTime),
            ForeignKeyConstraint(
                ["issue_id_key"],
                ["issue_ids.id"],
            ),
        )

        _: Table = Table(
            "pull_request_ids",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("pull_request_id", String),
        )

        _: Table = Table(
            "pull_requests",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("pull_request_id_key", Integer),
            Column("created_at", DateTime),
            Column("closed_at", DateTime),
            ForeignKeyConstraint(
                ["pull_request_id_key"],
                ["pull_request_ids.id"],
            ),
        )

        _: Table = Table(
            "project_productivity_per_commit",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("commit_hash_id", Integer),
            Column("delta_lines", Integer),
            Column("delta_code", Integer),
            Column("delta_comments", Integer),
            Column("delta_blanks", Integer),
            Column("delta_bytes", Integer),
            ForeignKeyConstraint(
                ["commit_hash_id"],
                ["commit_hashes.id"],
            ),
        )

        _: Table = Table(
            "project_productivity_per_day",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("date", DateTime),
            Column("delta_lines", Integer),
            Column("delta_code", Integer),
            Column("delta_comments", Integer),
            Column("delta_blanks", Integer),
            Column("delta_bytes", Integer),
        )

        _: Table = Table(
            "project_size_per_commit",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("commit_hash_id", Integer),
            Column("lines", Integer),
            Column("code", Integer),
            Column("comments", Integer),
            Column("blanks", Integer),
            Column("bytes", Integer),
            ForeignKeyConstraint(
                ["commit_hash_id"],
                ["commit_hashes.id"],
            ),
        )

        _: Table = Table(
            "project_size_per_day",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("date", DateTime),
            Column("lines", Integer),
            Column("code", Integer),
            Column("comments", Integer),
            Column("blanks", Integer),
            Column("bytes", Integer),
        )

        self.metadata.create_all(bind=self.engine, checkfirst=True)

    def write_df(self, df: DataFrame, table: str, model: type[BaseModel]) -> bool:
        """
        Write a DataFrame to a SQL table with validation against a specified model.

        Validates the provided DataFrame against a given Pydantic `BaseModel` to ensure
        data integrity and consistency. If validation passes, the DataFrame is written
        to the specified SQL table using the database engine associated with the
        instance. The data is appended to the table, and an index column labeled
        'id' is included. If a database integrity error occurs during the write
        operation, the function returns `False`.

        Args:
            df (DataFrame): The pandas DataFrame to be written to the SQL table.
            table (str): The name of the SQL table where the DataFrame will be appended.
            model (type[BaseModel]): A Pydantic model class used to validate
                the DataFrame before writing to the database.

        Returns:
            bool: `True` if the DataFrame is successfully written to the SQL table,
                `False` if an integrity error occurs during the write operation.

        """
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

    def read_table(self, table: str, model: type[BaseModel]) -> DataFrame:
        """
        Read data from a SQL table into a DataFrame with validation.

        Retrieves data from the specified SQL table using the instance's database
        engine, loading it into a pandas DataFrame with the 'id' column set as
        the index. The DataFrame is then validated against a given Pydantic
        `BaseModel` to ensure data integrity and consistency before returning.

        Args:
            table (str): The name of the SQL table to read data from.
            model (type[BaseModel]): A Pydantic model class used to validate the
                DataFrame after reading from the database.

        Returns:
            DataFrame: A pandas DataFrame containing the data from the SQL table,
                validated against the specified model.

        """
        df: DataFrame = pd.read_sql_table(
            table_name=table,
            con=self.engine,
            index_col="id",
        )

        validate_df(model=model, df=df)

        return df

    def query_database(self, sql: str, primary_key: str = "id") -> DataFrame:
        """
        Execute an SQL query on the connected database and return a DataFrame.

        This function uses the provided SQL query string to fetch data from the
        database using the established connection engine. The resulting
        DataFrame is indexed by the specified primary key column.

        Args:
            sql (str): The SQL query string to be executed on the database.
            primary_key (str, optional): The column name to be used as the index
                for the resulting DataFrame. Defaults to "id".

        Returns:
            DataFrame: A pandas DataFrame containing the result of the SQL
                query, indexed by the specified primary key column.

        """
        return pd.read_sql(sql=sql, con=self.engine, index_col=primary_key)
