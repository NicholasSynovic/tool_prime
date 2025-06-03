from datetime import datetime

from pandas import DataFrame, Series
from pydantic import BaseModel, Field, ValidationError


def validate_df(model: BaseModel, df: DataFrame) -> None:
    def _run(data: Series) -> None:
        row: dict = data.to_dict()
        try:
            model(**row)
        except ValidationError as ve:
            raise ve

    df.apply(_run, axis=1)


class CommitHashes(BaseModel):
    commit_hash: str = Field(default=..., description="Commit hash")


class Releases(BaseModel):
    commit_hash_id: int = Field(default=..., description="Revision hash")


class Authors(BaseModel):
    author: str = Field(default=..., description="Author name")
    author_email: str = Field(default=..., description="Author email")


class Committers(BaseModel):
    committer: str = Field(default=..., description="Committer name")
    committer_email: str = Field(
        default=...,
        description="Committer email",
    )


class CommitLog(BaseModel):
    commit_hash_id: int = Field(
        default=...,
        description="Index value of related commit hash",
    )
    author_id: int = Field(
        default=...,
        description="Index value of related author",
    )
    committer_id: int = Field(
        default=...,
        description="Index value of related committer",
    )
    co_author_ids: str = Field(
        default=...,
        description="JSON stringified index values of co-authors",
    )
    parent_hash_ids: str = Field(
        default=...,
        description="JSON stringified index values of parent hashes",
    )
    authored_datetime: datetime = Field(
        default=...,
        description="UTC datetime of when the commit was authored",
    )
    committed_datetime: datetime = Field(
        default=...,
        description="UTC datetime of when the commit was committed",
    )
    encoding: str = Field(default=..., description="Message encoding")
    message: str = Field(default=..., description="Commit message")
    gpgsign: str = Field(default=..., description="GPG signature")


class Size(BaseModel):
    language: str = Field(default=..., description="Identified programming language")
    filename: str = Field(
        default=..., description="Name of the file; not the full path"
    )
    lines: int = Field(default=..., description="Number of lines in the file")
    code: int = Field(default=..., description="Number of lines of code")
    comments: int = Field(default=..., description="Number of lines of comments")
    blanks: int = Field(default=..., description="Number of blank lines")
    bytes: int = Field(default=..., description="Number of bytes")
    commit_hash_id: int = Field(default=..., description="Commit hash ID from database")
