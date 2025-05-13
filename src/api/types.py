from datetime import datetime

from pandas import DataFrame, Series
from pydantic import BaseModel, Field, ValidationError


def validate_df(model: BaseModel, df: DataFrame) -> None:
    def _run(data: Series) -> None:
        try:
            model(**data.to_dict())
        except ValidationError as ve:
            raise ve

    df.apply(_run, axis=1)


class CommitHashes(BaseModel):
    commit_hash: str = Field(default=..., description="Commit hash")


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
