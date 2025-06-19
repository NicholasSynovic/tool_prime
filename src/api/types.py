"""
Type checking for DataFrames.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from datetime import datetime

from pandas import DataFrame, Series
from pydantic import BaseModel, Field, ValidationError


class CommitHashes(BaseModel):
    """
    Represents a commit hash of a specific commit.

    This model captures the commit hash between a release and the corresponding
    commit in the version control history.

    Attributes:
        commit_hash (str): Commit hash associated with a commit.

    """

    commit_hash: str = Field(default=..., description="Commit hash")


class Releases(BaseModel):
    """
    Represents a release entry linked to a specific commit.

    This model captures the relationship between a release and the corresponding
    commit hash in the version control history.

    Attributes:
        commit_hash_id (int): ID referencing the commit hash associated with the
            release.

    """

    commit_hash_id: int = Field(default=..., description="Revision hash")


class Authors(BaseModel):
    """
    Represents an author entity with name and email information.

    This model is used to store and validate data related to individuals
    who have authored commits in a version control system.

    Attributes:
        author (str): Name of the author.
        author_email (str): Email address of the author.

    """

    author: str = Field(default=..., description="Author name")
    author_email: str = Field(default=..., description="Author email")


class Committers(BaseModel):
    """
    Represents a committer entity with name and email information.

    This model is used to store and validate data related to individuals
    who have committed changes in a version control system.

    Attributes:
        committer (str): Name of the committer.
        committer_email (str): Email address of the committer.

    """

    committer: str = Field(default=..., description="Committer name")
    committer_email: str = Field(
        default=...,
        description="Committer email",
    )


class CommitLog(BaseModel):
    """
    Data model representing metadata for a single commit in a version control system.

    This model captures information about commit authorship, message content,
    associated signatures, and relationships to other commits or contributors.
    It is designed to support version control history analysis, contributor
    tracking, and auditing.

    Attributes:
        commit_hash_id (int): Index value referencing the related commit hash in
            the database.
        author_id (int): Index value referencing the author of the commit.
        committer_id (int): Index value referencing the committer of the commit.
        co_author_ids (str): JSON-encoded list of index values representing co-authors.
        parent_hash_ids (str): JSON-encoded list of index values representing parent
            commits.
        authored_datetime (datetime): UTC timestamp of when the commit was authored.
        committed_datetime (datetime): UTC timestamp of when the commit was committed.
        encoding (str): Character encoding used for the commit message.
        message (str): Full commit message as recorded in the version control system.
        gpgsign (str): GPG signature associated with the commit, if available.

    """

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


class FileSize(BaseModel):
    language: str = Field(
        default=..., description="Identified programming language of the file"
    )
    provider: str = Field(default=..., description="Absolute filepath")
    lines: int = Field(default=..., description="Number of lines in the file")
    code: int = Field(default=..., description="Number of lines of code")
    comments: int = Field(default=..., description="Number of lines of comments")
    blanks: int = Field(default=..., description="Number of blank lines")
    bytes: int = Field(default=..., description="Number of bytes")
    commit_hash_id: int = Field(default=..., description="Commit hash ID from database")


class IssueIDs(BaseModel):
    """
    Pydantic model representing a GitHub issue ID.

    Attributes:
        issue_id (str): Unique identifier of the issue as returned by the GitHub
            GraphQL API.

    """

    issue_id: str = Field(default=..., description="Issue ID")


class Issues(BaseModel):
    """
    Pydantic model representing metadata for a GitHub issue.

    Attributes:
        issue_id_key (int): Index value of the related issue in the database.
        created_at (datetime): Datetime when the issue was created.
        closed_at (datetime): Datetime when the issue was closed.

    """

    issue_id_key: int = Field(
        default=...,
        description="Index value of related issue",
    )
    created_at: datetime = Field(
        default=..., description="Datetime when an issue was created"
    )
    closed_at: datetime = Field(
        default=..., description="Datetime when an issue was closed"
    )


class PullRequestIDs(BaseModel):
    """
    Pydantic model representing a GitHub pull request ID.

    Attributes:
        pull_request_id (str): Unique identifier of the pull request as returned
        by the GitHub GraphQL API.

    """

    pull_request_id: str = Field(default=..., description="Pull request ID")


class PullRequests(BaseModel):
    """
    Pydantic model representing metadata for a GitHub pull request.

    Attributes:
        pull_request_id_key (int): Index value of the related pull request in
            the database.
        created_at (datetime): Datetime when the pull request was created.
        closed_at (datetime): Datetime when the pull request was closed.

    """

    pull_request_id_key: int = Field(
        default=...,
        description="Index value of related pull request",
    )
    created_at: datetime = Field(
        default=..., description="Datetime when an pull request was created"
    )
    closed_at: datetime = Field(
        default=..., description="Datetime when an pull request was closed"
    )


class ProjectSize(BaseModel):
    """
    A Pydantic model representing the size metrics of a project at a commit.

    Each attribute is initialized with a default value, which must be provided
    when creating an instance of the model.

    Attributes:
        commit_hash_id (int): The unique identifier for the commit hash from the
            database.
        lines (int): The total number of lines in the project, including code,
            comments, and blanks.
        code (int): The total number of lines that contain code.
        comments (int): The total number of lines that contain comments.
        blanks (int): The total number of blank lines.
        bytes (int): The total number of bytes used by the project files.

    """

    commit_hash_id: int = Field(default=..., description="Commit hash ID from database")
    lines: int = Field(default=..., description="Total number of lines")
    code: int = Field(default=..., description="Total number of code lines")
    comments: int = Field(default=..., description="Total number of comment lines")
    blanks: int = Field(default=..., description="Total number of blank lines")
    bytes: int = Field(default=..., description="Total number of bytes")


class DailyProjectSize(BaseModel):
    """
    Data model representing the daily size metrics of a project.

    This class is used to encapsulate various metrics related to the size of a
    project on a specific date. It includes fields for the total number of lines,
    code lines, comment lines, blank lines, and the total number of bytes. The
    model is designed to be used with data validation libraries like Pydantic,
    providing type safety and automatic validation.

    Attributes:
        date (datetime): The date on which the size measurement was taken.
        lines (int): The total number of lines in the project on the given date.
        code (int): The total number of lines that contain executable code.
        comments (int): The total number of lines that are comments.
        blanks (int): The total number of blank lines in the project.
        bytes (int): The total number of bytes occupied by the project files.

    """

    date: datetime = Field(default=..., description="Date of measurement")
    lines: int = Field(default=..., description="Total number of lines")
    code: int = Field(default=..., description="Total number of code lines")
    comments: int = Field(default=..., description="Total number of comment lines")
    blanks: int = Field(default=..., description="Total number of blank lines")
    bytes: int = Field(default=..., description="Total number of bytes")


class ProjectProductivity(BaseModel):
    """
    A Pydantic model representing the productivity metrics of a project.

    Each attribute is initialized with a default value, which must be provided
    when creating an instance of the model. This model is useful for tracking
    the differences in project size metrics between consecutive commits.

    Attributes:
        commit_hash_id (int): The unique identifier for the commit hash from the
            database.
        delta_lines (int): The change in the total number of lines in the
            project, including code, comments, and blanks.
        delta_code (int): The change in the total number of lines that contain
            code.
        delta_comments (int): The change in the total number of lines that
            contain comments.
        delta_blanks (int): The change in the total number of blank lines.
        delta_bytes (int): The change in the total number of bytes used by the
            project files.

    """

    commit_hash_id: int = Field(default=..., description="Commit hash ID from database")
    delta_lines: int = Field(default=..., description="Change in total number of lines")
    delta_code: int = Field(
        default=..., description="Change in total number of code lines"
    )
    delta_comments: int = Field(
        default=..., description="Change in total number of comment lines"
    )
    delta_blanks: int = Field(
        default=..., description="Change in total number of blank lines"
    )
    delta_bytes: int = Field(default=..., description="Change in total number of bytes")


class DailyProjectProductivity(BaseModel):
    """
    A data model representing daily changes in project productivity metrics.

    This class is used to encapsulate the changes in various size metrics of a
    project on a specific date. It includes fields for the changes in the total
    number of lines, code lines, comment lines, blank lines, and bytes. The
    model is designed to be used with data validation libraries like Pydantic,
    providing type safety and automatic validation.

    Each attribute is required and is accompanied by a description that provides
    additional context for its purpose and usage.

    Attributes:
        date (datetime): The date on which the productivity changes were
            measured.
        delta_lines (int): The change in the total number of lines compared to
            the previous measurement.
        delta_code (int): The change in the total number of code lines compared
            to the previous measurement.
        delta_comments (int): The change in the total number of comment lines
            compared to the previous measurement.
        delta_blanks (int): The change in the total number of blank lines
            compared to the previous measurement.
        delta_bytes (int): The change in the total number of bytes compared to
            the previous measurement.

    """

    date: datetime = Field(default=..., description="Date of measurement")
    delta_lines: int = Field(default=..., description="Change in total number of lines")
    delta_code: int = Field(
        default=..., description="Change in total number of code lines"
    )
    delta_comments: int = Field(
        default=..., description="Change in total number of comment lines"
    )
    delta_blanks: int = Field(
        default=..., description="Change in total number of blank lines"
    )
    delta_bytes: int = Field(default=..., description="Change in total number of bytes")


def validate_df(model: type[BaseModel], df: DataFrame) -> None:
    """
    Validate each row in a DataFrame against a Pydantic model.

    Args:
        model (type[BaseModel]): The Pydantic model class to validate against.
        df (DataFrame): The DataFrame to validate.

    """

    def _run(data: Series) -> None:
        """
        Instantiate a Pydantic model from a Pandas Series row.

        Converts the Series to a dictionary and validates it using the Pydantic model.
        Raises a ValidationError if the data does not conform to the model schema.

        Args:
            data (Series): A row from a DataFrame containing fields matching the
                Pydantic model.

        """
        row: dict = data.to_dict()
        try:
            model(**row)
        except ValidationError as ve:
            raise ve

    df.apply(_run, axis=1)
