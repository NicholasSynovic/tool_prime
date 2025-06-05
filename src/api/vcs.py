"""
Parsing and analyzing version control system (VCS) data.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from collections.abc import Iterator

from git import Commit, Repo, TagReference
from git.exc import InvalidGitRepositoryError
from pandas import DataFrame
from progress.bar import Bar
from typing import NamedTuple

from src.api.utils import (
    copy_dataframe_cols_and_remove_duplicate_rows_by_col,
    copy_dataframe_columns_to_dataframe,
    replace_dataframe_value_column_with_index_reference,
    replace_dataframe_value_column_with_index_reference_list,
)


class VersionControlSystem(ABC):
    """
    Abstract base class representing a version control system (VCS) interface.

    This class defines the structure and required methods for interacting with
    a version control repository. Subclasses must implement methods for
    repository initialization, revision extraction, revision parsing,
    release identification, and revision checkout.

    Attributes:
        repo_path (Path): The file system path to the repository.
        repo (Repo | Any): The initialized repository object.
        parseRevisionsBarMessage (str): Message to display when parsing
            revisions.

    Methods:
        _initialize_repo(): Initialize and return the repository object.
        get_revisions(): Return an iterable of commits and the total number of
            revisions.
        parse_revisions(revisions): Parse revisions into a structured DataFrame.
        get_release_revisions(): Extract and return release-related revisions.
        checkout_revision(revision_hash): Checkout a specific revision by its hash.
        checkout_most_recent_revision(): Checkout the most recent revision.

    """

    def __init__(self, repo_path: Path) -> None:
        """
        Initialize the VersionControlSystem object.

        Args:
            repo_path (Path): Path to the repository.

        """
        self.repo_path: Path = repo_path
        self.repo: Repo | Any = self._initialize_repo()
        self.parseRevisionsBarMessage: str = "Parsing revisions..."

    @abstractmethod
    def _initialize_repo(self) -> Any: ...  # noqa: ANN401

    @abstractmethod
    def get_revisions(self) -> tuple[Any, int]: ...  # noqa: D102

    @abstractmethod
    def parse_revisions(self, revisions: Any) -> DataFrame: ...  # noqa: D102, ANN401

    @abstractmethod
    def get_release_revisions(self) -> DataFrame: ...  # noqa: D102

    @abstractmethod
    def checkout_revision(self, revision_hash: str) -> None: ...  # noqa: D102

    @abstractmethod
    def checkout_most_recent_revision(self) -> None: ...  # noqa: D102


class Revision(NamedTuple):
    """
    Represents a Git revision (commit) with detailed metadata.

    This named tuple holds information extracted from a version control commit,
    including authorship, commit metadata, parent relationships, and cryptographic
    signature information. It is used to structure and serialize commit data
    for further processing or database insertion.

    Attributes:
        author (str): Name of the commit author.
        author_email (str): Email address of the commit author.
        authored_datetime (datetime): Date and time the commit was authored (UTC).
        co_authors (list[str]): Names of co-authors listed in the commit.
        co_author_emails (list[str]): Email addresses of co-authors.
        commit_hash (str): SHA-1 hash identifying the commit.
        committed_datetime (datetime): Date and time the commit was committed (UTC).
        committer (str): Name of the person who committed the change.
        committer_email (str): Email address of the committer.
        encoding (str): Character encoding used for the commit message.
        message (str): Commit message content.
        gpgsign (str): GPG signature of the commit, if present.
        parents (list[str]): List of parent commit hashes.

    Properties:
        data (dict[str, Any]): Dictionary representation of the commit metadata.

    """

    author: str
    author_email: str
    authored_datetime: datetime
    co_authors: list[str]
    co_author_emails: list[str]
    commit_hash: str
    committed_datetime: datetime
    committer: str
    committer_email: str
    encoding: str
    message: str
    gpgsign: str
    parents: list[str]

    @property
    def data(self) -> dict[str, Any]:
        """
        Return a dictionary representation of the revision metadata.

        This property provides access to all fields of the `Revision` instance
        as a dictionary, mapping attribute names to their corresponding values.
        Useful for serialization or structured logging.

        Returns:
            dict[str, Any]: Dictionary containing all commit metadata fields.

        """
        return self._asdict()


class Git(VersionControlSystem):
    """
    Git version control system interface.

    This class extends the VersionControlSystem base class to provide
    Git-specific functionality using the GitPython library. It supports
    operations such as retrieving commits, parsing revision metadata,
    identifying releases (tags), and checking out revisions.

    Attributes:
        repo_path (Path): Path to the Git repository.
        repo (Repo): GitPython `Repo` object for interacting with the repository.

    """

    def __init__(self, repo_path: Path) -> None:
        """
        Initialize the Git handler with the given repository path.

        Args:
            repo_path (Path): The file system path to the root of the repository.

        """
        super().__init__(repo_path=repo_path)

    def _initialize_repo(self) -> Repo:
        """
        Initialize and return a GitPython Repo object for the repository path.

        Returns:
            Repo: A GitPython `Repo` instance pointing to the specified repository.

        """
        return Repo(path=self.repo_path)

    def get_revisions(self) -> tuple[Iterator[Commit], int]:
        """
        Retrieve an iterator of all commits along with the total count.

        Returns a tuple containing
        - An iterator over commits in chronological order (oldest to newest).
        - The total number of commits in the repository.

        Returns:
            tuple[Iterator[Commit], int]: A tuple of commit iterator and commit
            count.

        """
        revision_count: int = sum(1 for _ in self.repo.iter_commits())
        return (
            self.repo.iter_commits(
                reverse=True,
                date="raw",
            ),
            revision_count,
        )

    def parse_revisions(
        self,
        revisions: tuple[Iterator[Commit], int],
    ) -> DataFrame:
        """
        Parse a sequence of Git commits into a structured DataFrame.

        Iterates over a provided iterator of Git `Commit` objects, extracting relevant
        metadata from each commit such as author, committer, timestamps, GPG signature,
        commit message, parents, and co-authors. The data is structured into
        dictionaries and collected into a DataFrame. A progress bar is displayed
        during processing.

        Args:
            revisions (tuple[Iterator[Commit], int]): A tuple containing:
                - An iterator over `Commit` objects.
                - An integer representing the total number of revisions (for
                    progress bar tracking).

        Returns:
            DataFrame: A pandas DataFrame where each row represents a parsed commit with
            structured metadata fields.

        """
        data: list[dict] = []

        with Bar(
            self.parseRevisionsBarMessage,
            max=revisions[1],
        ) as bar:
            commit: Commit
            for commit in revisions[0]:
                data.append(
                    Revision(
                        author=commit.author.name,
                        author_email=commit.author.email,
                        authored_datetime=commit.authored_datetime.astimezone(
                            tz=timezone.utc
                        ),
                        co_authors=[co_author.name for co_author in commit.co_authors],
                        co_author_emails=[
                            co_author.email for co_author in commit.co_authors
                        ],
                        commit_hash=commit.hexsha,
                        committed_datetime=commit.committed_datetime.astimezone(
                            tz=timezone.utc
                        ),
                        committer=commit.committer.name,
                        committer_email=commit.committer.email,
                        encoding=commit.encoding,
                        gpgsign=commit.gpgsig,
                        message=commit.message,
                        parents=[parent.hexsha for parent in commit.parents],
                    ).data
                )
                bar.next()

        return DataFrame(data=data)

    def get_release_revisions(self) -> DataFrame:
        """
        Extract commit hashes associated with Git tags (releases).

        This method iterates through all tags in the repository, extracts the
        commit hash each tag points to (if valid), and returns them in a
        DataFrame under the column "commit_hash_id".

        Returns:
            DataFrame: A DataFrame containing a single column, "commit_hash_id",
            which lists the commit hashes corresponding to valid tags.

        """
        data: dict[str, list[str]] = {"commit_hash_id": []}

        tags: list[TagReference] = self.repo.tags
        tag_revision_hashes: map[str | None] = map(
            extract_revision_hash_from_git_tag, tags
        )

        # trh is an abbreviation for tag_revision_hash
        data["commit_hash_id"] = [trh for trh in tag_revision_hashes if trh is not None]

        return DataFrame(data=data)

    def checkout_revision(self, revision_hash: str) -> None:
        """
        Check out a specific commit in the repository by its hash.

        This method resolves the given commit hash and performs a Git checkout
        to switch the working directory to that commit.

        Args:
            revision_hash (str): The SHA-1 hash of the commit to check out.

        """
        commit: Commit = self.repo.commit(rev=revision_hash)
        self.repo.git.checkout(commit)

    def checkout_most_recent_revision(self) -> None:
        """
        Check out the most recent (HEAD) commit in the repository.

        Retrieves the latest commit hash from the current branch and checks it out
        using the `checkout_revision` method.

        """
        self.checkout_revision(revision_hash=self.repo.head.commit.hexsha)


def identify_vcs(repo_path: Path) -> VersionControlSystem | int:
    """
    Identify and return the version control system used at a given repository path.

    Attempts to initialize a Git repository at the specified path. If the path
    is not a valid Git repository, returns -1 to indicate failure.

    Args:
        repo_path (Path): The filesystem path to the repository.

    Returns:
        VersionControlSystem | int: An instance of a `VersionControlSystem` (e.g.,
        `Git`) if successful; otherwise, -1 if the repository is invalid or unsupported.

    """
    try:
        return Git(repo_path=repo_path)
    except InvalidGitRepositoryError:
        return -1


def parse_vcs(
    vcs: VersionControlSystem,
    previous_revisions: DataFrame | None,
) -> dict[str, DataFrame]:
    """
    Parse and structure version control system data into normalized DataFrames.

    This function processes revision and release data from a version control
    system (VCS), filters out revisions that have already been processed, and
    normalizes the data by extracting static information (authors, committers,
    commit hashes) into separate DataFrames. It also replaces values in the
    commit and release logs with references to these static tables to support
    database-friendly indexing.

    Args:
        vcs (VersionControlSystem): The version control system instance to parse.
        previous_revisions (DataFrame | None): Optional DataFrame of previously
        processed commit hashes to exclude from the current run.

    Returns:
        dict[str, DataFrame]: A dictionary of normalized DataFrames with the
        following keys:
            - "commit_hashes": Unique commit hashes.
            - "authors": Unique authors based on email.
            - "committers": Unique committers based on email.
            - "releases": Mapped and filtered release information.
            - "commit_logs": Normalized commit logs with references to static tables.

    """
    data: dict[str, DataFrame] = {}

    # Extract the commit log and release revisions
    revisions: tuple[Any, int] = vcs.get_revisions()
    releases_df: DataFrame = vcs.get_release_revisions()
    commit_log_df: DataFrame = vcs.parse_revisions(revisions=revisions)

    # Remove previously stored revisions from DataFrames
    if isinstance(previous_revisions, DataFrame):
        commit_log_df = commit_log_df[
            ~commit_log_df["commit_hash"].isin(previous_revisions["commit_hash"])
        ]
        releases_df = releases_df[
            ~releases_df["commit_hash_id"].isin(previous_revisions["commit_hash"])
        ]

    # Copy static information to output data structure
    data["commit_hashes"] = copy_dataframe_columns_to_dataframe(
        df=commit_log_df,
        columns=["commit_hash"],
    )
    data["authors"] = copy_dataframe_cols_and_remove_duplicate_rows_by_col(
        df=commit_log_df,
        columns=["author", "author_email"],
        checkColumn="author_email",
    )
    data["committers"] = copy_dataframe_cols_and_remove_duplicate_rows_by_col(
        df=commit_log_df,
        columns=["committer", "committer_email"],
        checkColumn="committer_email",
    )

    # Replace commit log information with the index to static DataFrames
    releases_df = replace_dataframe_value_column_with_index_reference(
        df_1=releases_df,
        df_2=data["commit_hashes"],
        df_1_col="commit_hash_id",
        df_2_col="commit_hash",
    )
    releases_df = releases_df.dropna(how="any", ignore_index=True)
    releases_df["commit_hash_id"] = releases_df["commit_hash_id"].apply(int)

    commit_log_df = replace_dataframe_value_column_with_index_reference(
        df_1=commit_log_df,
        df_2=data["commit_hashes"],
        df_1_col="commit_hash",
        df_2_col="commit_hash",
    )
    commit_log_df = replace_dataframe_value_column_with_index_reference(
        df_1=commit_log_df,
        df_2=data["authors"],
        df_1_col="author_email",
        df_2_col="author_email",
    )
    commit_log_df = replace_dataframe_value_column_with_index_reference(
        df_1=commit_log_df,
        df_2=data["committers"],
        df_1_col="committer_email",
        df_2_col="committer_email",
    )

    # Replace commit log information with a list of indicies from static
    # DataFrames
    commit_log_df = replace_dataframe_value_column_with_index_reference_list(
        df_1=commit_log_df,
        df_2=data["authors"],
        df_1_col="co_author_emails",
        df_2_col="author_email",
    )
    commit_log_df = replace_dataframe_value_column_with_index_reference_list(
        df_1=commit_log_df,
        df_2=data["commit_hashes"],
        df_1_col="parents",
        df_2_col="commit_hash",
    )

    # Drop irrelevant columns and rename existing columns to match database
    # schema
    commit_log_df = commit_log_df.drop(
        columns=["author", "committer", "co_authors"],
    )
    commit_log_df = commit_log_df.rename(
        columns={
            "author_email": "author_id",
            "co_author_emails": "co_author_ids",
            "commit_hash": "commit_hash_id",
            "committer_email": "committer_id",
            "parents": "parent_hash_ids",
        },
    )

    data["releases"] = releases_df
    data["commit_logs"] = commit_log_df

    return data


def extract_revision_hash_from_git_tag(git_tag: TagReference) -> str | None:
    """
    Extract the commit hash from a Git tag.

    Attempts to resolve the provided Git tag to its associated commit and
    return the commit hash. Returns None if the tag cannot be resolved.

    Args:
        git_tag (TagReference): A GitPython TagReference object.

    Returns:
        str | None: The commit hash (SHA-1) if resolvable, otherwise None.

    """
    try:
        return git_tag.commit.hexsha
    except ValueError:
        return None
