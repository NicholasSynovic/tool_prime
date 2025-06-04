from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from git import Commit, Repo, TagReference
from git.exc import InvalidGitRepositoryError
from pandas import DataFrame
from progress.bar import Bar

from src.api.utils import (
    copyDFColumnsAndRemoveDuplicateRowsByColumn,
    copyDFColumnsToDF,
    replaceDFValueInColumnWithIndexReference,
    replaceDFValueInColumnWithlistOfIndexReferences,
)


class VersionControlSystem(ABC):
    """
    Abstract base class for interacting with version control systems.
    Defines a common interface for retrieving information about revisions.
    """

    def __init__(self, repo_path: Path):
        """
        Initializes the VersionControlSystem object.

        Args:
            repo_path (Path): Path to the repository.
        """
        self.repo_path: Path = repo_path
        self.repo: Repo | Any = self._initialize_repo()
        self.parseRevisionsBarMessage: str = "Parsing revisions..."

    @abstractmethod
    def _initialize_repo(self) -> Any:
        """
        Abstract method to initialize a repository with the proper VCS library.
        Subclasses must implement this method.
        """
        pass

    @abstractmethod
    def get_revisions(self) -> tuple[Any, int]:
        """
        Abstract method to retrieve revisions and the number of revisions.
        Revisions are retrieved from the oldest revision to the newest.
        Subclasses must implement this method.
        """
        pass

    @abstractmethod
    def parse_revisions(self, revisions: Any) -> DataFrame:
        """
        Abstract method to parse a list[Revisions] and extract relevant data
        Subclasses must implement this method.
        """
        pass

    @abstractmethod
    def get_release_revisions(self) -> DataFrame:
        pass

    @abstractmethod
    def checkout_revision(self, revision_hash: str) -> None:
        pass

    @abstractmethod
    def checkout_most_recent_revision(self) -> None:
        pass


class Revision:
    def __init__(
        self,
        author: str,
        authorEmail: str,
        authoredDatetime: datetime,
        coAuthors: list[str],
        coAuthorEmails: list[str],
        commitHash: str,
        committedDatetime: datetime,
        committer: str,
        committerEmail: str,
        encoding: str,
        message: str,
        gpgsign: str,
        parents: list[str],
    ):
        self.data: dict[str, Any] = {
            "author": author,
            "author_email": authorEmail,
            "authored_datetime": authoredDatetime,
            "co_authors": coAuthors,
            "co_author_emails": coAuthorEmails,
            "commit_hash": commitHash,
            "committed_datetime": committedDatetime,
            "committer": committer,
            "committer_email": committerEmail,
            "encoding": encoding,
            "gpgsign": gpgsign,
            "message": message,
            "parents": parents,
        }


class Git(VersionControlSystem):
    def __init__(self, repo_path: Path):
        super().__init__(repo_path=repo_path)

    def _initialize_repo(self) -> Repo:
        return Repo(path=self.repo_path)

    def _extract_revision_from_tag(self, tag: TagReference) -> str | None:
        try:
            return tag.commit.hexsha
        except ValueError:
            return None

    def get_revisions(self) -> tuple[Iterator[Commit], int]:
        revisionCount: int = sum(1 for _ in self.repo.iter_commits())
        return (
            self.repo.iter_commits(
                reverse=True,
                date="raw",
            ),
            revisionCount,
        )

    def parse_revisions(
        self,
        revisions: tuple[Iterator[Commit], int],
    ) -> DataFrame:
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
                        authorEmail=commit.author.email,
                        authoredDatetime=commit.authored_datetime.astimezone(
                            tz=timezone.utc
                        ),
                        coAuthors=[co_author.name for co_author in commit.co_authors],
                        coAuthorEmails=[
                            co_author.email for co_author in commit.co_authors
                        ],
                        commitHash=commit.hexsha,
                        committedDatetime=commit.committed_datetime.astimezone(
                            tz=timezone.utc
                        ),
                        committer=commit.committer.name,
                        committerEmail=commit.committer.email,
                        encoding=commit.encoding,
                        gpgsign=commit.gpgsig,
                        message=commit.message,
                        parents=[parent.hexsha for parent in commit.parents],
                    ).data
                )
                bar.next()

        return DataFrame(data=data)

    def get_release_revisions(self) -> DataFrame:
        data: dict[str, list[str]] = {"commit_hash_id": []}

        tags: list[TagReference] = self.repo.tags
        tag_revision_hashes: map[str | None] = map(
            self._extract_revision_from_tag, tags
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
    data["commit_hashes"] = copyDFColumnsToDF(
        df=commit_log_df,
        columns=["commit_hash"],
    )
    data["authors"] = copyDFColumnsAndRemoveDuplicateRowsByColumn(
        df=commit_log_df,
        columns=["author", "author_email"],
        checkColumn="author_email",
    )
    data["committers"] = copyDFColumnsAndRemoveDuplicateRowsByColumn(
        df=commit_log_df,
        columns=["committer", "committer_email"],
        checkColumn="committer_email",
    )

    # Replace commit log information with the index to static DataFrames
    releases_df = replaceDFValueInColumnWithIndexReference(
        df_1=releases_df,
        df_2=data["commit_hashes"],
        df_1_col="commit_hash_id",
        df_2_col="commit_hash",
    )
    releases_df = releases_df.dropna(how="any", ignore_index=True)
    releases_df["commit_hash_id"] = releases_df["commit_hash_id"].apply(int)

    commit_log_df = replaceDFValueInColumnWithIndexReference(
        df_1=commit_log_df,
        df_2=data["commit_hashes"],
        df_1_col="commit_hash",
        df_2_col="commit_hash",
    )
    commit_log_df = replaceDFValueInColumnWithIndexReference(
        df_1=commit_log_df,
        df_2=data["authors"],
        df_1_col="author_email",
        df_2_col="author_email",
    )
    commit_log_df = replaceDFValueInColumnWithIndexReference(
        df_1=commit_log_df,
        df_2=data["committers"],
        df_1_col="committer_email",
        df_2_col="committer_email",
    )

    # Replace commit log information with a list of indicies from static
    # DataFrames
    commit_log_df = replaceDFValueInColumnWithlistOfIndexReferences(
        df_1=commit_log_df,
        df_2=data["authors"],
        df_1_col="co_author_emails",
        df_2_col="author_email",
    )
    commit_log_df = replaceDFValueInColumnWithlistOfIndexReferences(
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
