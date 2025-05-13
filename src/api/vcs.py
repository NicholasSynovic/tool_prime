from abc import ABC, abstractmethod
from datetime import datetime, timezone
from os import getcwd
from pathlib import Path
from typing import Any, Iterator, List, Tuple

from git import Commit, Repo
from git.exc import InvalidGitRepositoryError
from pandas import DataFrame
from progress.bar import Bar

from src.api.utils import (
    copyDFColumnsAndRemoveDuplicateRowsByColumn,
    copyDFColumnsToDF,
    replaceDFValueInColumnWithIndexReference,
    replaceDFValueInColumnWithListOfIndexReferences,
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
    def _initialize_repo() -> Any:
        """
        Abstract method to initialize a repository with the proper VCS library.
        Subclasses must implement this method.
        """
        pass

    @abstractmethod
    def get_revisions(self) -> Tuple[Any, int]:
        """
        Abstract method to retrieve revisions and the number of revisions.
        Revisions are retrieved from the oldest revision to the newest.
        Subclasses must implement this method.
        """
        pass

    @abstractmethod
    def parse_revisions(self, revisions: Any) -> DataFrame:
        """
        Abstract method to parse a List[Revisions] and extract relevant data
        Subclasses must implement this method.
        """
        pass


class Revision:
    def __init__(
        self,
        author: str,
        authorEmail: str,
        authoredDatetime: datetime,
        coAuthors: List[str],
        coAuthorEmails: List[str],
        commitHash: str,
        committedDatetime: datetime,
        committer: str,
        committerEmail: str,
        encoding: str,
        message: str,
        gpgsign: str,
        parents: List[str],
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

    def get_revisions(self) -> Tuple[Iterator[Commit], int]:
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
        revisions: Tuple[Iterator[Commit], int],
    ) -> DataFrame:
        data: List[dict] = []

        with Bar(
            self.parseRevisionsBarMessage[DataFrame],
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
                        coAuthors=[
                            co_author.name for co_author in commit.co_authors
                        ],  # noqa: E501
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


def identifyVCS(repoPath: Path) -> VersionControlSystem | int:
    try:
        return Git(repo_path=repoPath)
    except InvalidGitRepositoryError:
        return -1


def parseVCS(vcs: VersionControlSystem) -> dict[str, DataFrame]:
    data: dict[str:DataFrame] = {}

    # Extract the commit log
    revisions: Tuple[Any, int] = vcs.get_revisions()
    commitLogDF: DataFrame = vcs.parse_revisions(revisions=revisions)

    # Copy static information to output data structure
    data["commit_hashes"] = copyDFColumnsToDF(
        df=commitLogDF,
        columns=["commit_hash"],
    )
    data["authors"] = copyDFColumnsAndRemoveDuplicateRowsByColumn(
        df=commitLogDF,
        columns=["author", "author_email"],
        checkColumn="author_email",
    )
    data["committers"] = copyDFColumnsAndRemoveDuplicateRowsByColumn(
        df=commitLogDF,
        columns=["committer", "committer_email"],
        checkColumn="committer_email",
    )

    # Replace commit log information with the index to static DataFrames
    commitLogDF = replaceDFValueInColumnWithIndexReference(
        df1=commitLogDF,
        df2=data["commit_hashes"],
        df1Col="commit_hash",
        df2Col="commit_hash",
    )
    commitLogDF = replaceDFValueInColumnWithIndexReference(
        df1=commitLogDF,
        df2=data["authors"],
        df1Col="author_email",
        df2Col="author_email",
    )
    commitLogDF = replaceDFValueInColumnWithIndexReference(
        df1=commitLogDF,
        df2=data["committers"],
        df1Col="committer_email",
        df2Col="committer_email",
    )

    # Replace commit log information with a list of indicies from static
    # DataFrames
    commitLogDF = replaceDFValueInColumnWithListOfIndexReferences(
        df1=commitLogDF,
        df2=data["authors"],
        df1Col="co_author_emails",
        df2Col="author_email",
    )
    commitLogDF = replaceDFValueInColumnWithListOfIndexReferences(
        df1=df,
        df2=data["commit_hashes"],
        df1Col="parents",
        df2Col="commit_hash",
    )

    # Drop irrelevant columns and rename existing columns to match database
    # schema
    commitLogDF.drop(
        columns=["author", "committer", "co_authors"],
        inplace=True,
    )
    commitLogDF.rename(
        columns={
            "author_email": "author_id",
            "co_author_emails": "co_author_ids",
            "commit_hash": "commit_hash_id",
            "committer_email": "committer_id",
            "parents": "parent_hash_ids",
        },
        inplace=True,
    )

    data["commit_logs"] = commitLogDF

    return data


if __name__ == "__main__":
    git: Git = Git(repo_path=getcwd())
    revisions: Iterator[Commit] = git.get_revisions()

    df: DataFrame = git.parse_revisions(revisions=revisions)

    df.to_json(
        path_or_buf="test.json",
        indent=4,
        index=False,
        orient="records",
    )
