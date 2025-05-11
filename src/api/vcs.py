import sys
from abc import ABC, abstractmethod
from os import getcwd
from pathlib import Path
from typing import Any, Iterator, List, Tuple

from git import Commit, Repo
from git.exc import InvalidGitRepositoryError
from pandas import DataFrame
from progress.bar import Bar


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


class Git(VersionControlSystem):
    def __init__(self, repo_path: Path):
        super().__init__(repo_path=repo_path)

    def _initialize_repo(self) -> Repo:
        try:
            return Repo(path=self.repo_path)
        except InvalidGitRepositoryError:
            sys.exit(1)

    def get_revisions(self) -> Tuple[Iterator[Commit], int]:
        revisionCount: int = sum(1 for _ in self.repo.iter_commits())
        return (self.repo.iter_commits(), revisionCount)

    def parse_revisions(
        self,
        revisions: Tuple[Iterator[Commit], int],
    ) -> DataFrame:
        data: List[dict] = []

        with Bar(self.parseRevisionsBarMessage, max=revisions[1]) as bar:
            commit: Commit
            for commit in revisions[0]:
                data.append(
                    {
                        "author": commit.author.name,
                        "author_email": commit.author.email,
                        "author_tz_offset": commit.author_tz_offset,
                        "authored_datetime": commit.authored_datetime,
                        "co_authors": [
                            co_author.name for co_author in commit.co_authors
                        ],
                        "co_authors_email": [
                            co_author.email for co_author in commit.co_authors
                        ],
                        "commit_hash": commit.hexsha,
                        "committed_datetime": commit.committed_datetime,
                        "committer": commit.committer.name,
                        "committer_email": commit.committer.email,
                        "committer_tz_offset": commit.committer_tz_offset,
                        "encoding": commit.encoding,
                        "gpgsign": commit.gpgsig,
                        "message": commit.message,
                        "parents": [
                            parent.hexsha for parent in commit.parents
                        ],  # noqa: E501
                    }
                )
                bar.next()

        return DataFrame(data=data)


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
