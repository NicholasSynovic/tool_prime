import sys
from abc import ABC, abstractmethod
from os import getcwd
from pathlib import Path
from typing import Any, List

from git import Commit, Repo
from git.exc import InvalidGitRepositoryError


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

    @abstractmethod
    def _initialize_repo() -> Any:
        """
        Abstract method to initialize a repository with the proper VCS library.
        Subclasses must implement this method.
        """
        pass

    @abstractmethod
    def get_revisions(self) -> List[Any]:
        """
        Abstract method to retrieve a list of revisions.
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

    def get_revisions(self) -> List[Commit]:
        return [commit for commit in self.repo.iter_commits()]


if __name__ == "__main__":
    git: Git = Git(repo_path=getcwd())
    print(git.get_revisions())
