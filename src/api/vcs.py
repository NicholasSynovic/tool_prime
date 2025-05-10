from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List


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
        self.repo_path = repo_path
        self.repo: Any = self._initialize_repo()

    @abstractmethod
    def _initialize_repo() -> Any:
        """
        Abstract method to initialize a repository with the proper VCS library.
        Subclasses must implement this method.
        """
        pass

    @abstractmethod
    def get_revisions(self) -> List[str]:
        """
        Abstract method to retrieve a list of revisions.
        Subclasses must implement this method.
        """
        pass
