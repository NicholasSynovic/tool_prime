"""
SCC executor.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from pandas import DataFrame
import pandas as pd
import subprocess  # nosec
from subprocess import CompletedProcess  # nosec
from pathlib import Path
from io import StringIO
import shlex


class SCC:
    """
    Class to execute SCC.

    This class defines the structure and required methods for interacting with
    SCC.

    Attributes:
        directory (Path): The file system path to the repository.
        command (str): SCC program name.
        options (str): SCC options.

    Methods:
        run(): Run SCC in a particular directory.

    """

    def __init__(self, directory: Path) -> None:
        """
        Initialize the SCC object with a specified directory.

        Args:
            directory (Path): The directory to use for the SCC command.

        """
        self.directory: Path = directory.resolve()
        self.command: str = "scc"
        self.options: str = (
            "--format=csv --by-file --no-cocomo --no-complexity --no-min-gen --no-size"
        )

    def run(self) -> DataFrame:
        """
        Execute the SCC and parse the output as a CSV DataFrame.

        This method executes the specified command and parses the output as a pandas
        DataFrame.

        Returns:
            DataFrame: A pandas DataFrame representing the output.

        """
        args: list[str] = (
            [self.command] + shlex.split(s=self.options) + [str(self.directory)]  # noqa: RUF005
        )

        result: CompletedProcess = subprocess.run(  # nosec
            args=args,
            capture_output=True,
            text=True,
            check=False,
        )

        return pd.read_csv(filepath_or_buffer=StringIO(result.stdout))
