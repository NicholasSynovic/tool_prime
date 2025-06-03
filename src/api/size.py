from pandas import DataFrame
import pandas
import subprocess  # nosec
from subprocess import CompletedProcess  # nosec
from pathlib import Path
from io import StringIO
import shlex


class SCC:
    def __init__(self, directory: Path):
        self.directory: Path = directory.resolve()
        self.command: str = "scc"
        self.options: str = (
            "--format=csv --by-file --no-cocomo --no-complexity --no-min-gen --no-size"
        )

    def run(self) -> DataFrame:
        result: CompletedProcess = subprocess.run(  # nosec
            args=[self.command] + shlex.split(s=self.options) + [str(self.directory)],
            capture_output=True,
            text=True,
        )

        return pandas.read_csv(filepath_or_buffer=StringIO(result.stdout))
