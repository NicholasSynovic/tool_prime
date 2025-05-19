from progress.bar import Bar
from pandas import DataFrame, Series
from typing import List
import pandas
from src.api.vcs import VersionControlSystem
import subprocess  # nosec
from subprocess import CompletedProcess  # nosec
from pathlib import Path
from io import StringIO


class SCC:
    def __init__(self):
        self.command: str = "scc"
        self.options: List[str] = (
            "--format=csv --by-file --no-cocomo --no-complexity --no-min-gen --no-size"
        )

    def run(self, repo_path: Path) -> DataFrame:
        result: CompletedProcess = subprocess.run(  # nosec
            args=[self.command, self.options, repo_path.__str__()],
            capture_output=True,
            text=True,
        )
        return pandas.read_csv(filepath_or_buffer=StringIO(result.stdout))


def compute_size_of_repo(
    commitsDF: DataFrame,
    vcs: VersionControlSystem,
) -> DataFrame:
    data: List[DataFrame] = []

    scc: SCC = SCC()

    with Bar(
        f"Measuring the size of {vcs.repo_path}...",
        max=commitsDF.shape[0],
    ) as bar:
        row: Series
        for _, row in commitsDF.iterrows():
            _df: DataFrame = scc.run(repo_path=vcs.repo_path)
            print(_df)
            continue
            vcs.checkout_revision(revision_hash=row["commit_hash"])

    return DataFrame()
