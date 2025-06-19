"""
Classes to compute metrics.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from abc import ABC, abstractmethod
from collections.abc import Iterator

import pandas as pd
from pandas import DataFrame, Grouper, Series, Timestamp
from pandas.core.groupby import DataFrameGroupBy
from progress.bar import Bar
from pydantic import BaseModel

from src.api.size import SCC
from src.api.vcs import VersionControlSystem


class Metric(ABC):
    def __init__(self, input_data: DataFrame) -> None:
        self.input_data: DataFrame = input_data
        self.computed_data: DataFrame = DataFrame()

    @abstractmethod
    def compute(self) -> None: ...


class FileSizePerCommit(Metric):
    def __init__(
        self,
        vcs: VersionControlSystem,
        scc: SCC,
        commit_hashes: DataFrame,
    ) -> None:
        self.scc: SCC = scc
        self.vcs: VersionControlSystem = vcs
        super().__init__(input_data=commit_hashes)

    def compute(self) -> None:
        data: list[DataFrame] = []

        number_of_commits: int = self.input_data.shape[0]
        rows: Iterator[tuple[int, Series]] = self.input_data.iterrows()

        idx: int
        row: Series
        with Bar("Computing size per commit...", max=number_of_commits) as bar:
            for idx, row in rows:
                commit_hash: str = row["commit_hash"]

                self.vcs.checkout_revision(revision_hash=commit_hash)

                scc_data: DataFrame = self.scc.run()
                scc_data["commit_hash_id"] = idx
                scc_data = scc_data.drop(columns=["Filename", "Complexity", "ULOC"])

                data.append(scc_data)
                bar.next()

        self.vcs.checkout_most_recent_revision()

        self.computed_data = pd.concat(objs=data, ignore_index=True)
        self.computed_data.columns = self.computed_data.columns.str.lower()


class ProjectProductivityPerCommit(Metric):
    def __init__(self, project_size_per_commit: DataFrame) -> None:
        super().__init__(input_data=project_size_per_commit)

    def compute(self) -> None:
        self.computed_data = self.input_data.diff().fillna(0)
        self.computed_data = self.computed_data.drop(columns="commit_hash_id")
        self.computed_data = self.computed_data.add_prefix(prefix="delta_")

        self.computed_data["commit_hash_id"] = self.input_data["commit_hash_id"]


class ProjectProductivityPerDay(Metric):
    def __init__(self, input_data: DataFrame) -> None:
        super().__init__(input_data=input_data)

    def compute(self) -> None:
        data_grouped_by_days: DataFrameGroupBy = self.input_data.groupby(
            by=Grouper(
                key="committed_datetime",
                freq="D",
            ),
        )

        self.computed_data = data_grouped_by_days.sum()

        self.computed_data["date"] = self.computed_data.index

        self.computed_data = self.computed_data.drop(columns="commit_hash_id")
        self.computed_data = self.computed_data.reset_index(drop=True)


class ProjectSizePerDay(Metric):
    def __init__(self, input_data: DataFrame) -> None:
        super().__init__(input_data=input_data)

    def compute(self) -> None:
        data_grouped_by_days: DataFrameGroupBy = self.input_data.groupby(
            by=Grouper(
                key="committed_datetime",
                freq="D",
            ),
        )

        self.computed_data = data_grouped_by_days.sum(numeric_only=True)
        self.computed_data = self.computed_data.drop(columns="commit_hash_id")

        self.computed_data["date"] = self.computed_data.index
        self.computed_data = self.computed_data.reset_index(drop=True)


class ProjectSizePerCommit(Metric):
    def __init__(self, file_sizes: DataFrame) -> None:
        super().__init__(input_data=file_sizes)

    def compute(self) -> None:
        # Group files by commit hash
        commit_groups: DataFrameGroupBy = self.input_data.groupby(
            by="commit_hash_id",
        )

        # Sum each column and convert floats to integers
        self.computed_data = commit_groups.sum(numeric_only=True)
        self.computed_data = self.computed_data.apply(
            pd.to_numeric,
            downcast="integer",
        )

        # Reestablish the commit_hash_id column
        self.computed_data["commit_hash_id"] = self.computed_data.index

        # Reset index
        self.computed_data = self.computed_data.reset_index(drop=True)


class BusFactorMetric:
    def __init__(self, input_data: DataFrame) -> None:
        self.input_data: DataFrame = input_data
        self.data: DataFrame = DataFrame()

    def compute(self) -> None:
        data: list[dict[str, Timestamp | int | float]] = []

        grouped_data_by_day: DataFrameGroupBy = self.input_data.groupby(
            by=Grouper(key="committed_datetime", freq="D")
        )

        idx: Timestamp
        df: DataFrame
        for idx, df in grouped_data_by_day:
            print("\n", idx, df)
            input()
