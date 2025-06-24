"""
Classes to compute metrics.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from abc import ABC, abstractmethod
from collections.abc import Iterator

import pandas as pd
from pandas import (
    DataFrame,
    Grouper,
    IntervalIndex,
    Series,
    Timestamp,
)
from pandas.core.groupby import DataFrameGroupBy
from progress.bar import Bar

from prime.api.size import SCC
from prime.api.vcs import VersionControlSystem


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
        # Group data by committed datetime
        data_grouped_by_days: DataFrameGroupBy = self.input_data.groupby(
            by=Grouper(
                key="committed_datetime",
                freq="D",
            ),
        )

        # Get the most recent commit information per group
        scratch_dataframe: DataFrame = data_grouped_by_days.nth[-1]
        scratch_dataframe = scratch_dataframe.reset_index(drop=True)
        scratch_dataframe["date"] = scratch_dataframe["committed_datetime"]
        scratch_dataframe = scratch_dataframe.drop(
            columns=[
                "commit_hash_id",
                "committed_datetime",
            ]
        )

        # Fill in missing dates
        date_fill_dataframe: DataFrame = scratch_dataframe.set_index(
            keys="date",
        )
        date_fill_dataframe = date_fill_dataframe.resample(rule="D").ffill()

        self.computed_data = date_fill_dataframe.reset_index()


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


class BusFactorPerDay(Metric):
    def __init__(self, input_data: DataFrame) -> None:
        super().__init__(input_data=input_data)
        self.datum_list: list[DataFrame] = []

    def compute(self) -> None:
        data_grouped_by_days: DataFrameGroupBy = self.input_data.groupby(
            by=Grouper(
                key="committed_datetime",
                freq="D",
            ),
        )

        with Bar("Computing bus factor...", max=len(data_grouped_by_days)) as bar:
            idx: Timestamp
            date_group: DataFrame
            for idx, date_group in data_grouped_by_days:
                date_group = date_group.drop(columns="committed_datetime")
                date_group = date_group.abs()

                data_group_by_committer: DataFrameGroupBy = date_group.groupby(
                    by="committer_id",
                )

                committer_contributions: DataFrame = data_group_by_committer.sum(
                    numeric_only=True
                )
                committer_contributions["committer_id"] = committer_contributions.index

                if committer_contributions.empty:
                    committer_contributions.loc[0] = [-1, -1, -1, -1, -1, -1]

                committer_contributions["date"] = idx
                committer_contributions = committer_contributions.reset_index(drop=True)

                self.datum_list.append(committer_contributions)

                bar.next()

        self.computed_data = pd.concat(objs=self.datum_list, ignore_index=True)


class IssueSpoilagePerDay(Metric):
    def __init__(
        self,
        daily_intervals: IntervalIndex,
        input_data: DataFrame,
    ) -> None:
        super().__init__(input_data=input_data)
        self.daily_intervals: IntervalIndex = daily_intervals

    def compute(self) -> None:
        # Extract left and right bounds from input intervals
        input_left = self.input_data["interval"].apply(lambda i: i.left).values
        input_right = self.input_data["interval"].apply(lambda i: i.right).values

        # Extract left and right bounds from daily intervals
        daily_left = self.daily_intervals.left.values
        daily_right = self.daily_intervals.right.values

        # Broadcast comparisons: input intervals that fully cover daily intervals
        # Shape: (num_daily, num_input)
        covers = (input_left[None, :] <= daily_left[:, None]) & (
            input_right[None, :] >= daily_right[:, None]
        )

        # Count how many input intervals cover each daily interval
        open_issues = covers.sum(axis=1)

        # Construct result DataFrame
        self.computed_data = pd.DataFrame(
            {"start": daily_left, "end": daily_right, "open_issues": open_issues}
        )


class IssueDensityPerDay(Metric):
    def __init__(
        self,
        issue_spoilage_per_day: DataFrame,
        project_size_per_day: DataFrame,
    ) -> None:
        super().__init__(input_data=issue_spoilage_per_day)
        self.project_size_per_day: DataFrame = project_size_per_day

    def compute(self) -> None:
        self.computed_data = self.input_data.merge(
            self.project_size_per_day.rename(columns={"date": "start"}),
            on="start",
            how="left",
        ).ffill()
