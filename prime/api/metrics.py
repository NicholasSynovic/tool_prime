"""
Classes to compute metrics.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from abc import ABC, abstractmethod
from collections.abc import Iterator

import pandas as pd
from pandas import (
    DataFrame,
    DatetimeIndex,
    Grouper,
    IntervalIndex,
    Series,
    Timestamp,
)
from pandas.core.groupby import DataFrameGroupBy
from progress.bar import Bar

import prime.api.types as prime_types
from prime.api.db import DB
from prime.api.size import SCC
from prime.api.vcs import VersionControlSystem


class Metric(ABC):
    def __init__(self, db: DB) -> None:
        self.db: DB = db
        self.input_data: DataFrame = DataFrame()
        self.computed_data: DataFrame = DataFrame()
        self.to_utc_date = lambda x: Timestamp(ts_input=x, tz="UTC").floor(freq="D")

    @abstractmethod
    def compute(self) -> None: ...

    @abstractmethod
    def preprocess(self) -> None: ...

    @abstractmethod
    def write(self) -> None: ...


class FileSizePerCommit(Metric):
    def __init__(self, vcs: VersionControlSystem, scc: SCC, db: DB) -> None:
        super().__init__(db=db)
        self.scc: SCC = scc
        self.vcs: VersionControlSystem = vcs

    def preprocess(self) -> None:
        self.input_data = self.db.read_table(
            table="commit_hashes",
            model=prime_types.CommitHashes,
        )

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

    def write(self) -> None:
        self.db.write_df(
            df=self.computed_data,
            table="file_size_per_commit",
            model=prime_types.T_FileSizePerCommit,
        )


class ProjectSizePerCommit(Metric):
    def __init__(self, db: DB) -> None:
        super().__init__(db=db)

    def preprocess(self) -> None:
        self.input_data = self.db.read_table(
            table="file_size_per_commit",
            model=prime_types.T_FileSizePerCommit,
        )

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

    def write(self) -> None:
        self.db.write_df(
            df=self.computed_data,
            table="project_size_per_commit",
            model=prime_types.T_ProjectSizePerCommit,
        )


class ProjectSizePerDay(Metric):
    def __init__(self, db: DB) -> None:
        super().__init__(db=db)

    def preprocess(self) -> None:
        sql: str = """
        SELECT
            p.id,
            p.lines,
            p.code,
            p.comments,
            p.blanks,
            p.bytes,
            c.committed_datetime
        FROM
            project_size_per_commit p
        JOIN
            commit_logs c
            ON p.commit_hash_id = c.commit_hash_id;
        """

        data: DataFrame = self.db.query_database(sql=sql)
        data["committed_datetime"] = data["committed_datetime"].apply(self.to_utc_date)

        self.input_data = data.drop_duplicates(
            subset="committed_datetime",
            keep="last",
            ignore_index=True,
        )

    def compute(self) -> None:
        # Get the oldest and current dates
        oldest_date: Timestamp = self.input_data["committed_datetime"][0]
        current_date: Timestamp = Timestamp.utcnow().floor(freq="D")

        # Create a daily date range between oldest and current date
        date_range: DatetimeIndex = pd.date_range(
            start=oldest_date,
            end=current_date,
            freq="D",
            tz="UTC",
        )

        # Reindex data to fit date range
        self.computed_data = self.input_data.set_index(keys="committed_datetime")
        self.computed_data = self.computed_data.reindex(date_range)
        self.computed_data.index.name = "date"
        self.computed_data = self.computed_data.reset_index()

        # Flood fill data
        self.computed_data = self.computed_data.ffill()

    def write(self) -> None:
        self.db.write_df(
            df=self.computed_data,
            table="project_size_per_day",
            model=prime_types.T_ProjectSizePerDay,
        )


class ProjectProductivityPerCommit(Metric):
    def __init__(self, db: DB) -> None:
        super().__init__(db=db)

    def preprocess(self) -> None:
        self.input_data = self.db.read_table(
            table="project_size_per_commit",
            model=prime_types.T_ProjectSizePerCommit,
        )

    def compute(self) -> None:
        self.computed_data = self.input_data.diff().fillna(0)
        self.computed_data = self.computed_data.drop(columns="commit_hash_id")
        self.computed_data = self.computed_data.add_prefix(prefix="delta_")

        self.computed_data["commit_hash_id"] = self.input_data["commit_hash_id"]

    def write(self) -> None:
        self.db.write_df(
            df=self.computed_data,
            table="project_productivity_per_commit",
            model=prime_types.T_ProjectProductivityPerCommit,
        )


class ProjectProductivityPerDay(Metric):
    def __init__(self, db: DB) -> None:
        super().__init__(db=db)

    def preprocess(self) -> None:
        sql: str = """
        SELECT
            p.id,
            p.delta_lines,
            p.delta_code,
            p.delta_comments,
            p.delta_blanks,
            p.delta_bytes,
            c.committed_datetime
        FROM
            project_productivity_per_commit p
        JOIN
            commit_logs c
            ON p.commit_hash_id = c.commit_hash_id;
        """

        data: DataFrame = self.db.query_database(sql=sql)
        data["committed_datetime"] = data["committed_datetime"].apply(self.to_utc_date)

        self.input_data = data

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

    def write(self) -> None:
        self.db.write_df(
            df=self.computed_data,
            table="project_productivity_per_day",
            model=prime_types.T_ProjectProductivityPerDay,
        )


class BusFactorPerDay(Metric):
    def __init__(self, db: DB) -> None:
        super().__init__(db=db)
        self.datum_list: list[DataFrame] = []

    def preprocess(self) -> None:
        sql: str = """
        SELECT
            p.id,
            p.delta_lines,
            p.delta_code,
            p.delta_comments,
            p.delta_blanks,
            p.delta_bytes,
            c.committer_id
            c.committed_datetime,
        FROM
            project_productivity_per_commit p
        JOIN
            commit_logs c
            ON p.commit_hash_id = c.commit_hash_id;
        """

        data: DataFrame = self.db.query_database(sql=sql)
        data["committed_datetime"] = data["committed_datetime"].apply(self.to_utc_date)

        self.input_data = data

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

    def write(self) -> None:
        self.db.write_df(
            df=self.computed_data,
            table="bus_factor",
            model=prime_types.T_BusFactorPerDay,
        )


class IssueSpoilagePerDay(Metric):
    def __init__(self, db: DB) -> None:
        super().__init__(db=db)
        self.daily_intervals: IntervalIndex = IntervalIndex.from_arrays(
            left=[0],
            right=[1],
            closed="both",
        )

    def preprocess(self) -> None:
        # Value to store temporary information
        data: DataFrame = DataFrame()

        # Get current date
        current_date: Timestamp = Timestamp.utcnow().floor(freq="D")

        # Get project start date
        sql: str = "SELECT id, MIN(committed_datetime) as date FROM commit_logs;"
        data = self.db.query_database(sql=sql)
        data["committed_datetime"] = data["committed_datetime"].apply(self.to_utc_date)
        project_start_date: Timestamp = data["committed_datetime"][0]

        # Create daily time intervals from 00:00:00:00:00 -> 23:59:59
        date_range: DatetimeIndex = pd.date_range(
            start=project_start_date,
            end=current_date,
            freq="D",
        )

        self.daily_intervals = IntervalIndex.from_arrays(
            left=date_range,
            right=date_range + pd.Timedelta(hours=23, minutes=59, seconds=59),
            closed="both",
        )[0:-1]

        # Get issues table
        data = self.db.read_table(table="issues", model=prime_types.Issues)
        data["created_at"] = data["created_at"].apply(self.to_utc_date)
        data["closed_at"] = data["closed_at"].apply(self.to_utc_date)
        data["closed_at"] = data["closed_at"].fillna(value=current_date)

        # Create closed interval between created_at and closed_at
        data["interval"] = data.apply(
            lambda row: pd.Interval(
                left=row["created_at"],
                right=row["closed_at"],
                closed="both",
            ),
            axis=1,
        )

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

    def write(self) -> None:
        self.db.write_df(
            df=self.computed_data,
            table="issue_spoilage_per_day",
            model=prime_types.T_IssueSpoilagePerDay,
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
