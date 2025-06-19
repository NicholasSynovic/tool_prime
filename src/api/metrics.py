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


class ProjectSizeMetric:
    """
    A class to compute and store project size metrics aggregated by commit.

    This class processes a DataFrame containing project size data and calculates
    aggregated metrics for each commit. The results are stored in the `data`
    attribute as a DataFrame.

    Attributes:
        size_table (DataFrame): The input DataFrame containing size data for
            each commit.
        data (DataFrame): The output DataFrame containing computed size metrics
            per commit.

    """

    def __init__(self, size_table: DataFrame) -> None:
        """
        Initialize the ProjectSizeMetric with the provided size data.

        Args:
            size_table (DataFrame): A DataFrame containing size metrics for each
                commit.

        """
        self.size_table: DataFrame = size_table
        self.data: DataFrame = DataFrame()

    def compute(self) -> None:
        """
        Compute the project size metrics based on the size data.

        This method groups the size data by commit hash, calculates the sum of
        numeric columns for each group, and stores the results in the `data`
        attribute as a DataFrame with integer downcasting applied.

        The resulting DataFrame includes:
        - Aggregated size metrics for each commit.
        - A "commit_hash_id" column representing the commit identifiers.
        """
        commit_groups: DataFrameGroupBy = self.size_table.groupby(
            by="commit_hash_id",
        )
        size: DataFrame = commit_groups.sum(numeric_only=True)
        size["commit_hash_id"] = size.index
        size = size.apply(pd.to_numeric, downcast="integer")
        self.data = size.reset_index(drop=True)


class DailyProjectSizeMetric:
    """
    Class to compute and store daily project size metrics.

    This class processes a DataFrame containing project size data with timestamps
    and calculates daily aggregated metrics. The results are stored in the `data`
    attribute as a DataFrame.

    Attributes:
        input_data (DataFrame): The input DataFrame containing project size data
            with a 'committed_datetime' column.
        data (DataFrame): The output DataFrame containing computed daily size metrics.

    """

    def __init__(self, input_data: DataFrame) -> None:
        """
        Initialize the DailyProjectSizeMetric with the provided input data.

        Args:
            input_data (DataFrame): A DataFrame containing project size metrics
                with a 'committed_datetime' column for each entry.

        """
        self.input_data = input_data
        self.data: DataFrame = DataFrame()

    def compute(self) -> None:
        """
        Compute the daily project size metrics based on the input data.

        This method groups the input data by day using the 'committed_datetime'
        column, calculates the sum of numeric columns for each day, and stores
        the results in the `data` attribute as a DataFrame with integer
        downcasting applied.

        The resulting DataFrame includes:
        - Aggregated size metrics for each day.
        - A 'date' column representing the date of each aggregated entry.

        """
        grouped_data_by_day: DataFrameGroupBy = self.input_data.groupby(
            by=Grouper(key="committed_datetime", freq="D")
        )

        daily_size_data: DataFrame = grouped_data_by_day.sum(numeric_only=True)
        daily_size_data = daily_size_data.apply(
            pd.to_numeric,
            downcast="integer",
        )
        daily_size_data["date"] = daily_size_data.index

        self.data = daily_size_data.reset_index(drop=True)


class ProjectProductivityMetric:
    """
    Class to compute productivity metrics for a project based on size data.

    This class processes a DataFrame containing project size data and calculates
    productivity metrics by analyzing changes in size metrics across different
    commits. The results are stored in the `data` attribute as a DataFrame.

    Attributes:
        size_table (DataFrame): The input DataFrame containing size data for
            each commit.
        data (DataFrame): The output DataFrame containing computed productivity
            metrics.

    """

    def __init__(self, size_table: DataFrame) -> None:
        """
        Initialize the ProjectProductivityMetric with the provided size data.

        Args:
            size_table (DataFrame): A DataFrame containing size metrics for each
                commit.

        """
        self.size_table: DataFrame = size_table
        self.data: DataFrame = DataFrame()

    def compute(self) -> None:
        """
        Compute the productivity metrics based on the size data.

        This method groups the size data by commit hash, calculates the sum of
        numeric columns for each group, and computes the differences between
        consecutive commits to determine the changes in size metrics. The
        results are stored in the `data` attribute as a DataFrame with integer
        downcasting applied.

        The resulting DataFrame includes:
        - Prefixed columns with "delta_" indicating changes in size metrics.
        - A "commit_hash_id" column representing the commit identifiers.

        """
        commit_groups: DataFrameGroupBy = self.size_table.groupby(
            by="commit_hash_id",
        )

        size: DataFrame = commit_groups.sum(numeric_only=True)
        size = size.reset_index(drop=True)
        print(size)
        input()

        size = size.add_prefix(prefix="delta_")
        size["commit_hash_id"] = self.size_table.index

        delta_size: DataFrame = size.diff().fillna(value=0)

        self.data = delta_size.apply(
            pd.to_numeric,
            downcast="integer",
        )


class DailyProjectProductivityMetric:
    """
    A class to compute and store daily project productivity metrics.

    This class processes a DataFrame containing daily project size data and
    calculates the changes in size metrics from one day to the next. The results
    are stored in the `data` attribute as a DataFrame.

    Attributes:
        daily_project_size_table (DataFrame): The input DataFrame containing
            daily project size data.
        data (DataFrame): The output DataFrame containing computed daily
            productivity metrics.

    """

    def __init__(self, daily_project_size_table: DataFrame) -> None:
        """
        Initialize the DailyProjectProductivityMetric.

        Args:
            daily_project_size_table (DataFrame): A DataFrame containing daily
                project size metrics.

        """
        self.daily_project_size_table: DataFrame = daily_project_size_table
        self.data: DataFrame = DataFrame()

    def compute(self) -> None:
        """
        Compute the daily project productivity metrics based on the size data.

        This method prefixes the columns of the input DataFrame with "delta_",
        calculates the differences between consecutive days to determine the
        changes in size metrics, and stores the results in the `data` attribute
        as a DataFrame.

        The resulting DataFrame includes:
        - Prefixed columns with "delta_" indicating changes in size metrics.
        - A "date" column representing the date of each entry.

        """
        size: DataFrame = self.daily_project_size_table.add_prefix(
            prefix="delta_",
        )

        delta_size: DataFrame = size.diff().fillna(0)
        delta_size = delta_size.drop(columns=["delta_date"])
        delta_size["date"] = self.daily_project_size_table["date"]

        self.data = delta_size


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
