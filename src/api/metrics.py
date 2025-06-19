"""
Classes to compute metrics.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import pandas as pd
from pandas import DataFrame, Grouper
from pandas.core.groupby import DataFrameGroupBy


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

        size: DataFrame = commit_groups.sum(numeric_only=True).add_prefix(
            prefix="delta_"
        )
        size["commit_hash_id"] = size.index

        delta_size: DataFrame = size.diff().fillna(value=0)

        self.data = delta_size.apply(
            pd.to_numeric,
            downcast="integer",
        )
