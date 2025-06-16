from pandas import DataFrame
from pandas.core.groupby import DataFrameGroupBy
import pandas as pd


class ProjectSizeMetric:
    def __init__(self, size_table: DataFrame) -> None:
        self.size_table: DataFrame = size_table
        self.data: DataFrame = DataFrame()

    def compute(self) -> None:
        commit_groups: DataFrameGroupBy = self.size_table.groupby(
            by="commit_hash_id",
        )
        size: DataFrame = commit_groups.sum(numeric_only=True)
        size["commit_hash_id"] = size.index.to_list()
        self.data = size.apply(pd.to_numeric, downcast="integer")


class ProjectActivityMetric:
    def __init__(self, size_table: DataFrame) -> None:
        self.size_table: DataFrame = size_table
        self.data: DataFrame = DataFrame()

    def compute(self) -> None:
        commit_groups: DataFrameGroupBy = self.size_table.groupby(
            by="commit_hash_id",
        )
        size: DataFrame = commit_groups.sum(numeric_only=True)
        size["commit_hash_id"] = size.index.to_list()
        delta_size: DataFrame = size.diff().fillna(value=0)
        delta_size_int: DataFrame = delta_size.apply(
            pd.to_numeric,
            downcast="integer",
        )
        self.data = delta_size_int.add_prefix(prefix="delta_")
