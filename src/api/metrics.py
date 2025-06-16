from pandas import DataFrame
from pandas.core.groupby import DataFrameGroupBy
from src.api.db import DB
from pathlib import Path
from src.api.types import Size
import pandas as pd


class CodeSize:
    def __init__(self, size_table: DataFrame) -> None:
        self.size_table: DataFrame = size_table
        self.data: DataFrame = DataFrame()

    def compute(self) -> None:
        commit_groups: DataFrameGroupBy = self.size_table.groupby(
            by="commit_hash_id",
        )
        size: DataFrame = commit_groups.sum(numeric_only=True)
        self.data = size.apply(pd.to_numeric, downcast="integer")


class DeltaCodeSize:
    def __init__(self, size_table: DataFrame) -> None:
        self.size_table: DataFrame = size_table
        self.data: DataFrame = DataFrame()

    def compute(self) -> None:
        commit_groups: DataFrameGroupBy = self.size_table.groupby(
            by="commit_hash_id",
        )
        size_per_commit: DataFrame = commit_groups.sum(numeric_only=True)
        delta_size: DataFrame = size_per_commit.diff().fillna(value=0)
        delta_size_int: DataFrame = delta_size.apply(pd.to_numeric, downcast="integer")
        self.data = delta_size_int.add_prefix(prefix="delta_")
