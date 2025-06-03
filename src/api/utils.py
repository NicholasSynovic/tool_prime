from json import dumps
from typing import Any

from pandas import DataFrame, Series
from progress.bar import Bar


def copyDFColumnsToDF(df: DataFrame, columns: list[str]) -> DataFrame:
    return df[columns].copy()


def removeDuplicateDFRows(df: DataFrame, column: str) -> DataFrame:
    """
    Keeps the first instance in a column.

    Replaces the index after dropping duplicate rows.

    Returns DataFrame `df`

    """
    return df.drop_duplicates(
        subset=column,
        keep="first",
        ignore_index=True,
    )


def copyDFColumnsAndRemoveDuplicateRowsByColumn(
    df: DataFrame, columns: list[str], checkColumn: str
) -> DataFrame:
    dfCopy: DataFrame = copyDFColumnsToDF(df=df, columns=columns)
    removeDuplicateDFRows(df=dfCopy, column=checkColumn)
    return dfCopy


def replaceDFValueInColumnWithIndexReference(
    df_1: DataFrame,
    df_2: DataFrame,
    df_1_col: str,
    df_2_col: str,
) -> DataFrame:
    """
    Replaces the values in DataFrame `df_1` in column `df_1_col` by the index of
    the values in DataFrame `df_2` in column `df_2_col`.

    Returns DataFrame `df_1`

    """
    value_to_index = df_2.reset_index().set_index(df_2_col)["index"].to_dict()

    df_1 = df_1.copy()
    df_1[df_1_col] = df_1[df_1_col].map(value_to_index)
    return df_1


def replaceDFValueInColumnWithlistOfIndexReferences(
    df_1: DataFrame,
    df_2: DataFrame,
    df_1_col: str,
    df_2_col: str,
) -> DataFrame:
    """
    Replace the values stored in a list in DataFrame `df_1` in column `df_1_col`
    by the index of the values in DataFrame `df_2` in column `df_2_col`.

    Returns DataFrame `df_1`

    """
    value_to_index = df_2.reset_index().set_index(df_2_col)["index"].to_dict()
    df_1 = df_1.copy()

    idx: int
    row: Series
    with Bar(f"Updating values in ''{df_1_col}''...", max=df_1.shape[0]) as bar:
        for idx, row in df_1.iterrows():
            value_list: list[Any] = row[df_1_col]
            new_list: list[dict[str, Any]] = []

            for value in value_list:
                replacement_index = value_to_index.get(value)
                new_list.append({df_2_col: replacement_index})

            df_1.at[idx, df_1_col] = new_list
            bar.next()

    df_1[df_1_col] = df_1[df_1_col].apply(
        lambda x: ([{df_2_col: None}] if isinstance(x, list) and len(x) == 0 else x)
    )

    df_1[df_1_col] = df_1[df_1_col].apply(lambda x: dumps(obj=x))

    return df_1
