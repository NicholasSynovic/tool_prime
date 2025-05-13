from json import dumps
from typing import Any, List

from pandas import DataFrame, Series
from progress.bar import Bar


def copyDFColumnsToDF(df: DataFrame, columns: List[str]) -> DataFrame:
    return df[columns].copy()


def removeDuplicateDFRows(df: DataFrame, column: str) -> None:
    """
    Keeps the first instance in a column.
    Edits `df` inplace.
    Replaces the index after dropping duplicate rows.
    """
    df.drop_duplicates(
        subset=column,
        keep="first",
        inplace=True,
        ignore_index=True,
    )


def copyDFColumnsAndRemoveDuplicateRowsByColumn(
    df: DataFrame, columns: List[str], checkColumn: str
) -> DataFrame:
    dfCopy: DataFrame = copyDFColumnsToDF(df=df, columns=columns)
    removeDuplicateDFRows(df=dfCopy, column=checkColumn)
    return dfCopy


def replaceDFValueInColumnWithIndexReference(
    df1: DataFrame,
    df2: DataFrame,
    df1Col: str,
    df2Col: str,
) -> DataFrame:
    """
    Replaces the values in DataFrame `df1` in column `df1Col` by the index of the values in DataFrame `df2` in column `df2Col`. # noqa: E501

    Returns DataFrame `df1`
    """
    value_to_index = df2.reset_index().set_index(df2Col)["index"].to_dict()

    df1 = df1.copy()
    df1[df1Col] = df1[df1Col].map(value_to_index)
    return df1


def replaceDFValueInColumnWithListOfIndexReferences(
    df1: DataFrame,
    df2: DataFrame,
    df1Col: str,
    df2Col: str,
) -> DataFrame:
    """
    Replace the values stored in a List in DataFrame `df1` in column `df1Col` by the index of the values in DataFrame `df2` in column `df2Col`. # noqa: E501

    Returns DataFrame `df1`
    """
    value_to_index = df2.reset_index().set_index(df2Col)["index"].to_dict()
    df1 = df1.copy()

    with Bar(f"Updating values in ''{df1Col}''...", max=df1.shape[0]) as bar:
        idx: int
        row: Series
        for idx, row in df1.iterrows():
            value_list: List[Any] = row[df1Col]
            new_list: List[dict[str, any]] = []

            for value in value_list:
                replacement_index = value_to_index.get(value)
                new_list.append({df2Col: replacement_index})

            df1.at[idx, df1Col] = new_list
            bar.next()

    df1[df1Col] = df1[df1Col].apply(
        lambda x: (
            [{df2Col: None}] if isinstance(x, list) and len(x) == 0 else x
        )  # noqa: E501
    )

    df1[df1Col] = df1[df1Col].apply(lambda x: dumps(obj=x))

    return df1
