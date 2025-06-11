"""
Utility DataFrame modification functions.

Copyright (C) 2025 Nicholas M. Synovic.

"""

from requests import Response, post

from json import dumps
from typing import Any

from pandas import DataFrame, Series
from progress.bar import Bar


def copy_dataframe_columns_to_dataframe(
    df: DataFrame,
    columns: list[str],
) -> DataFrame:
    """
    Create a new DataFrame containing a copy of specific columns.

    Args:
        df (DataFrame): The source pandas DataFrame.
        columns (list[str]): A list of column names to copy.

    Returns:
        DataFrame: A new DataFrame containing only the specified columns, copied
            from the original.

    """
    return df[columns].copy()


def remove_duplicate_dataframe_rows(df: DataFrame, column: str) -> DataFrame:
    """
    Remove duplicate rows from a DataFrame based on the values in a specified column.

    This function identifies and removes duplicate rows where the values in the
    specified column are identical.

    Args:
        df (DataFrame): The DataFrame to process.
        column (str): The name of the column to check for duplicates.

    Returns:
        DataFrame: A new DataFrame with duplicate rows removed, preserving the first
        occurrence.

    """
    return df.drop_duplicates(
        subset=column,
        keep="first",
        ignore_index=True,
    )


def copy_dataframe_cols_and_remove_duplicate_rows_by_col(
    df: DataFrame,
    keep_columns: list[str],
    unique_column: str,
) -> DataFrame:
    """
    Copy columns from a DataFrame and remove duplicate rows based on a column.

    This function creates a shallow copy of selected columns from the input DataFrame
    and removes any duplicate rows based on the values in the specified unique column.

    Args:
        df (DataFrame): The source pandas DataFrame.
        keep_columns (list[str]): List of column names to copy from the original
            DataFrame.
        unique_column (str): Column name to use for identifying and removing duplicate
            rows.

    Returns:
        DataFrame: A new DataFrame with the selected columns and unique rows based
        on `unique_column`.

    """
    df_copy: DataFrame = copy_dataframe_columns_to_dataframe(
        df=df, columns=keep_columns
    )
    return remove_duplicate_dataframe_rows(df=df_copy, column=unique_column)


def replace_dataframe_value_column_with_index_reference(
    df_1: DataFrame,
    df_2: DataFrame,
    df_1_col: str,
    df_2_col: str,
) -> DataFrame:
    """
    Replace values in a column of DataFrame df_1 with index reference from  df_2.

    This function iterates through the specified column of df_1 and replaces each value
    with the index of the corresponding value in df_2.

    Args:
        df_1 (DataFrame): The DataFrame to modify.
        df_2 (DataFrame): The DataFrame containing the index references.
        df_1_col (str): The name of the column in df_1 to modify.
        df_2_col (str): The name of the column in df_2 to use for index references.

    Returns:
        DataFrame: The modified DataFrame with values replaced by index references.

    """
    value_to_index = df_2.reset_index().set_index(df_2_col)["index"].to_dict()

    df_1 = df_1.copy()
    df_1[df_1_col] = df_1[df_1_col].map(value_to_index)
    return df_1


def replace_dataframe_value_column_with_index_reference_list(
    df_1: DataFrame,
    df_2: DataFrame,
    df_1_col: str,
    df_2_col: str,
) -> DataFrame:
    """
    Replace values in a column of DataFrame df_1 with index references from df_2.

    This function iterates through the specified column of df_1 and replaces each value
    with a dictionary containing the index of the corresponding value in df_2.

    Args:
        df_1 (DataFrame): The DataFrame to modify.
        df_2 (DataFrame): The DataFrame containing the index references.
        df_1_col (str): The name of the column in df_1 to modify.
        df_2_col (str): The name of the column in df_2 to use for index references.

    Returns:
        DataFrame: The modified DataFrame with values replaced by index references.

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

            df_1.at[idx, df_1_col] = new_list  # noqa: PD008
            bar.next()

    df_1[df_1_col] = df_1[df_1_col].apply(
        lambda x: ([{df_2_col: None}] if isinstance(x, list) and len(x) == 0 else x)
    )

    df_1[df_1_col] = df_1[df_1_col].apply(lambda x: dumps(obj=x))

    return df_1


def query_graphql(
    url: str,
    json_query: str,
    headers: dict[str, str],
    timeout: int = 60,
) -> Response:
    """
    Send a GraphQL query to the specified endpoint.

    Executes a POST request to the given GraphQL URL with the specified query,
    headers, and optional timeout. Returns the HTTP response.

    Args:
        url (str): The GraphQL endpoint URL.
        json_query (str): The GraphQL query string to send.
        headers (dict[str, str]): A dictionary of HTTP headers to include in the
            request.
        timeout (int, optional): Timeout for the request in seconds. Defaults to 60.

    Returns:
        Response: The HTTP response object from the request.

    """
    return post(
        url=url,
        json={"query": json_query},
        headers=headers,
        timeout=timeout,
    )
