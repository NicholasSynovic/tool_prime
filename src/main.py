"""
Main entrypoint into the PRiMe application.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import sys
from math import ceil
from pathlib import Path
from typing import Any

import pandas as pd
from pandas import DataFrame, Series, Timestamp
from progress.bar import Bar

from src.api.db import DB
from src.api.issues import GitHubIssues
from src.api.metrics import (
    DailyProjectProductivityMetric,
    DailyProjectSizeMetric,
    ProjectProductivityMetric,
    ProjectSizeMetric,
)
from src.api.pull_requests import GitHubPullRequests
from src.api.size import SCC
from src.api.types import (
    Authors,
    CommitHashes,
    CommitLog,
    Committers,
    DailyProjectProductivity,
    DailyProjectSize,
    IssueIDs,
    Issues,
    ProjectProductivity,
    ProjectSize,
    PullRequestIDs,
    PullRequests,
    Releases,
    Size,
)
from src.api.utils import (
    copy_dataframe_columns_to_dataframe,
    replace_dataframe_value_column_with_index_reference,
)
from src.api.vcs import VersionControlSystem, identify_vcs, parse_vcs
from src.cli import CLI


def get_first_namespace_key(namespace: dict[str, Any]) -> str:
    """
    Return a top-level key prefix from a namespaced dictionary.

    This function assumes that keys in the dictionary follow a dot-separated namespace
    format (e.g., "user.name", "config.value"). It splits each key at the first
    dot and collects the first segment, then returns one arbitrary unique prefix
    from the resulting set.

    Args:
        namespace (dict[str, Any]): A dictionary with dot-separated string keys.

    Returns:
        str: A single unique top-level key prefix extracted from the keys.

    """
    return {key.split(".")[0] for key in namespace}.pop()


def handle_db(namespace: dict[str, Any], namespace_key: str) -> DB | None:  # noqa: PLR0911
    """
    Connect to the database based on the provided namespace key.

    This function establishes a database connection using the information
    extracted from the command-line namespace.

    Args:
        namespace: A dictionary containing command-line arguments.
        namespace_key: The key indicating the type of database connection required
            (e.g., "vcs", "size").

    Returns:
        A DB object representing the established database connection, or None
        if no suitable connection can be created.

    """
    match namespace_key:
        case "vcs":
            return DB(db_path=namespace["vcs.output"])
        case "size":
            return DB(db_path=namespace["size.output"])
        case "issues":
            return DB(db_path=namespace["issues.output"])
        case "pull_requests":
            return DB(db_path=namespace["pull_requests.output"])
        case "project_size":
            return DB(db_path=namespace["project_size.output"])
        case "project_productivity":
            return DB(db_path=namespace["project_productivity.output"])
        case _:
            return None


def handle_vcs(namespace: dict[str, Any], db: DB) -> None:
    """
    Process VCS data and store it in the database.

    This function retrieves VCS data from a specified repository,
    processes it, and stores the results in the database.

    Args:
        namespace: A dictionary containing command-line arguments.
        db: A DB object representing the database connection.

    """
    existing_commits_df: DataFrame = db.read_table(
        table="commit_hashes",
        model=CommitHashes,
    )

    repository_path: Path = Path(namespace["vcs.input"]).resolve()
    vcs: VersionControlSystem | int = identify_vcs(repo_path=repository_path)
    if vcs == -1:
        sys.exit(2)

    data: dict[str, DataFrame] = parse_vcs(
        vcs=vcs,
        previous_revisions=existing_commits_df,
    )

    db.write_df(
        df=data["commit_hashes"],
        table="commit_hashes",
        model=CommitHashes,
    )
    db.write_df(df=data["authors"], table="authors", model=Authors)
    db.write_df(df=data["committers"], table="committers", model=Committers)
    db.write_df(df=data["commit_logs"], table="commit_logs", model=CommitLog)
    db.write_df(df=data["releases"], table="releases", model=Releases)


def handle_size(namespace: dict[str, Any], db: DB) -> None:
    """
    Compute and store repository size measured in lines of code into a database.

    This function calculates the size of a repository based on lines of code
    and stores the result in the database.

    Args:
      namespace: A dictionary containing command-line arguments.
      db: A DB object representing the database connection.

    """
    data: list[DataFrame] = []

    repo_path: Path = Path(namespace["size.input"]).resolve()
    vcs: VersionControlSystem = identify_vcs(repo_path=repo_path)
    if vcs == -1:
        sys.exit(2)

    scc: SCC = SCC(directory=vcs.repo_path)

    commits_df: DataFrame = db.read_table(
        table="commit_hashes",
        model=CommitHashes,
    )

    idx: int
    row: Series
    with Bar("Computing size per commit...", max=commits_df.shape[0]) as bar:
        for idx, row in commits_df.iterrows():
            commit_hash: str = row["commit_hash"]

            vcs.checkout_revision(revision_hash=commit_hash)

            scc_data: DataFrame = scc.run()
            scc_data["commit_hash_id"] = idx
            scc_data = scc_data.drop(columns=["Filename", "Complexity", "ULOC"])

            data.append(scc_data)
            bar.next()

    vcs.checkout_most_recent_revision()

    size_data: DataFrame = pd.concat(objs=data, ignore_index=True)
    size_data.columns = size_data.columns.str.lower()

    db.write_df(df=size_data, table="size", model=Size)


def handle_issues(namespace: dict[str, Any], db: DB) -> None:
    """
    Retrieve all issues for a given repository and concatenate them into a DataFrame.

    This function initializes a `GitHubIssues` instance using the parameters provided
    in `namespace`, then iteratively queries GitHub's GraphQL API for issues in pages
    of 100 until all are retrieved. A progress bar is displayed during the process.
    The resulting issues are combined into a single DataFrame a single DataFrame
    and stored in `db.pull_requests`.

    Args:
        namespace (dict[str, Any]): A dictionary containing required keys:
            - "issues.owner": Owner of the GitHub repository.
            - "issues.repo_name": Name of the repository.
            - "issues.auth": Authentication token or API key for GitHub access.
        db (DB): A database interface or object.

    """
    data: list[DataFrame] = []

    ghi: GitHubIssues = GitHubIssues(
        owner=namespace["issues.owner"],
        repo_name=namespace["issues.repo_name"],
        auth_key=namespace["issues.auth"],
    )

    total_issues: int = ghi.get_total_issues()
    total_pages: int = ceil(total_issues / 100)
    has_next_page: bool = True
    after_cursor: str = "null"
    with Bar("Getting issues from GitHub...", max=total_pages) as bar:
        while has_next_page:
            resp: tuple[DataFrame, str, bool] = ghi.get_issues(
                after_cursor=after_cursor,
            )

            data.append(resp[0])
            after_cursor = f'"{resp[1]}"'
            has_next_page = resp[2]

            bar.next()

    issue_data: DataFrame = pd.concat(objs=data, ignore_index=True)

    issue_ids: DataFrame = copy_dataframe_columns_to_dataframe(
        df=issue_data, columns=["issue_id"]
    )

    issue_data = replace_dataframe_value_column_with_index_reference(
        df_1=issue_data,
        df_2=issue_ids,
        df_1_col="issue_id",
        df_2_col="issue_id",
    )

    issue_data = issue_data.rename(columns={"issue_id": "issue_id_key"})

    db.write_df(df=issue_ids, table="issue_ids", model=IssueIDs)
    db.write_df(df=issue_data, table="issues", model=Issues)


def handle_pull_requests(namespace: dict[str, Any], db: DB) -> None:
    """
    Retrieve all issues for a given repository and concatenate them into a DataFrame.

    This function initializes a `GitHubPullRequests` instance using the parameters
    provided in `namespace`, then iteratively queries GitHub's GraphQL API for
    pull requests in pages of 100 until all are retrieved. A progress bar is
    displayed during the process. The resulting pull requests are combined into
    a single DataFrame and stored in `db.pull_requests`.

    Args:
        namespace (dict[str, Any]): A dictionary containing required keys:
            - "pull_requests.owner": Owner of the GitHub repository.
            - "pull_requests.repo_name": Name of the repository.
            - "pull_requests.auth": Authentication token or API key for GitHub access.
        db (DB): A database interface or object.

    """
    data: list[DataFrame] = []

    ghpr: GitHubPullRequests = GitHubPullRequests(
        owner=namespace["pull_requests.owner"],
        repo_name=namespace["pull_requests.repo_name"],
        auth_key=namespace["pull_requests.auth"],
    )

    total_pull_requests: int = ghpr.get_total_pull_requests()
    total_pages: int = ceil(total_pull_requests / 100)
    has_next_page: bool = True
    after_cursor: str = "null"
    with Bar("Getting pull requests from GitHub...", max=total_pages) as bar:
        while has_next_page:
            resp: tuple[DataFrame, str, bool] = ghpr.get_pull_requests(
                after_cursor=after_cursor,
            )

            data.append(resp[0])
            after_cursor = f'"{resp[1]}"'
            has_next_page = resp[2]

            bar.next()

    pull_requests_data: DataFrame = pd.concat(objs=data, ignore_index=True)

    pull_request_ids: DataFrame = copy_dataframe_columns_to_dataframe(
        df=pull_requests_data, columns=["pull_request_id"]
    )

    pull_requests_data = replace_dataframe_value_column_with_index_reference(
        df_1=pull_requests_data,
        df_2=pull_request_ids,
        df_1_col="pull_request_id",
        df_2_col="pull_request_id",
    )

    pull_requests_data = pull_requests_data.rename(
        columns={"pull_request_id": "pull_request_id_key"}
    )

    db.write_df(df=pull_request_ids, table="pull_request_ids", model=PullRequestIDs)
    db.write_df(df=pull_requests_data, table="pull_requests", model=PullRequests)


def handle_project_size(db: DB) -> None:
    """
    Handle project size metrics both per commit and per day.

    This function performs the following operations:
    1. Executes an SQL query to retrieve daily project size data, including
        committed datetime, by joining `commit_logs` and `project_size` tables.
    2. Computes the project size per commit using the `ProjectSizeMetric` class,
        which processes data from the `size` table.
    3. Writes the computed project size per commit data back to the database in
        the `project_size` table.
    4. Retrieves daily project size data using the SQL query and processes the
        `committed_datetime` column to convert it into `Timestamp` objects.
    5. Computes the project size per day using the `DailyProjectSizeMetric`
        class, which processes the input data derived from the SQL query.
    6. Writes the computed project size per day data back to the database in the
        `daily_project_size` table.

    Args:
        db (DB): An instance of the database connection object used to execute
            queries and write data.

    """
    # SQL query to get input daily project size
    sql_query: str = """
    SELECT
        ps.*,
        c.committed_datetime
    FROM
        commit_logs c
    JOIN
        project_size ps ON c.commit_hash_id = ps.commit_hash_id;
    """

    # Compute size per commit
    size_table: DataFrame = db.read_table(table="size", model=Size)
    project_size: ProjectSizeMetric = ProjectSizeMetric(size_table=size_table)
    project_size.compute()

    # Write size per commit to DB
    db.write_df(df=project_size.data, table="project_size", model=ProjectSize)

    # Get daily project size data
    daily_project_size_input: DataFrame = db.query_database(sql=sql_query)
    daily_project_size_input["committed_datetime"] = daily_project_size_input[
        "committed_datetime"
    ].apply(lambda x: Timestamp(ts_input=x))
    daily_project_size_input = daily_project_size_input.drop(columns="commit_hash_id")

    # Compute size per day
    daily_project_size: DailyProjectSizeMetric = DailyProjectSizeMetric(
        input_data=daily_project_size_input
    )
    daily_project_size.compute()

    # Write size per day to DB
    db.write_df(
        df=daily_project_size.data, table="daily_project_size", model=DailyProjectSize
    )


def handle_project_productivity(db: DB) -> None:
    """
    Handle the computation and storage of project productivity metrics.

    This function reads the 'size' table from the database, computes the project
    productivity metrics, and writes the results back to the
    'project_productivity' table in the database.

    The function performs the following steps:
    1. Reads the 'size' table from the database using the specified model
        `Size`.
    2. Initializes a `ProjectProductivityMetric` object with the data from the
        'size' table.
    3. Computes the project productivity metrics using the `compute` method of
        `ProjectProductivityMetric`.
    4. Writes the computed metrics to the 'project_productivity' table in the
        database using the specified model `ProjectProductivity`.

    Args:
        db (DB): The database connection object used to read from and write to
            the database.

    """
    # Compute productivity per commit
    size_table: DataFrame = db.read_table(table="size", model=Size)
    project_productivity: ProjectProductivityMetric = ProjectProductivityMetric(
        size_table=size_table,
    )
    project_productivity.compute()

    # Compute daily productivity
    daily_size_table: DataFrame = db.read_table(
        table="daily_project_size",
        model=DailyProjectSize,
    )

    daily_project_productivity: DailyProjectProductivityMetric = (
        DailyProjectProductivityMetric(daily_project_size_table=daily_size_table)
    )
    daily_project_productivity.compute()

    db.write_df(
        df=project_productivity.data,
        table="project_productivity",
        model=ProjectProductivity,
    )

    db.write_df(
        df=daily_project_productivity.data,
        table="daily_project_productivity",
        model=DailyProjectProductivity,
    )


def main() -> None:
    """
    Execute the application based on command-line arguments.

    This function parses command-line arguments, connects to a database,
    and then executes specific subroutines based on the parsed arguments.

    Args:
        None

    """
    cli: CLI = CLI()
    namespace: dict[str, Any] = cli.parse_args().__dict__
    try:
        namespace_key: str = get_first_namespace_key(namespace=namespace)
    except KeyError:
        sys.exit(1)

    # Connect to database
    db: DB = handle_db(namespace=namespace, namespace_key=namespace_key)

    # Run subroutines based on command line parser
    match namespace_key:
        case "vcs":
            handle_vcs(namespace=namespace, db=db)
        case "size":
            handle_size(namespace=namespace, db=db)
        case "issues":
            handle_issues(namespace=namespace, db=db)
        case "pull_requests":
            handle_pull_requests(namespace=namespace, db=db)
        case "project_size":
            handle_project_size(db=db)
        case "project_productivity":
            handle_project_productivity(db=db)
        case _:
            sys.exit(3)


if __name__ == "__main__":
    main()
