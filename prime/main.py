"""
Main entrypoint into the PRiMe application.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import sys
from math import ceil
from pathlib import Path
from typing import Any

import pandas as pd
from pandas import DataFrame, IntervalIndex, Timestamp
from progress.bar import Bar

from prime.api.db import DB
from prime.api.issues import GitHubIssues
from prime.api.metrics import (
    BusFactorPerDay,
    FileSizePerCommit,
    IssueDensityPerDay,
    IssueSpoilagePerDay,
    ProjectProductivityPerCommit,
    ProjectProductivityPerDay,
    ProjectSizePerCommit,
    ProjectSizePerDay,
)
from prime.api.pull_requests import GitHubPullRequests
from prime.api.size import SCC
from prime.api.types import (
    Authors,
    CommitHashes,
    CommitLog,
    Committers,
    IssueIDs,
    Issues,
    PullRequestIDs,
    PullRequests,
    Releases,
    T_BusFactorPerDay,
    T_FileSizePerCommit,
    T_IssueDensityPerDay,
    T_IssueSpoilagePerDay,
    T_ProjectProductivityPerCommit,
    T_ProjectProductivityPerDay,
    T_ProjectSizePerCommit,
    T_ProjectSizePerDay,
)
from prime.api.utils import (
    copy_dataframe_columns_to_dataframe,
    replace_dataframe_value_column_with_index_reference,
)
from prime.api.vcs import VersionControlSystem, identify_vcs, parse_vcs
from prime.cli import CLI, get_first_namespace_key


def handle_db(namespace: dict[str, Any], namespace_key: str) -> DB | None:
    """
    Handle database initialization based on the provided namespace key.

    This function matches the `namespace_key` to predefined cases and
    initializes a database object using the corresponding output path from the
    `namespace` dictionary. If the `namespace_key` does not match any predefined
    case, it returns None.

    Args:
        namespace (dict[str, Any]): The dictionary containing namespace keys and
            values.
        namespace_key (str): The key indicating which database path to use.

    Returns:
        DB | None: A DB object initialized with the appropriate path, or None if
            the `namespace_key` does not match any case.

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
        case "project_productivity":
            return DB(db_path=namespace["project_productivity.output"])
        case "bus_factor":
            return DB(db_path=namespace["bus_factor.output"])
        case "issue_spoilage":
            return DB(db_path=namespace["issue_spoilage.output"])
        case "issue_density":
            return DB(db_path=namespace["issue_density.output"])
        case _:
            return None


def handle_vcs(namespace: dict[str, Any], db: DB) -> bool:
    """
    Handle version control system parsing and database operations.

    This function identifies the version control system of a given repository,
    parses it for revisions, and writes the parsed data into the database.
    If the version control system is invalid, the function returns False.

    Args:
        namespace (dict[str, Any]): The dictionary containing namespace keys
            and values.
        db (DB): The database object used for reading and writing data.

    Returns:
        bool: True if the VCS parsing and database operations are successful,
            False otherwise.

    """
    # Get the commits that have already been stored
    existing_commits_df: DataFrame = db.read_table(
        table="commit_hashes",
        model=CommitHashes,
    )

    # Identify the VCS. If invalid, return False
    vcs: VersionControlSystem | int = identify_vcs(
        repo_path=namespace["vcs.input"],
    )
    if isinstance(vcs, int):
        return False

    # Parse VCS for revisions
    data: dict[str, DataFrame] = parse_vcs(
        vcs=vcs,
        previous_revisions=existing_commits_df,
    )

    # Write tables
    db.write_df(
        df=data["commit_hashes"],
        table="commit_hashes",
        model=CommitHashes,
    )
    db.write_df(df=data["authors"], table="authors", model=Authors)
    db.write_df(df=data["committers"], table="committers", model=Committers)
    db.write_df(df=data["commit_logs"], table="commit_logs", model=CommitLog)
    db.write_df(df=data["releases"], table="releases", model=Releases)

    return True


def handle_size(namespace: dict[str, Any], db: DB) -> bool:
    # Instantiate VCS class
    vcs: VersionControlSystem | int = identify_vcs(
        repo_path=namespace["size.input"],
    )
    if isinstance(vcs, int):
        return False

    # Instantiate SCC class
    scc: SCC = SCC(directory=vcs.repo_path)

    # Get commit hashes from the database to parse through
    commit_hashes: DataFrame = db.read_table(
        table="commit_hashes",
        model=CommitHashes,
    )

    # Compute size of each file per commit
    fspc: FileSizePerCommit = FileSizePerCommit(
        vcs=vcs,
        scc=scc,
        commit_hashes=commit_hashes,
    )
    fspc.compute()

    # Write file size per commit to the database
    db.write_df(
        df=fspc.computed_data,
        table="file_size_per_commit",
        model=T_FileSizePerCommit,
    )

    # Compute size of the project per commit
    pspc: ProjectSizePerCommit = ProjectSizePerCommit(
        file_sizes=fspc.computed_data,
    )
    pspc.compute()

    # Write project size per commit to the database
    db.write_df(
        df=pspc.computed_data,
        table="project_size_per_commit",
        model=T_ProjectSizePerCommit,
    )

    # Project size per day requires datetimes of commits
    sql: str = "SELECT id, commit_hash_id, committed_datetime FROM commit_logs"
    commit_datetimes: DataFrame = db.query_database(sql=sql)
    commit_datetimes["committed_datetime"] = commit_datetimes[
        "committed_datetime"
    ].apply(lambda x: Timestamp(ts_input=x, tz="UTC").floor(freq="D"))

    # Join commit datetimes with project size per commit
    pspd_input_data: DataFrame = pspc.computed_data.copy()
    pspd_input_data = pspd_input_data.merge(
        commit_datetimes[["commit_hash_id", "committed_datetime"]],
        on="commit_hash_id",
        how="left",
    )

    # Compute project size per day
    pspd: ProjectSizePerDay = ProjectSizePerDay(input_data=pspd_input_data)
    pspd.compute()

    # Write metrics to the database

    db.write_df(
        df=pspd.computed_data,
        table="project_size_per_day",
        model=T_ProjectSizePerDay,
    )

    return True


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


def handle_project_productivity(db: DB) -> None:
    # Get project size per commit
    project_size_per_commit: DataFrame = db.read_table(
        table="project_size_per_commit",
        model=T_ProjectSizePerCommit,
    )

    # Compute project productivity per commit
    pppc: ProjectProductivityPerCommit = ProjectProductivityPerCommit(
        project_size_per_commit=project_size_per_commit
    )
    pppc.compute()

    # Project productivity per day requires datetimes of commits
    sql: str = "SELECT id, commit_hash_id, committed_datetime FROM commit_logs"
    commit_datetimes: DataFrame = db.query_database(sql=sql)
    commit_datetimes["committed_datetime"] = commit_datetimes[
        "committed_datetime"
    ].apply(lambda x: Timestamp(ts_input=x))

    # Join commit datetimes with project size per commit
    pppd_input_data: DataFrame = pppc.computed_data.copy()
    pppd_input_data = pppd_input_data.merge(
        commit_datetimes[["commit_hash_id", "committed_datetime"]],
        on="commit_hash_id",
        how="left",
    )

    # Compute project productivity per day
    pppd: ProjectProductivityPerDay = ProjectProductivityPerDay(
        input_data=pppd_input_data,
    )
    pppd.compute()

    # Write metrics to the database
    db.write_df(
        df=pppc.computed_data,
        table="project_productivity_per_commit",
        model=T_ProjectProductivityPerCommit,
    )

    db.write_df(
        df=pppd.computed_data,
        table="project_productivity_per_day",
        model=T_ProjectProductivityPerDay,
    )


def handle_bus_factor(db: DB) -> None:
    # Get project productivity per commit
    project_productivity_per_commit: DataFrame = db.read_table(
        table="project_productivity_per_commit",
        model=T_ProjectProductivityPerCommit,
    )

    # Bus factor per day requires datetimes of commits and the committer id
    sql: str = (
        "SELECT id, commit_hash_id, committed_datetime, committer_id FROM commit_logs"
    )
    commit_datetimes: DataFrame = db.query_database(sql=sql)
    commit_datetimes["committed_datetime"] = commit_datetimes[
        "committed_datetime"
    ].apply(lambda x: Timestamp(ts_input=x))

    # Join commit datetimes with project size per commit
    bfpd_input_data: DataFrame = project_productivity_per_commit.merge(
        commit_datetimes[["commit_hash_id", "committed_datetime", "committer_id"]],
        on="commit_hash_id",
        how="left",
    )
    bfpd_input_data = bfpd_input_data.drop(columns="commit_hash_id")

    bfpd: BusFactorPerDay = BusFactorPerDay(input_data=bfpd_input_data)
    bfpd.compute()

    # Write metric to database
    db.write_df(
        df=bfpd.computed_data,
        table="bus_factor",
        model=T_BusFactorPerDay,
    )


def handle_issue_spoilage(db: DB) -> None:
    # SQL to get the smallest date from VCS
    sql: str = "SELECT id, MIN(date) as date FROM project_size_per_day;"

    # Get current day
    current_date: Timestamp = Timestamp.utcnow().floor(freq="D")

    # Get all valid dates from the VCS; issues can't be created before the
    # project is created
    vcs_dates: DataFrame = db.query_database(sql=sql)
    oldest_date: Timestamp = Timestamp(
        ts_input=vcs_dates["date"][0],
        tz="UTC",
    ).floor(freq="D")

    # Create daily time intervals from 00:00:00 -> 23:59:59
    daily_intervals: IntervalIndex = IntervalIndex.from_arrays(
        left=pd.date_range(start=oldest_date, end=current_date, freq="D"),
        right=pd.date_range(start=oldest_date, end=current_date, freq="D")
        + pd.Timedelta(
            hours=23,
            minutes=59,
            seconds=59,
        ),
        closed="both",
    )[0:-1]

    # Get issues and set created_at and closed_at to pandas.Timestamp
    issues: DataFrame = db.read_table(table="issues", model=Issues)
    issues["created_at"] = issues["created_at"].apply(
        lambda x: Timestamp(ts_input=x, tz="UTC").floor(freq="D"),
    )
    issues["closed_at"] = issues["closed_at"].apply(
        lambda x: Timestamp(ts_input=x, tz="UTC").floor(freq="D"),
    )
    issues["closed_at"] = issues["closed_at"].fillna(value=current_date)
    issues["interval"] = issues.apply(
        lambda row: pd.Interval(
            left=row["created_at"], right=row["closed_at"], closed="both"
        ),
        axis=1,
    )

    ispd: IssueSpoilagePerDay = IssueSpoilagePerDay(
        daily_intervals=daily_intervals,
        input_data=issues,
    )
    ispd.compute()

    # Write metric to database
    db.write_df(
        df=ispd.computed_data,
        table="issue_spoilage_per_day",
        model=T_IssueSpoilagePerDay,
    )


def handle_issue_density(db: DB) -> None:
    # Get issue spoilage per day
    issue_spoilage_per_day: DataFrame = db.read_table(
        table="issue_spoilage_per_day",
        model=T_IssueSpoilagePerDay,
    )

    # Get project size per day
    project_size_per_day: DataFrame = db.read_table(
        table="project_size_per_day",
        model=T_ProjectSizePerDay,
    )

    # Set dates to be Timestamps
    issue_spoilage_per_day["start"] = issue_spoilage_per_day["start"].apply(
        lambda x: Timestamp(ts_input=x, tz="UTC"),
    )
    issue_spoilage_per_day["end"] = issue_spoilage_per_day["end"].apply(
        lambda x: Timestamp(ts_input=x, tz="UTC"),
    )

    project_size_per_day["date"] = project_size_per_day["date"].apply(
        lambda x: Timestamp(ts_input=x, tz="UTC").floor(freq="D"),
    )

    idpd: IssueDensityPerDay = IssueDensityPerDay(
        issue_spoilage_per_day=issue_spoilage_per_day,
        project_size_per_day=project_size_per_day,
    )
    idpd.compute()

    # Write metric to database
    db.write_df(
        df=idpd.computed_data,
        table="issue_density_per_day",
        model=T_IssueDensityPerDay,
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
    db: DB | None = handle_db(namespace=namespace, namespace_key=namespace_key)
    if db is None:
        sys.exit(2)

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
        case "project_productivity":
            handle_project_productivity(db=db)
        case "bus_factor":
            handle_bus_factor(db=db)
        case "issue_spoilage":
            handle_issue_spoilage(db=db)
        case "issue_density":
            handle_issue_density(db=db)
        case _:
            sys.exit(3)


if __name__ == "__main__":
    main()
