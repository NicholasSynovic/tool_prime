"""
Main entrypoint into the PRiMe application.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import sys
from math import ceil
from pathlib import Path
from typing import Any
from prime.api.metrics import Metric

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
)
from prime.api.utils import (
    copy_dataframe_columns_to_dataframe,
    replace_dataframe_value_column_with_index_reference,
)
from prime.api.vcs import VersionControlSystem, identify_vcs, parse_vcs
from prime.cli import CLI, get_first_namespace_key


def handle_db(namespace: dict[str, Any], namespace_key: str) -> DB | None:
    match namespace_key:
        case "vcs":
            return DB(db_path=namespace["vcs.output"])
        case "filesize":
            return DB(db_path=namespace["filesize.output"])
        case "project_size":
            return DB(db_path=namespace["project_size.output"])
        case "project_productivity":
            return DB(db_path=namespace["project_productivity.output"])
        case "bus_factor":
            return DB(db_path=namespace["bus_factor.output"])
        case "issues":
            return DB(db_path=namespace["issues.output"])
        case "pull_requests":
            return DB(db_path=namespace["pull_requests.output"])
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


def handle_metric(metric: Metric) -> None:
    metric.preprocess()
    metric.compute()
    metric.write()


def handle_filesize_per_commit(repo_path: Path, db: DB) -> None | bool:
    # Instantiate VCS class
    vcs: VersionControlSystem | int = identify_vcs(repo_path=repo_path)
    if isinstance(vcs, int):
        return False

    # Instantiate SCC class
    scc: SCC = SCC(directory=repo_path)

    # Compute size of each file per commit
    metric: FileSizePerCommit = FileSizePerCommit(vcs=vcs, scc=scc, db=db)
    handle_metric(metric=metric)


def handle_issues(namespace: dict[str, Any], db: DB) -> None:
    # TODO: Create a GH Pull Request Class in an dvcs.py file
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
    # TODO: Create a GH Pull Request Class in an dvcs.py file
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
        case "filesize":
            handle_filesize_per_commit(repo_path=namespace["filesize.input"], db=db)
        case "project_size":
            handle_metric(metric=ProjectSizePerCommit(db=db))
            handle_metric(metric=ProjectSizePerDay(db=db))
        case "project_productivity":
            handle_metric(metric=ProjectProductivityPerCommit(db=db))
            handle_metric(metric=ProjectProductivityPerDay(db=db))
        case "bus_factor":
            handle_metric(metric=BusFactorPerDay(db=db))
        case "issues":
            # TODO: Update this call to support generic calls to many DVCS
            handle_issues(namespace=namespace, db=db)
        case "pull_requests":
            # TODO: Update this call to support generic calls to many DVCS
            handle_pull_requests(namespace=namespace, db=db)
        case "issue_spoilage":
            handle_metric(metric=IssueSpoilagePerDay(db=db))
        case "issue_density":
            handle_metric(metric=IssueDensityPerDay(db=db))
        case _:
            sys.exit(3)


if __name__ == "__main__":
    main()
