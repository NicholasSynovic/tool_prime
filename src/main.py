"""
Main entrypoint into the PRiMe application.

Copyright (C) 2025 Nicholas M. Synovic.

"""

import sys
from pathlib import Path
from typing import Any
from progress.bar import Bar
from src.api.size import SCC
from pandas import Series
import pandas as pd

from pandas import DataFrame

from src.api.db import DB
from src.api.types import Authors, CommitHashes, CommitLog, Committers, Releases, Size
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


def handle_db(namespace: dict[str, Any], namespace_key: str) -> DB | None:
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

            data.append(
                scc_data.drop(
                    columns=["Provider", "Complexity", "ULOC"],
                )
            )
            bar.next()

    vcs.checkout_most_recent_revision()

    size_data: DataFrame = pd.concat(objs=data, ignore_index=True)
    size_data.columns = size_data.columns.str.lower()

    db.write_df(df=size_data, table="size", model=Size)


def handle_issues(namespace: dict[str, Any], db: DB) -> None:
    data: list[DataFrame] = []

    print(namespace)
    print(namespace["issues.owner"])
    print(namespace["issues.repo_name"])

    pass


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
        case _:
            sys.exit(3)


if __name__ == "__main__":
    main()
