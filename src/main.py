import sys
from pathlib import Path
from typing import Any, Iterator, List, Tuple

from git import Commit
from pandas import DataFrame

from src.api.db import DB
from src.api.vcs import Git
from src.cli import CLI


def _copyDFColumns(df: DataFrame, columns: List[str]) -> DataFrame:
    return df[columns].copy()


def _removeDuplicateDFRows(df: DataFrame, column: str) -> None:
    """
    Edits DataFrame in place
    """
    df.sort_values(by=column, inplace=True)
    df.drop_duplicates(
        subset=column,
        keep="first",
        inplace=True,
        ignore_index=True,
    )


def git(repo: Path) -> DataFrame:
    git: Git = Git(repo_path=repo)
    revisions: Tuple[Iterator[Commit], int] = git.get_revisions()
    return git.parse_revisions(revisions=revisions)


def storeRevisionDF(df: DataFrame, db: DB) -> None:
    commitHashes: DataFrame = _copyDFColumns(
        df=df,
        columns=["commit_hash"],
    )
    authors: DataFrame = _copyDFColumns(
        df=df,
        columns=["author", "author_email"],
    )
    committers: DataFrame = _copyDFColumns(
        df=df,
        columns=["committer", "committer_email"],
    )

    _removeDuplicateDFRows(df=authors, column="author_email")
    _removeDuplicateDFRows(df=committers, column="committer_email")

    db.write_df(df=commitHashes, table="commit_hashes")
    db.write_df(df=authors, table="authors")
    db.write_df(df=committers, table="committers")


def main() -> None:
    cli: CLI = CLI()
    ns: dict[str, Any] = cli.parse_args().__dict__

    nsKey: str = set([key.split(".")[0] for key in ns.keys()]).pop()

    db: DB
    # Extract/compute data to a DataFrame
    match nsKey:
        case "vcs":
            db = DB(db_path=ns["vcs.output"])
            df: DataFrame = git(repo=ns["vcs.input"])
        case _:
            sys.exit(1)

    # Store DataFrame(s) into DB
    match nsKey:
        case "vcs":
            storeRevisionDF(df=df, db=db)
        case _:
            sys.exit(1)


if __name__ == "__main__":
    main()
