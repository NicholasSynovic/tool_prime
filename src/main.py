import sys
from pathlib import Path
from typing import Any, Iterator, Tuple

from git import Commit
from pandas import DataFrame

from src.api.db import DB
from src.api.vcs import Git
from src.cli import CLI


def git(repo: Path) -> DataFrame:
    git: Git = Git(repo_path=repo)
    revisions: Tuple[Iterator[Commit], int] = git.get_revisions()
    return git.parse_revisions(revisions=revisions)


def storeRevisionDF(df: DataFrame, db: DB) -> None:
    commitHashes: DataFrame = df[["commit_hash"]]

    authors: DataFrame = df[["author", "author_email"]]
    authors.sort_values(by="author_email", inplace=True)
    authors.drop_duplicates(
        subset="author_email",
        keep="first",
        inplace=True,
        ignore_index=True,
    )

    committers: DataFrame = df[["committer", "committer_email"]]
    committers.sort_values(by="committer_email", inplace=True)
    committers.drop_duplicates(
        subset="committer_email",
        keep="first",
        inplace=True,
        ignore_index=True,
    )

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
