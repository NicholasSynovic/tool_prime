import sys
from json import dumps
from pathlib import Path
from typing import Any, Iterator, List, Tuple

from git import Commit
from pandas import DataFrame, Series
from progress.bar import Bar

from src.api.db import DB
from src.api.types import Authors, CommitHashes, CommitLog, Committers
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


def _dfReplaceValueWithIndexReference(
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


def _dfReplaceListValueWithIndexReference(
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

    df = _dfReplaceValueWithIndexReference(
        df1=df,
        df2=commitHashes,
        df1Col="commit_hash",
        df2Col="commit_hash",
    )
    df = _dfReplaceValueWithIndexReference(
        df1=df,
        df2=authors,
        df1Col="author_email",
        df2Col="author_email",
    )
    df = _dfReplaceValueWithIndexReference(
        df1=df,
        df2=committers,
        df1Col="committer_email",
        df2Col="committer_email",
    )

    df = _dfReplaceListValueWithIndexReference(
        df1=df,
        df2=authors,
        df1Col="co_author_emails",
        df2Col="author_email",
    )

    df = _dfReplaceListValueWithIndexReference(
        df1=df,
        df2=commitHashes,
        df1Col="parents",
        df2Col="commit_hash",
    )

    df.drop(columns=["author", "committer", "co_authors"], inplace=True)
    df.rename(
        columns={
            "author_email": "author_id",
            "co_author_emails": "co_author_ids",
            "commit_hash": "commit_hash_id",
            "committer_email": "committer_id",
            "parents": "parent_hash_ids",
        },
        inplace=True,
    )

    db.write_df(
        df=commitHashes,
        table="commit_hashes",
        model=CommitHashes,
    )
    db.write_df(df=authors, table="authors", model=Authors)
    db.write_df(df=committers, table="committers", model=Committers)
    db.write_df(df=df, table="commit_log", model=CommitLog)


def main() -> None:
    cli: CLI = CLI()
    ns: dict[str, Any] = cli.parse_args().__dict__

    nsKey: str = set([key.split(".")[0] for key in ns.keys()]).pop()

    db: DB
    # Extract/compute data to a DataFrame
    match nsKey:
        case "vcs":
            db = DB(db_path=ns["vcs.output"][0])
            df: DataFrame = git(repo=ns["vcs.input"][0])
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
