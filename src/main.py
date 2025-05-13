import sys
from typing import Any

from pandas import DataFrame

from src.api.db import DB
from src.api.types import Authors, CommitHashes, CommitLog, Committers
from src.api.vcs import VersionControlSystem, identifyVCS, parseVCS
from src.cli import CLI


def getNameSpaceKey(ns: dict[str, Any]) -> str:
    return set([key.split(".")[0] for key in ns.keys()]).pop()


def handleDB(ns: dict[str, Any], nsKey: str) -> DB | None:
    nsKey: str = getNameSpaceKey(ns=ns)

    match nsKey:
        case "vcs":
            return DB(db_path=ns["vcs.output"][0])
        case _:
            return None


def handleVCS(ns: dict[str, Any], db: DB) -> None:
    vcs: VersionControlSystem | int = identifyVCS(repoPath=ns["vcs.input"][0])
    if vcs == -1:
        sys.exit(2)

    data: dict[str, DataFrame] = parseVCS(vcs=vcs)

    db.write_df(
        df=data["commit_hashes"],
        table="commit_hashes",
        model=CommitHashes,
    )
    db.write_df(df=data["authors"], table="authors", model=Authors)
    db.write_df(df=data["committers"], table="committers", model=Committers)
    db.write_df(df=data["commit_logs"], table="commit_logs", model=CommitLog)


def main() -> None:
    cli: CLI = CLI()
    ns: dict[str, Any] = cli.parse_args().__dict__
    try:
        nsKey: str = getNameSpaceKey(ns=ns)
    except KeyError:
        sys.exit(1)

    # Connect to database
    db: DB = handleDB(ns=ns, nsKey=nsKey)

    # Run subroutines based on command line parser
    match nsKey:
        case "vcs":
            handleVCS(ns=ns, db=db)
        case _:
            sys.exit(3)


if __name__ == "__main__":
    main()
