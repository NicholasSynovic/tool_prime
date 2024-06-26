from argparse import Namespace
from pathlib import Path
from typing import List, Tuple

import pandas
from pandas import DataFrame
from progress.bar import Bar

from prime.cloc._classes._clocTool import CLOCTool_ABC
from prime.cloc.cloc import CLOC
from prime.cloc.gocloc import GoCLOC
from prime.cloc.scc import SCC
from prime.cloc.sloccount import SLOCCount
from prime.datamodels.vcs import VCS_DF_DATAMODEL
from prime.db import CLOC_DB_TABLE_NAME, VCS_DB_TABLE_NAME
from prime.db.sqlite import SQLite
from prime.exceptions import *
from prime.vcs._classes._vcsHandler import VCSHandler_ABC
from prime.vcs.git import GitHandler
from prime.vcs.mercurial import MercurialHandler


def computeCLOC(
    df: DataFrame,
    tool: CLOCTool_ABC,
    vcs: VCSHandler_ABC,
) -> DataFrame:
    data: List[DataFrame] = []

    with Bar("Computing CLOC-like metrics...", max=df.shape[0]) as bar:
        row: Tuple[str, str, str]
        for row in df.itertuples(index=False):
            vcs.checkoutCommit(commitHash=row[0])
            data.append(tool.compute(commitHash=row[0]))
            bar.next()

    return pandas.concat(objs=data, ignore_index=True)


def main(namespace: Namespace, db: SQLite) -> None:
    programInput: dict[str, List[Path]] = dict(namespace._get_kwargs())
    programKeys: List[str] = list(programInput.keys())

    inputKey: str = [key for key in programKeys if "input" in key][0]
    inputKeySplit: List[str] = inputKey.split(sep=".")

    vcsDF: DataFrame = db.read(
        tdf=VCS_DF_DATAMODEL,
        tableName=VCS_DB_TABLE_NAME,
    )

    relevantColumnsDF: DataFrame = vcsDF[
        [
            "commit_hash",
            "vcs",
            "path",
        ]
    ]

    capturedVCS: List = relevantColumnsDF["vcs"].unique()
    capturedPath: List = relevantColumnsDF["path"].unique()

    if len(capturedVCS) > 1:
        raise VCSDBError_MultipleVCSCaptured

    if len(capturedPath) > 1:
        raise VCSDBError_MultiplePathCaptured

    repositoryPath: Path = Path(capturedPath[0])

    # Create instance of the CLOC tool for analysis from Namespace
    match inputKeySplit[1]:
        case "scc":
            tool: CLOCTool_ABC = SCC(path=repositoryPath)
        case "cloc":
            tool: CLOCTool_ABC = CLOC(path=repositoryPath)
        case "sloccount":
            tool: CLOCTool_ABC = SLOCCount(path=repositoryPath)
        case "gocloc":
            tool: CLOCTool_ABC = GoCLOC(path=repositoryPath)
        case _:
            raise InvalidCLOCTool

    # Create instance of VCS handler from DB
    match capturedVCS[0]:
        case "git":
            vcsHandler: VCSHandler_ABC = GitHandler(path=repositoryPath)
        case "mercurial":
            vcsHandler: VCSHandler_ABC = MercurialHandler(path=repositoryPath)
        case _:
            raise InvalidVersionControl

    df: DataFrame = computeCLOC(
        df=relevantColumnsDF,
        tool=tool,
        vcs=vcsHandler,
    )
    df.index.name = "index"

    db.write(df=df, tableName=CLOC_DB_TABLE_NAME, includeIndex=True)
