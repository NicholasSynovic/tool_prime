from os.path import isdir
from pathlib import Path
from pprint import pprint as print
from typing import List

from prime_vx.shell.shell import resolvePath, runProgram
from prime_vx.vcs._vcsHandler import VCSHandler_ABC


class GitHandler(VCSHandler_ABC):
    def __init__(self, path: Path) -> None:
        resolvedPath: Path = resolvePath(path=path)
        if isdir(s=resolvedPath):
            self.path = resolvedPath
        else:
            print("Invalid git repository. Please point path to a directory")
            quit(1)

        self.cmdPrefix: str = f"git --no-pager -C {self.path}"

    def isRepository(self) -> bool:
        code: int = runProgram(
            cmd=f"{self.cmdPrefix} rev-parse --is-inside-work-tree"
        ).returncode

        if code == 0:
            return True
        else:
            return False

    def getCommitHashes(self) -> List[str]:
        hashes: str = runProgram(
            cmd=f"{self.cmdPrefix} log --reverse --format='%H'"
        ).stdout.decode()
        hashList: List[str] = hashes.strip().replace("'", "").split(sep="\n")
        return hashList

    def checkoutCommit(self, commitHash: str) -> bool:
        code: int = runProgram(
            cmd=f"{self.cmdPrefix} checkout --force {commitHash}"
        ).returncode

        if code == 0:
            return True
        else:
            return False

    def getCommitMetadata(self, commitHash: str) -> dict[str, str | List[str]]:
        keys: List[str] = [
            "commitHash",
            "treeHash",
            "parentHashes",
            "authorName",
            "authorEmail",
            "authorDate",
            "committerName",
            "committerEmail",
            "committerDate",
            "gpgSignature",
        ]
        values: List[str] = (
            runProgram(
                cmd=f"{self.cmdPrefix} log {commitHash} -n 1 --format='%H,,%T,,%P,,%an,,%ae,,%at,,%cn,,%ce,,%ct,,%G?'"
            )
            .stdout.decode()
            .strip()
            .replace("'", "")
            .split(sep=",,")
        )

        metadata: dict[str, str | List[str]] = dict(zip(keys, values))
        metadata["parentHashes"] = metadata["parentHashes"].split(sep=" ")

        return metadata
