import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path

import src


class CLI:
    """
    A helper class to manage argument parsing using the argparse module.
    Provides a more structured way to define and handle command-line arguments.
    """

    def __init__(self) -> None:
        self.parser: ArgumentParser = ArgumentParser(
            prog=src.PROG,
            description=src.DESCRIPTION,
            epilog=src.EPILOG,
        )
        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=open(file=Path(sys._MEIPASS, "_version")).read().strip(),
        )

        self.subparsers: _SubParsersAction[ArgumentParser] = (
            self.parser.add_subparsers()
        )

        # VCS Subparser
        self.vcs_parser = self.vcs_subparser()

        # size Subparser
        self.size_parser = self.size_subparser()

    def vcs_subparser(self) -> ArgumentParser:
        """
        Defines the VCS subparser with its arguments.
        """

        vcsParser: ArgumentParser = self.subparsers.add_parser(
            name="vcs",
            help="Parse a project's version control system for project metadata",  # noqa: E501
            prog=f"{src.PROG} vcs",
            epilog=src.EPILOG,
        )

        vcsParser.add_argument(
            "-i",
            "--input",
            nargs=1,
            required=True,
            help="Path to project to analyze",
            type=Path,
            dest="vcs.input",
        )
        vcsParser.add_argument(
            "-o",
            "--output",
            nargs=1,
            required=True,
            help="Path to output SQLite3",
            type=Path,
            dest="vcs.output",
        )

        return vcsParser

    def size_subparser(self) -> ArgumentParser:
        """
        Defines the size subparser with its arguments.
        """

        sizeParser: ArgumentParser = self.subparsers.add_parser(
            name="size",
            help="Measure the size of repository by lines of code",
            prog=f"{src.PROG} size",
            epilog=src.EPILOG,
        )

        sizeParser.add_argument(
            "-i",
            "--input",
            nargs=1,
            required=True,
            help="Path to project to analyze",
            type=Path,
            dest="size.input",
        )
        sizeParser.add_argument(
            "-o",
            "--output",
            nargs=1,
            required=True,
            help="Path to output SQLite3",
            type=Path,
            dest="size.output",
        )

        return sizeParser

    def parse_args(self) -> Namespace:
        """
        Parses the command-line arguments.
        """
        self.args = self.parser.parse_args()
        return self.args


if __name__ == "__main__":
    cli: CLI = CLI()

    print(cli.parse_args())
