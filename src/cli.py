from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path

import src


class CLI:
    """
    A helper class to manage argument parsing using the argparse module.
    Provides a more structured way to define and handle command-line arguments.
    """

    def __init__(self) -> None:
        self.destCommands: set[str] = {"vcs"}

        self.parser: ArgumentParser = ArgumentParser(
            prog=src.PROG,
            description=src.DESCRIPTION,
            epilog=src.EPILOG,
        )

        self.subparsers: _SubParsersAction[ArgumentParser] = (
            self.parser.add_subparsers()
        )

        # VCS Subparser
        self.vcs_parser = self.vcs_subparser()

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
            default=Path("."),
            required=False,
            help="Path to project to analyze",
            dest="vcs.input",
        )
        vcsParser.add_argument(
            "-o",
            "--output",
            nargs=1,
            default=Path("./prime.sqlite3"),
            required=False,
            help="Path to output SQLite3",
            dest="vcs.output",
        )

        return vcsParser

    def parse_args(self) -> Namespace:
        """
        Parses the command-line arguments.
        """
        self.args = self.parser.parse_args()
        return self.args


if __name__ == "__main__":
    cli: CLI = CLI()

    print(cli.parse_args())
