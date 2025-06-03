"""
Handle command line argument parsing.

Copyright 2025 (C) Nicholas M. Synovic

"""

import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path

import src


def get_version() -> str:
    """
    Retrieve the application version from a bundled file.

    Attempts to read the version string from a file named "_version"
    located in the temporary directory used by PyInstaller (`sys._MEIPASS`).
    If the application is not running in a PyInstaller bundle, returns
    a fallback string.

    Returns:
        str: The version string if found, otherwise "i don't know".

    """
    try:
        return Path(sys._MEIPASS, "_version").read_text(encoding="UTF-8").strip()
    except AttributeError:
        return "i don't know"


class CLI:
    """
    A helper class to manage argument parsing using the argparse module.

    Provides a more structured way to define and handle command-line arguments.

    """

    def __init__(self) -> None:
        """Initialize the argument parser."""
        self.parser: ArgumentParser = ArgumentParser(
            prog=src.PROG,
            description=src.DESCRIPTION,
            epilog=src.EPILOG,
        )
        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=get_version(),
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
        Define a subparser for parsing a project's version control system.

        This function creates a subparser to handle command-line arguments
        related to version control system (VCS) analysis. It defines the
        arguments needed to extract metadata from a project's VCS repository.

        Returns:
           An ArgumentParser object representing the VCS subparser.

        """
        vcs_parser: ArgumentParser = self.subparsers.add_parser(
            name="vcs",
            help="Parse a project's version control system for project metadata",
            prog=f"{src.PROG} vcs",
            epilog=src.EPILOG,
        )

        vcs_parser.add_argument(
            "-i",
            "--input",
            nargs=1,
            required=True,
            help="Path to project to analyze",
            type=Path,
            dest="vcs.input",
        )
        vcs_parser.add_argument(
            "-o",
            "--output",
            nargs=1,
            required=True,
            help="Path to output SQLite3",
            type=Path,
            dest="vcs.output",
        )

        return vcs_parser

    def size_subparser(self) -> ArgumentParser:
        """
        Create a subparser for measuring repository size.

        This function creates a subparser for handling size-related
        command-line arguments.  It defines the arguments required to
        calculate and store the repository size.

        Returns:
           An ArgumentParser object representing the size subparser.

        """
        size_parser: ArgumentParser = self.subparsers.add_parser(
            name="size",
            help="Measure the size of repository by lines of code",
            prog=f"{src.PROG} size",
            epilog=src.EPILOG,
        )

        size_parser.add_argument(
            "-i",
            "--input",
            nargs=1,
            required=True,
            help="Path to project to analyze",
            type=Path,
            dest="size.input",
        )
        size_parser.add_argument(
            "-o",
            "--output",
            nargs=1,
            required=True,
            help="Path to output SQLite3",
            type=Path,
            dest="size.output",
        )

        return size_parser

    def parse_args(self) -> Namespace:
        """
        Parse command-line arguments.

        This function parses the command-line arguments using the
        command-line argument parser.

        Returns:
           A Namespace object containing the parsed command-line arguments.

        """
        return self.parser.parse_args()
