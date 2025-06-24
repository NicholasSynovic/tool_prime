"""
Handle command line argument parsing.

Copyright 2025 (C) Nicholas M. Synovic

"""

import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path

import prime


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
            prog=prime.PROG,
            description=prime.DESCRIPTION,
            epilog=prime.EPILOG,
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

        # issue Subparser
        self.issue_parser = self.issue_subparser()

        # pr Subparser
        self.pull_request_parser = self.pull_request_subparser()

        # pp Subparser
        self.project_productivity_parser = self.project_productivity_subparser()

        # bf Subparser
        self.bus_factor_parser = self.bus_factor_subparser()

        # is Subparser
        self.issue_spoilage_parser = self.issue_spoilage_subparser()

        # id Subparser
        self.issue_density_parser = self.issue_density_subparser()

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
            description="The first stage of the metrics pipeline",
            prog=f"{prime.PROG} vcs",
            epilog=prime.EPILOG,
        )

        vcs_parser.add_argument(
            "-i",
            "--input",
            required=True,
            help="Path to project to analyze",
            type=Path,
            dest="vcs.input",
        )
        vcs_parser.add_argument(
            "-o",
            "--output",
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
            description="The second stage of the metrics pipeline",
            prog=f"{prime.PROG} size",
            epilog=prime.EPILOG,
        )

        size_parser.add_argument(
            "-i",
            "--input",
            required=True,
            help="Path to project to analyze",
            type=Path,
            dest="size.input",
        )
        size_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="Path to output SQLite3",
            type=Path,
            dest="size.output",
        )

        return size_parser

    def issue_subparser(self) -> ArgumentParser:
        """
        Define and return the argument subparser for the 'issues' command.

        This subparser handles command-line arguments related to retrieving GitHub
        issue metadata for a specific repository. It configures required flags for
        authentication, repository identification, and output location.

        Returns:
            ArgumentParser: Configured subparser for the 'issues' command.

        """
        issue_parser: ArgumentParser = self.subparsers.add_parser(
            name="issues",
            help="Get issue metadata from a GitHub repository",
            description="The third stage of the metrics pipeline",
            prog=f"{prime.PROG} issues",
            epilog=prime.EPILOG,
        )

        issue_parser.add_argument(
            "-a",
            "--auth",
            required=True,
            help="GitHub personal auth token",
            type=str,
            dest="issues.auth",
        )
        issue_parser.add_argument(
            "--owner",
            required=True,
            help="Owner of a GitHub repository",
            type=str,
            dest="issues.owner",
        )
        issue_parser.add_argument(
            "--repo-name",
            required=True,
            help="Name of a GitHub repository",
            type=str,
            dest="issues.repo_name",
        )
        issue_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="Path to output SQLite3",
            type=Path,
            dest="issues.output",
        )

        return issue_parser

    def pull_request_subparser(self) -> ArgumentParser:
        """
        Define and return the argument subparser for the 'pr' command.

        This subparser handles command-line arguments related to retrieving GitHub
        issue metadata for a specific repository. It configures required flags for
        authentication, repository identification, and output location.

        Returns:
            ArgumentParser: Configured subparser for the 'pr' command.

        """
        pull_requests_parser: ArgumentParser = self.subparsers.add_parser(
            name="pr",
            help="Get pull request metadata from a GitHub repository",
            description="The fourth stage of the metrics pipeline",
            prog=f"{prime.PROG} pr",
            epilog=prime.EPILOG,
        )

        pull_requests_parser.add_argument(
            "-a",
            "--auth",
            required=True,
            help="GitHub personal auth token",
            type=str,
            dest="pull_requests.auth",
        )
        pull_requests_parser.add_argument(
            "--owner",
            required=True,
            help="Owner of a GitHub repository",
            type=str,
            dest="pull_requests.owner",
        )
        pull_requests_parser.add_argument(
            "--repo-name",
            required=True,
            help="Name of a GitHub repository",
            type=str,
            dest="pull_requests.repo_name",
        )
        pull_requests_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="Path to output SQLite3",
            type=Path,
            dest="pull_requests.output",
        )

        return pull_requests_parser

    def project_productivity_subparser(self) -> ArgumentParser:
        """
        Define and return the argument subparser for the 'pp' command.

        This subparser handles command-line arguments related to computing the
        productivity of a project. Project  productivity is a measurement of
        activity in a project and is not reflective of the team's capabilities.

        Returns:
            ArgumentParser: Configured subparser for the 'pp' command.

        """
        project_productivity_parser: ArgumentParser = self.subparsers.add_parser(
            name="pp",
            help="Compute project productivity",
            description="The sixth stage of the metrics pipeline",
            prog=f"{prime.PROG} pp",
            epilog=prime.EPILOG,
        )

        project_productivity_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="Path to SQLite3 database",
            type=Path,
            dest="project_productivity.output",
        )

        return project_productivity_parser

    def bus_factor_subparser(self) -> ArgumentParser:
        """
        Define and return the argument subparser for the 'bf' command.

        This subparser handles command-line arguments related to computing the
        bus factor of a project. Bus factor is a measurement of an individuals
        contributions to a a project and is not reflective of the content that
        they contribute.

        Returns:
            ArgumentParser: Configured subparser for the 'bf' command.

        """
        bus_factor_parser: ArgumentParser = self.subparsers.add_parser(
            name="bf",
            help="Compute bus factor",
            description="The seventh stage of the metrics pipeline",
            prog=f"{prime.PROG} bf",
            epilog=prime.EPILOG,
        )

        bus_factor_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="Path to SQLite3 database",
            type=Path,
            dest="bus_factor.output",
        )

        return bus_factor_parser

    def issue_spoilage_subparser(self) -> ArgumentParser:
        issue_spoilage_parser: ArgumentParser = self.subparsers.add_parser(
            name="is",
            help="Compute issue spoilage",
            description="The eighth stage of the metrics pipeline",
            prog=f"{prime.PROG} is",
            epilog=prime.EPILOG,
        )

        issue_spoilage_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="Path to SQLite3 database",
            type=Path,
            dest="issue_spoilage.output",
        )

        return issue_spoilage_parser

    def issue_density_subparser(self) -> ArgumentParser:
        issue_density_parser: ArgumentParser = self.subparsers.add_parser(
            name="id",
            help="Compute issue density",
            description="The eighth stage of the metrics pipeline",
            prog=f"{prime.PROG} id",
            epilog=prime.EPILOG,
        )

        issue_density_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help="Path to SQLite3 database",
            type=Path,
            dest="issue_density.output",
        )

        return issue_density_parser

    def parse_args(self) -> Namespace:
        """
        Parse command-line arguments.

        This function parses the command-line arguments using the
        command-line argument parser.

        Returns:
           A Namespace object containing the parsed command-line arguments.

        """
        return self.parser.parse_args()
