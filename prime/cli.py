"""
Handle command line argument parsing.

Copyright 2025 (C) Nicholas M. Synovic

"""

import importlib.metadata
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path
from typing import Any

import prime
from prime.api.utils import resolve_path


def add_output_argument(
    parser: ArgumentParser,
    dest_var: str,
) -> None:
    """
    Add a required output argument to the argument parser.

    This function modifies the provided ArgumentParser object by adding a
    required output argument. The argument is specified with the flags '-o' and
    '--output', and it expects a path as its value. The destination variable
    for the argument is constructed using the provided `dest_var` string.

    Args:
        parser (ArgumentParser): The argument parser to which the output
            argument will be added.
        dest_var (str): The base name for the destination variable where the
            output path will be stored. The final destination will be formatted
            as '{dest_var}.output'.

    """
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help=prime.OUTPUT_HELP,
        type=resolve_path,
        dest=f"{dest_var}.output",
    )


def add_input_argument(
    parser: ArgumentParser,
    dest_var: str,
) -> None:
    """
    Add a required input argument to the argument parser.

    This function modifies the provided ArgumentParser object by adding a
    required input argument. The argument is specified with the flags '-i' and
    '--intput', and it expects a path as its value. The destination variable
    for the argument is constructed using the provided `dest_var` string.

    Args:
        parser (ArgumentParser): The argument parser to which the input
            argument will be added.
        dest_var (str): The base name for the destination variable where the
            input path will be stored. The final destination will be formatted
            as '{dest_var}.input'.

    """
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help=prime.REPO_PATH_INPUT_HELP,
        type=resolve_path,
        dest=f"{dest_var}.input",
    )


def add_gh_args(
    parser: ArgumentParser,
    dest_var: str,
) -> None:
    """
    Add GitHub-related arguments to the argument parser.

    This function modifies the provided ArgumentParser object by adding required
    arguments related to GitHub authentication and repository identification.
    The arguments include authentication token, repository owner, and repository
    name.

    Args:
        parser (ArgumentParser): The argument parser to which the GitHub
            arguments will be added.
        dest_var (str): The base name for the destination variables where the
            GitHub argument values will be stored. The final destinations will
            be formatted as '{dest_var}.auth', '{dest_var}.owner', and
            '{dest_var}.repo_name'.

    """
    parser.add_argument(
        "-a",
        "--auth",
        required=True,
        help=prime.GH_AUTH_HELP,
        type=str,
        dest=f"{dest_var}.auth",
    )
    parser.add_argument(
        "--owner",
        required=True,
        help=prime.GH_OWNER_HELP,
        type=str,
        dest=f"{dest_var}.owner",
    )
    parser.add_argument(
        "--name",
        required=True,
        help=prime.GH_NAME_HELP,
        type=str,
        dest=f"{dest_var}.repo_name",
    )


def get_first_namespace_key(namespace: dict[str, Any]) -> str:
    """
    Retrieve the first unique key prefix from a namespace dictionary.

    This function extracts the prefix from each key in the provided dictionary,
    splits the keys by the '.' character, and returns the first unique prefix found.

    Args:
        namespace (dict[str, Any]): The dictionary containing namespace keys.

    Returns:
        str: The first unique prefix from the keys in the namespace.

    """
    return {key.split(".")[0] for key in namespace}.pop()


class CLI:
    """
    Command Line Interface for parsing and executing various subcommands.

    This class sets up an argument parser with multiple subcommands, each
    corresponding to a specific task related to project analysis, such as
    version control system parsing, size measurement, project productivity
    computation, and more.

    Attributes:
        parser (ArgumentParser): The main argument parser for the CLI.
        subparsers (_SubParsersAction[ArgumentParser]): The subparsers for
            different tasks.

    """

    def __init__(self) -> None:
        """Initialize the CLI with subparsers."""
        self.parser: ArgumentParser = ArgumentParser(
            prog=prime.PROG,
            description=prime.DESCRIPTION,
            epilog=prime.EPILOG,
        )
        self.parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=importlib.metadata.version(distribution_name="prime"),
        )

        self.subparsers: _SubParsersAction[ArgumentParser] = (
            self.parser.add_subparsers()
        )

        # Initialize subparsers for different tasks
        self.vcs_subparser()
        self.filesize_subparser()
        self.project_size_subparser()
        self.project_productivity_subparser()
        self.bus_factor_subparser()
        self.issue_subparser()
        self.pull_request_subparser()
        self.issue_spoilage_subparser()
        self.issue_density_subparser()

    def vcs_subparser(self) -> ArgumentParser:
        """
        Add a subparser for parsing version control system metadata.

        Returns:
            ArgumentParser: The subparser for the 'vcs' command.

        """
        vcs_parser: ArgumentParser = self.subparsers.add_parser(
            name="vcs",
            help="Parse a project's version control system for project metadata",
            description="Step 1",
            prog=f"{prime.PROG} vcs",
            epilog=prime.EPILOG,
        )

        add_input_argument(parser=vcs_parser, dest_var="vcs")
        add_output_argument(parser=vcs_parser, dest_var="vcs")

        return vcs_parser

    def filesize_subparser(self) -> ArgumentParser:
        """
        Add a subparser for measuring repository size by lines of code.

        Measure a source code repository by the number of lines of code per
        file per commit.

        Returns:
            ArgumentParser: The subparser for the 'size' command.

        """
        filesize_parser: ArgumentParser = self.subparsers.add_parser(
            name="filesize",
            help="Measure the size of files by lines of code",
            description="Step 2",
            prog=f"{prime.PROG} filesize",
            epilog=prime.EPILOG,
        )

        add_input_argument(parser=filesize_parser, dest_var="filesize")
        add_output_argument(parser=filesize_parser, dest_var="filesize")

        return filesize_parser

    def project_size_subparser(self) -> ArgumentParser:
        """
        Add a subparser for measuring project size.

        Measure a source code repository by the number of lines of code per
        commit.

        Returns:
            ArgumentParser: The subparser for the 'size' command.

        """
        project_size_parser: ArgumentParser = self.subparsers.add_parser(
            name="project-size",
            help="Measure the size of project by the lines of code",
            description="Step 2",
            prog=f"{prime.PROG} project-size",
            epilog=prime.EPILOG,
        )

        add_output_argument(parser=project_size_parser, dest_var="project_size")

        return project_size_parser

    def project_productivity_subparser(self) -> ArgumentParser:
        """
        Add a subparser for computing project productivity.

        Returns:
            ArgumentParser: The subparser for the 'project-productivity'
                command.

        """
        project_productivity_parser: ArgumentParser = self.subparsers.add_parser(
            name="project-productivity",
            help="Compute project productivity",
            description="Step 3",
            prog=f"{prime.PROG} project-productivity",
            epilog=prime.EPILOG,
        )

        add_output_argument(
            parser=project_productivity_parser,
            dest_var="project_productivity",
        )

        return project_productivity_parser

    def bus_factor_subparser(self) -> ArgumentParser:
        """
        Add a subparser for computing bus factor.

        Returns:
            ArgumentParser: The subparser for the 'bus-factor' command.

        """
        bus_factor_parser: ArgumentParser = self.subparsers.add_parser(
            name="bus-factor",
            help="Compute bus factor",
            description="Step 4",
            prog=f"{prime.PROG} bus-factor",
            epilog=prime.EPILOG,
        )

        add_output_argument(parser=bus_factor_parser, dest_var="bus_factor")

        return bus_factor_parser

    def issue_subparser(self) -> ArgumentParser:
        """
        Add a subparser for retrieving issue metadata from a GitHub repository.

        Returns:
            ArgumentParser: The subparser for the 'issues' command.

        """
        issue_parser: ArgumentParser = self.subparsers.add_parser(
            name="issues",
            help="Get issue metadata from a GitHub repository",
            description="Step 5",
            prog=f"{prime.PROG} issues",
            epilog=prime.EPILOG,
        )

        add_gh_args(parser=issue_parser, dest_var="issues")
        add_output_argument(parser=issue_parser, dest_var="issues")

        return issue_parser

    def pull_request_subparser(self) -> ArgumentParser:
        """
        Add a subparser for retrieving pull requests from a GitHub repository.

        Returns:
            ArgumentParser: The subparser for the 'pull-requests' command.

        """
        pull_requests_parser: ArgumentParser = self.subparsers.add_parser(
            name="pull-requests",
            help="Get pull request metadata from a GitHub repository",
            description="Step 6",
            prog=f"{prime.PROG} pull-requests",
            epilog=prime.EPILOG,
        )

        add_gh_args(parser=pull_requests_parser, dest_var="pull_requests")
        add_output_argument(
            parser=pull_requests_parser,
            dest_var="pull_requests",
        )

        return pull_requests_parser

    def issue_spoilage_subparser(self) -> ArgumentParser:
        """
        Add a subparser for computing issue spoilage.

        Returns:
            ArgumentParser: The subparser for the 'issue-spoilage' command.

        """
        issue_spoilage_parser: ArgumentParser = self.subparsers.add_parser(
            name="issue-spoilage",
            help="Compute issue spoilage",
            description="Step 7",
            prog=f"{prime.PROG} issue-spoilage",
            epilog=prime.EPILOG,
        )

        add_output_argument(
            parser=issue_spoilage_parser,
            dest_var="issue_spoilage",
        )

        return issue_spoilage_parser

    def issue_density_subparser(self) -> ArgumentParser:
        """
        Add a subparser for computing issue density.

        Returns:
            ArgumentParser: The subparser for the 'issue-density' command.

        """
        issue_density_parser: ArgumentParser = self.subparsers.add_parser(
            name="issue-density",
            help="Compute issue density",
            description="Step 8",
            prog=f"{prime.PROG} issue-density",
            epilog=prime.EPILOG,
        )

        add_output_argument(
            parser=issue_density_parser,
            dest_var="issue_density",
        )

        return issue_density_parser

    def parse_args(self) -> Namespace:
        """
        Parse the command-line arguments.

        Returns:
            Namespace: The parsed arguments as a Namespace object.

        """
        return self.parser.parse_args()
