"""
Handle command line argument parsing.

Copyright 2025 (C) Nicholas M. Synovic

"""

import importlib.metadata
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path

import prime


def add_output_argument(
    parser: ArgumentParser,
    dest_var: str,
) -> None:
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help=prime.OUTPUT_HELP,
        type=Path,
        dest=f"{dest_var}.output",
    )


def add_input_argument(
    parser: ArgumentParser,
    dest_var: str,
) -> None:
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help=prime.REPO_PATH_INPUT_HELP,
        type=Path,
        dest=f"{dest_var}.input",
    )


def add_gh_args(
    parser: ArgumentParser,
    dest_var: str,
) -> None:
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


class CLI:
    def __init__(self) -> None:
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

        # Version Control System (VCS) Subparser
        self.vcs_parser = self.vcs_subparser()

        # Size Subparser
        self.size_parser = self.size_subparser()

        # Project Productivity Subparser
        self.project_productivity_parser = self.project_productivity_subparser()

        # Bus Factor Subparser
        self.bus_factor_parser = self.bus_factor_subparser()

        # Pull Request Subparser
        self.pull_request_parser = self.pull_request_subparser()

        # Issue Subparser
        self.issue_parser = self.issue_subparser()

        # Issue Spoilage Subparser
        self.issue_spoilage_parser = self.issue_spoilage_subparser()

        # Issue Density Subparser
        self.issue_density_parser = self.issue_density_subparser()

    def vcs_subparser(self) -> ArgumentParser:
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

    def size_subparser(self) -> ArgumentParser:
        size_parser: ArgumentParser = self.subparsers.add_parser(
            name="size",
            help="Measure the size of repository by lines of code",
            description="Step 2",
            prog=f"{prime.PROG} size",
            epilog=prime.EPILOG,
        )

        add_input_argument(parser=size_parser, dest_var="size")
        add_output_argument(parser=size_parser, dest_var="size")

        return size_parser

    def project_productivity_subparser(self) -> ArgumentParser:
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
        return self.parser.parse_args()
