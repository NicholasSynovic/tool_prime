"""
Handle command line argument parsing.

Copyright 2025 (C) Nicholas M. Synovic

"""

import importlib.metadata
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path

import prime


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

        vcs_parser.add_argument(
            "-i",
            "--input",
            required=True,
            help=prime.REPO_PATH_INPUT_HELP,
            type=Path,
            dest="vcs.input",
        )
        vcs_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help=prime.OUTPUT_HELP,
            type=Path,
            dest="vcs.output",
        )

        return vcs_parser

    def size_subparser(self) -> ArgumentParser:
        size_parser: ArgumentParser = self.subparsers.add_parser(
            name="size",
            help="Measure the size of repository by lines of code",
            description="Step 2",
            prog=f"{prime.PROG} size",
            epilog=prime.EPILOG,
        )

        size_parser.add_argument(
            "-i",
            "--input",
            required=True,
            help=prime.REPO_PATH_INPUT_HELP,
            type=Path,
            dest="size.input",
        )
        size_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help=prime.OUTPUT_HELP,
            type=Path,
            dest="size.output",
        )

        return size_parser

    def project_productivity_subparser(self) -> ArgumentParser:
        project_productivity_parser: ArgumentParser = self.subparsers.add_parser(
            name="project-productivity",
            help="Compute project productivity",
            description="Step 3",
            prog=f"{prime.PROG} project-productivity",
            epilog=prime.EPILOG,
        )

        project_productivity_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help=prime.OUTPUT_HELP,
            type=Path,
            dest="project_productivity.output",
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

        bus_factor_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help=prime.OUTPUT_HELP,
            type=Path,
            dest="bus_factor.output",
        )

        return bus_factor_parser

    def issue_subparser(self) -> ArgumentParser:
        issue_parser: ArgumentParser = self.subparsers.add_parser(
            name="issues",
            help="Get issue metadata from a GitHub repository",
            description="Step 5",
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
            help=prime.OUTPUT_HELP,
            type=Path,
            dest="issues.output",
        )

        return issue_parser

    def pull_request_subparser(self) -> ArgumentParser:
        pull_requests_parser: ArgumentParser = self.subparsers.add_parser(
            name="pull-requests",
            help="Get pull request metadata from a GitHub repository",
            description="Step 6",
            prog=f"{prime.PROG} pull-requests",
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
            help=prime.OUTPUT_HELP,
            type=Path,
            dest="pull_requests.output",
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

        issue_spoilage_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help=prime.OUTPUT_HELP,
            type=Path,
            dest="issue_spoilage.output",
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

        issue_density_parser.add_argument(
            "-o",
            "--output",
            required=True,
            help=prime.OUTPUT_HELP,
            type=Path,
            dest="issue_density.output",
        )

        return issue_density_parser

    def parse_args(self) -> Namespace:
        return self.parser.parse_args()
