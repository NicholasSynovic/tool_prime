# PRIME: PRocess Internal MEtrics

> A rewrite of the PRocess Internal MEtrics tools (PRIME)

## Table of Contents

- [PRIME: PRocess Internal MEtrics](#prime-process-internal-metrics)
  - [Table of Contents](#table-of-contents)
  - [About](#about)
    - [What Are "Process Metrics"?](#what-are-process-metrics)
    - [What Are "Longitudinal Metrics"?](#what-are-longitudinal-metrics)
    - [Supported Metrics](#supported-metrics)
      - [What's The Difference Between *per commit* And *per day* Metrics?](#whats-the-difference-between-per-commit-and-per-day-metrics)
    - [How To Cite](#how-to-cite)
  - [System Dependencies](#system-dependencies)
  - [Using PRIME](#using-prime)
    - [Install With `pip`](#install-with-pip)
    - [Install With `pipx`](#install-with-pipx)
    - [Install with `uvx`](#install-with-uvx)
    - [Build From Source](#build-from-source)
    - [Command Line Options](#command-line-options)
      - [`prime --help`](#prime---help)
      - [`prime vcs`](#prime-vcs)
      - [`prime filesize`](#prime-filesize)
      - [`prime project-size`](#prime-project-size)
      - [`prime project-productivity`](#prime-project-productivity)
      - [`prime bus-factor`](#prime-bus-factor)
      - [`prime issues`](#prime-issues)
      - [`prime issue-spoilage`](#prime-issue-spoilage)
      - [`prime issue-density`](#prime-issue-density)
      - [`prime pull-requests`](#prime-pull-requests)
      - [`prime pull-request-spoilage`](#prime-pull-request-spoilage)
    - [Leveraging PRIME SQLite3 Database](#leveraging-prime-sqlite3-database)
  - [Contributing To PRIME](#contributing-to-prime)
    - [Submitting Issues](#submitting-issues)
      - [Submitting Bug Reports](#submitting-bug-reports)
      - [Submitting Inaccurate Metric Results](#submitting-inaccurate-metric-results)
      - [Submitting Security Patches](#submitting-security-patches)
    - [Requesting Features](#requesting-features)
      - [Requesting Version Control System (VCS) Support](#requesting-version-control-system-vcs-support)
      - [Requesting Metrics](#requesting-metrics)
      - [Requesting Issue Trackers](#requesting-issue-trackers)
      - [Requesting Pull Request Trackers](#requesting-pull-request-trackers)
    - [Developing Features](#developing-features)
      - [Adding VCS Support](#adding-vcs-support)
      - [Adding Metrics](#adding-metrics)
      - [Adding Issue Trackers](#adding-issue-trackers)
      - [Adding Pull Request Trackers](#adding-pull-request-trackers)

## About

Software metrics capture information about software development processes and
products. These metrics support decision-making, e.g., in team management or
dependency selection. However, existing metrics tools measure only a snapshot of
a software project. Little attention has been given to enabling engineers to
reason about metric trends over time --- longitudinal metrics that give insight
about process, not just product. In this work, we present PRIME (PRocess
Internal MEtrics), a tool to compute process metrics. The currently-supported
metrics include productivity, issue density, issue spoilage, pull request
spoilage, and bus factor. We invite the open-source software engineering
community to extend this tool with additional metrics, version control system
(VCS), and visualization support.

### What Are "Process Metrics"?

Process metrics are measurements of how a product is made. This could include
the number of steps involved the create the product, the total number of defects
identified, or the cost to create the product. In software engineering, process
metrics are used to evaluate the quality of the development process used to
create the project.

### What Are "Longitudinal Metrics"?

Longitudinal metrics are repeated measurements taken over established time
intervals. Examples include an hourly weather report, a daily report on stock
market performance, or a weekly count of how many miles someone drove. In
software engineering, examples include a weekly count of the number of new
issues, a monthly count of number of defects per thousand lines of code, or a
yearly count of the number of releases. By taking measurements at specific time
intervals, project managers, reviewers, and consumers are able to identify
trends and inform decisions regarding the trajectory of the project.

### Supported Metrics

PRIME currently supports the following metrics:

- bus factor *per day*
- file size *per commit*
- issue density *per day*
- issue spoilage *per day*
- project productivity *per commit*
- project productivity *per day*
- project size *per commit*
- project size *per day*
- pull request spoilage *per day*

#### What's The Difference Between *per commit* And *per day* Metrics?

*per commit* metrics are *base* metrics. A *base* metric is one that we can
measure directly from the version control system (VCS) or files, such as file
size. Thus, when we measure the file size of the project, we iterate *per
commit* to measure it.

*per day* metrics can either be *base* or *derived* metrics. A *derived* metric
is one that involves computing a value from one or more *base* metrics. For
example, productivity per day is measured by computing the absolute value of
changes to a project per day, often in reported as the change in the number of
lines of code. Some metrics (e.g., project size per day) are considered as
*base* metrics because the computation is considered trivial. For project size
per day, this is effectively the project size of the last commit for a given
day.

### How To Cite

See [CITATION.cff](CITATION.cff) for how to cite this work.

## System Dependencies

PRIME depends on the following system utilities:

- [`python3.13`](https://www.python.org/downloads/release/python-3130/)
- [`git`](https://git-scm.com/)
- [`scc`](https://github.com/boyter/scc)

## Using PRIME

### Install With `pip`

`pip install git+https://github.com/NicholasSynovic/prime.git`

### Install With `pipx`

`pipx install "git+https://github.com/NicholasSynovic/prime.git"`

### Install with `uvx`

`uv --from git+https://github.com/NicholasSynovic/prime tool install prime`

### Build From Source

```shell
git clone https://github.com/NicholasSynovic/prime.git
cd prime/
make build
```

### Command Line Options

#### `prime --help`

> PRocess Internal MEtrics

```shell
usage: prime [-h] [-v] {vcs,filesize,project-size,project-productivity,bus-factor,issues,issue-spoilage,issue-density,pull-requests,pull-request-spoilage} ...

PRocess Internal MEtrics

positional arguments:
  {vcs,filesize,project-size,project-productivity,bus-factor,issues,issue-spoilage,issue-density,pull-requests,pull-request-spoilage}
    vcs                 Parse a project's version control system for project metadata
    filesize            Measure the size of files by lines of code
    project-size        Measure the size of project by the lines of code
    project-productivity
                        Compute project productivity
    bus-factor          Compute bus factor
    issues              Get issue metadata from a GitHub repository
    issue-spoilage      Compute issue spoilage
    issue-density       Compute issue density
    pull-requests       Get pull request metadata from a GitHub repository
    pull-request-spoilage
                        Compute pull request spoilage

options:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime vcs`

> Parse a project's version control system for project metadata

```shell
usage: prime vcs [-h] -i VCS.INPUT -o VCS.OUTPUT

Step 1

options:
  -h, --help            show this help message and exit
  -i, --input VCS.INPUT
                        Filepath to a repository to analyze
  -o, --output VCS.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime filesize`

> Measure the size of files by lines of code

```shell
usage: prime filesize [-h] -i FILESIZE.INPUT -o FILESIZE.OUTPUT

Step 2

options:
  -h, --help            show this help message and exit
  -i, --input FILESIZE.INPUT
                        Filepath to a repository to analyze
  -o, --output FILESIZE.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime project-size`

> Measure the size of project by the lines of code

```shell
usage: prime project-size [-h] -o PROJECT_SIZE.OUTPUT

Step 3

options:
  -h, --help            show this help message and exit
  -o, --output PROJECT_SIZE.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime project-productivity`

> Compute project productivity

```shell
usage: prime project-productivity [-h] -o PROJECT_PRODUCTIVITY.OUTPUT

Step 4

options:
  -h, --help            show this help message and exit
  -o, --output PROJECT_PRODUCTIVITY.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime bus-factor`

> Compute bus factor

```shell
usage: prime bus-factor [-h] -o BUS_FACTOR.OUTPUT

Step 5

options:
  -h, --help            show this help message and exit
  -o, --output BUS_FACTOR.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime issues`

> Get issue metadata from a GitHub repository

```shell
usage: prime issues [-h] -a ISSUES.AUTH --owner ISSUES.OWNER --name ISSUES.REPO_NAME -o ISSUES.OUTPUT

Step 6

options:
  -h, --help            show this help message and exit
  -a, --auth ISSUES.AUTH
                        GitHub personal auth token
  --owner ISSUES.OWNER  GitHub repository owner
  --name ISSUES.REPO_NAME
                        GitHub repository name
  -o, --output ISSUES.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime issue-spoilage`

> Compute issue spoilage

```shell
usage: prime issue-spoilage [-h] -o ISSUE_SPOILAGE.OUTPUT

Step 7

options:
  -h, --help            show this help message and exit
  -o, --output ISSUE_SPOILAGE.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime issue-density`

> Compute issue density

```shell
usage: prime issue-density [-h] -o ISSUE_DENSITY.OUTPUT

Step 8

options:
  -h, --help            show this help message and exit
  -o, --output ISSUE_DENSITY.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime pull-requests`

> Get pull request metadata from a GitHub repository

```shell
usage: prime pull-requests [-h] -a PULL_REQUESTS.AUTH --owner PULL_REQUESTS.OWNER --name PULL_REQUESTS.REPO_NAME -o PULL_REQUESTS.OUTPUT

Step 9

options:
  -h, --help            show this help message and exit
  -a, --auth PULL_REQUESTS.AUTH
                        GitHub personal auth token
  --owner PULL_REQUESTS.OWNER
                        GitHub repository owner
  --name PULL_REQUESTS.REPO_NAME
                        GitHub repository name
  -o, --output PULL_REQUESTS.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

#### `prime pull-request-spoilage`

> Compute pull request spoilage

```shell
usage: prime pull-request-spoilage [-h] -o PULL-REQUEST-SPOILAGE.OUTPUT

Step 10

options:
  -h, --help            show this help message and exit
  -o, --output PULL-REQUEST-SPOILAGE.OUTPUT
                        Path to output SQLite3

Read the original research paper here: https://doi.org/10.1145/3551349.3559517
```

### Leveraging PRIME SQLite3 Database

## Contributing To PRIME

### Submitting Issues

#### Submitting Bug Reports

#### Submitting Inaccurate Metric Results

#### Submitting Security Patches

### Requesting Features

#### Requesting Version Control System (VCS) Support

#### Requesting Metrics

#### Requesting Issue Trackers

#### Requesting Pull Request Trackers

### Developing Features

#### Adding VCS Support

#### Adding Metrics

#### Adding Issue Trackers

#### Adding Pull Request Trackers
