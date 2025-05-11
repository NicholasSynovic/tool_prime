# 5. Use Pandas to store information about commits

## Status

Accepted

## Context

Commit objects from `GitPython` contain nearly all of the commit information
from a `git commit` object. This information needs to be extracted from
individual `Commit` objects into a data structure that can be easily loaded into
a SQLite3 database table.

## Decision

We will use `pandas.DataFrame`s as our intermediate data structure. These
objects are performant, can scale with `Ray` or `Midas` storage backends, and
work well for loading into SQLite3 tables.

## Consequences

`pandas.DataFrame`s do not adhere to schemas by default. We will need to write a
`pydantic` object that wraps around a `pandas.DataFrame` object to ensure that
types are consistent when performing I/O operations with the database.
