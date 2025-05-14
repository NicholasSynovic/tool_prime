# 7. Identify release revisions of a repository

## Status

Accepted

## Context

Currently, we are keeping track of all revisions of a version control
repository. However, some of these revisions are (arguably) more important than
others as they specify a release canidate of the project's source code. In order
to compute Agile process metrics, we should identify and keep track of these
release revisions.

## Decision

Using `GitPython`, we will identify and release revisions and keep a table that
maps revision hashes to tag names.

## Consequences

I'm not sure how different VCS's keep track of release canidates, so this may
not be viable for all projects.
