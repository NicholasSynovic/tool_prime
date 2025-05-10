# 4. Use SQLAlchemy to handle database connections

## Status

Accepted

## Context

We need a way to manage and create an SQLite3 database to store VCS information.
Ideally, we'd want something that allows for a connection to *any* relational
database to be supported.

## Decision

We will use `SQLAlchemy` to handle DB connections. We will use the SQLite3
database to store information as it allows for a single file to be stored in a
repository that contains all of the repository information.

## Consequences

SQLite3 databases are limited to the number of parallel connections. If we ever
write this application to support multi-threading or add a UI to it, we need to
be careful of our concurrent database connections.
