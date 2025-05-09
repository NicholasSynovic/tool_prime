# 1. Use PyTest for handling unit tests

## Status

Accepted

## Context

We need to have unit tests for this application from the begininng. While
python's builtin `unittest` package works, simpler and more modern options
exists today.

## Decision

We will use the `pytest` library to handle writing and testing our code.
Additionally, code will initially be written with tests in the same file as the
functional code until ready. Then the tests will be migrated to a test specific
file.

## Consequences

This will slow down development of the tool. However, we can provide safety and
gaurentee that our code functionally works via this method.
