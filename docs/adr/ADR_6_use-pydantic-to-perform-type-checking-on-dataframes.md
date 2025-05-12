# 6. Use Pydantic to perform type checking on DataFrames

## Status

Accepted

## Context

In the [previous ADR](./ADR_5_use-pandas-to-store-information-about-commits.md)
it was identified that a consequence of using Pandas DataFrames to store data is
that column types may not be consistent. Thus when we write to the database, we
can't gaurentee that the data written adheres to the database schema. We also
don't know what type data read from the database, thereby undermining confidence
in functions that operate on the DataFrame.

## Decision

We will define a Pydantic type per database table and write code to check if a
Pandas DataFrame adheres to the type. Errors **will not** be gracefully handled
at this time.

## Consequences

We will ensure that our types are consistent across the data representations at
the expense of having to check each DataFrame prior to be written to the
database and after we read from the database.
