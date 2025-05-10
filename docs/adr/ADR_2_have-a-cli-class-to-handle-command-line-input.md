# 2. Have a CLI class to handle command line input

## Status

Accepted

## Context

`prime` is primarily going to be a command line application. The user should be
able to provide input into the application prior to it running.

## Decision

We will create a `CLI` class that leverages the builtin `argparse` library to
handle command line input. `argparse` was selected because of its robustness and
compatibility with multiple versions of `python`.

## Consequences

`argparse` is verbose, and updating command line interfaces that use it is not
the easiest process. However, we will try to be explicit about the parser and
utilize comments to identify each subparser.
