# 3. Use GitPython to interface with the `git` VCS

## Status

Accepted

## Context

We need a way to interact with version control systems (VCSs). While we could
write and execute shell code via `popen`, this is often insecure and limits
portability. Using a wrapper library around a VCS would be best preferred.

## Decision

We will use `GitPython` as our library of choice for interfacing with the `git`
VCS. This option is one of the most popular choices for working with `git`
projects. Other options include `pygit2` and `dulwitch`.

## Consequences

We will need to have `git` installed on the system for `GitPython` to be of use.
If portability becomes a problem, `dulwitch` - a pure python implementation of
`git`, could be leveraged instead. However, a `docker` image should first be
considered to resolve portability.
