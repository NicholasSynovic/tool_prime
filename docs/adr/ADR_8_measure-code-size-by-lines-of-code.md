# 8. Measure code size by lines of code

## Status

Accepted

## Context

The original PrIMe project measured code size by the total lines of code using
`sloccount`. With the advent of `scc` we can get finer grain information about
repositories on a per file basis.

## Decision

We will use `scc` to measure a repositories size by the number of lines of code.
We will not post process this data until we start computing metrics.

## Consequences

As this is purely a measurement of the code size, we will have to write
additional metrics to compute measurements such as KLOC, dKLOC, and productivity
