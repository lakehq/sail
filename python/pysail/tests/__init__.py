"""Tests for Sail.

Sanity tests verify that an installed release works as expected. They are included
in release packages and can be run with `pytest --pyargs pysail.tests`.

The broader development test suite lives in `pysail.tests._dev` and is excluded
from release packages. In an editable installation, the same command also collects
these development tests. To run them against a release package, set
`PYSAIL_SOURCE_PATH` to the `python/pysail` directory in the source checkout.
This makes the `_dev` packages importable, but pytest needs the explicit package
name `pysail.tests._dev` with `--pyargs` to collect tests from the source checkout.
"""
