import sys

from pysail import _native


def main():
    # When the Sail CLI is invoked via `python -m pysail`, the first argument in `sys.argv` is
    # the absolute path to `__main__.py`.
    _native.main(sys.argv)
