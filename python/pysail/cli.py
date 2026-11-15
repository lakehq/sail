import sys


def main():
    from pysail import _native  # noqa: PLC0415

    # When the Sail CLI is invoked via `python -m pysail`, the first argument in `sys.argv` is
    # the absolute path to `__main__.py`.
    _native.main(sys.argv)
