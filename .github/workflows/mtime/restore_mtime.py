#!/usr/bin/env python3

"""
This script reads the modification times, paths, and MD5 checksums of files from JSON Lines input,
and restores the modification times of the files if they exist and have not changed.
"""

import hashlib
import json
import os
import sys


def main():
    for line in sys.stdin:
        line = line.strip()  # noqa: PLW2901
        if not line:
            continue

        data = json.loads(line)
        path = data["path"]
        mtime = data["mtime"]
        md5 = data["md5"]

        if not os.path.isfile(path):
            continue

        with open(path, "rb") as file:
            content = file.read()

        if hashlib.md5(content).hexdigest() == md5:  # noqa: S324
            os.utime(path, (mtime, mtime))
            print(path)  # noqa: T201


if __name__ == "__main__":
    main()
