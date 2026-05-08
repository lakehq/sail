#!/usr/bin/env python3

"""
This script outputs the modification times of a list of files,
along with their paths and MD5 checksums, in JSON Lines format.
The output can be used to restore the modification times of the files later
if the files have not changed.
"""

import hashlib
import json
import os
import sys


def main():
    for line in sys.stdin:
        path = line.rstrip("\n")
        if not path or not os.path.isfile(path):
            continue

        mtime = int(os.path.getmtime(path))
        with open(path, "rb") as file:
            content = file.read()
        md5 = hashlib.md5(content).hexdigest()  # noqa: S324

        print(json.dumps({"path": path, "mtime": mtime, "md5": md5}))  # noqa: T201


if __name__ == "__main__":
    main()
