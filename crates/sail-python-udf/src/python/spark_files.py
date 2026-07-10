# ruff: noqa: EM101, N802, TRY003

import os


class SparkFiles:
    _root_directory = None
    _is_running_on_worker = False
    _sc = None

    def __init__(self):
        raise NotImplementedError("Do not construct SparkFiles objects")

    @classmethod
    def get(cls, filename):
        return os.path.abspath(os.path.join(cls.getRootDirectory(), filename))

    @classmethod
    def getRootDirectory(cls):
        if cls._is_running_on_worker:
            return cls._root_directory
        raise RuntimeError("SparkFiles root directory is only available on Sail Python workers")
