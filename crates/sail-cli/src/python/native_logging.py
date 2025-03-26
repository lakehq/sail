import logging
import sys


class NativeHandler(logging.Handler):
    """A logging handler that emits log records to Rust's logging system."""

    def __init__(self, level=0):
        super().__init__(level=level)
        self._emitter = self._get_emitter()

    def emit(self, record):
        self._emitter(record)

    def _get_emitter(self):
        return sys.modules[self.__module__].NativeLogging.emit
