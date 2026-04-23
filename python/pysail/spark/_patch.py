"""Patches to PySpark for Sail-specific features.

This module monkey-patches PySpark internals to support Sail-specific
session configurations. The patches are applied automatically when the
``pysail.spark`` package is imported.
"""

from __future__ import annotations

import os
import platform
import time as _time_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql.connect.conf import RuntimeConf

_SAIL_USER_TIME_ZONE_KEY = "sail.user.timeZone"

# Sentinel object indicating that the original TZ has not been saved yet.
_NOT_SET = object()

# Saved original TZ value before any sail.user.timeZone override was applied.
# ``_NOT_SET`` means no override has been applied yet.
# ``None`` means TZ was not set in the environment before the override.
# A string means the TZ value that was set before the override.
_original_tz: object = _NOT_SET


def _apply_user_timezone(tz: str) -> None:
    """Apply a user timezone override by setting the process TZ environment variable.

    This affects how Python interprets naive :class:`datetime.datetime` objects,
    matching the semantics of ``-Duser.timezone`` in OSS Spark.

    On Windows, ``time.tzset()`` is not available, so the function only sets
    the environment variable.
    """
    global _original_tz  # noqa: PLW0603
    if _original_tz is _NOT_SET:
        # Save the original TZ before the first override.
        _original_tz = os.environ.get("TZ")

    if tz:
        os.environ["TZ"] = tz
    else:
        os.environ.pop("TZ", None)

    if platform.system() != "Windows":
        _time_module.tzset()


def _restore_user_timezone() -> None:
    """Restore the TZ environment variable to its value before the override."""
    global _original_tz  # noqa: PLW0603
    if _original_tz is _NOT_SET:
        return

    if _original_tz is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = str(_original_tz)

    _original_tz = _NOT_SET

    if platform.system() != "Windows":
        _time_module.tzset()


def _patch_runtime_conf() -> None:
    """Patch ``pyspark.sql.connect.conf.RuntimeConf.set`` to handle ``sail.user.timeZone``.

    When the user calls ``spark.conf.set("sail.user.timeZone", tz)``, the patch
    intercepts the call and applies the timezone override to the current process.
    """
    try:
        from pyspark.sql.connect.conf import RuntimeConf  # noqa: PLC0415
    except ImportError:
        return

    _original_set = RuntimeConf.set

    def _patched_set(self: RuntimeConf, key: str, value: Any) -> None:
        _original_set(self, key, value)
        if key == _SAIL_USER_TIME_ZONE_KEY:
            # Mirror the string conversion that RuntimeConf.set performs
            # for bool and int values, then apply the timezone override.
            if isinstance(value, bool):
                str_value = "true" if value else "false"
            elif isinstance(value, int):
                str_value = str(value)
            elif isinstance(value, str):
                str_value = value
            else:
                return
            _apply_user_timezone(str_value)

    RuntimeConf.set = _patched_set  # type: ignore[method-assign]

    _original_unset = RuntimeConf.unset

    def _patched_unset(self: RuntimeConf, key: str) -> None:
        _original_unset(self, key)
        if key == _SAIL_USER_TIME_ZONE_KEY:
            _restore_user_timezone()

    RuntimeConf.unset = _patched_unset  # type: ignore[method-assign]


_patch_runtime_conf()
