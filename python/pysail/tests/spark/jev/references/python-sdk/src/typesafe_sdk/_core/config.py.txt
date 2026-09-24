"""Configuration resolution."""

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

import httpx2

from typesafe_sdk._core.errors import TypeSafeError
from typesafe_sdk.constants import (
    API_KEY_ENV,
    BASE_URL_ENV,
    DEFAULT_BASE_URL,
    DEFAULT_MODEL,
    DEFAULT_MODEL_ENV,
    DEFAULT_TIMEOUT,
)


def _resolve_string(value: str | None, env: str, default: str = "") -> str:
    """Resolve an explicit string or a stripped environment value, falling back to a default."""
    return value if value is not None else os.environ.get(env, "").strip() or default


def resolve_and_validate_api_key(api_key: str | None) -> str:
    """Resolve an API key from the argument or environment, strip whitespace, and validate it."""
    key = _resolve_string(api_key, API_KEY_ENV).strip()
    if not key:
        raise TypeSafeError(f"No API key was provided. Pass api_key or set the {API_KEY_ENV} environment variable.")
    if not key.isascii() or not key.isprintable() or " " in key:
        raise TypeSafeError("API key must contain only printable ASCII characters without whitespace.")
    return key


def resolve_timeout(timeout: float | httpx2.Timeout) -> float | httpx2.Timeout:
    if not isinstance(timeout, httpx2.Timeout) and (not math.isfinite(timeout) or timeout <= 0):
        raise TypeSafeError("timeout must be a positive, finite number of seconds.")
    return timeout


@dataclass
class Config:
    api_key: str = field(repr=False)
    base_url: str
    default_model: str
    timeout: float | httpx2.Timeout
    default_headers: httpx2.Headers = field(repr=False)

    @classmethod
    def resolve(
        cls,
        api_key: str | None,
        base_url: str | None,
        default_model: str | None,
        timeout: float | httpx2.Timeout | None,
        default_headers: Mapping[str, str] | None,
    ) -> "Config":
        return cls(
            resolve_and_validate_api_key(api_key),
            _resolve_string(base_url, BASE_URL_ENV, DEFAULT_BASE_URL).rstrip("/"),
            _resolve_string(default_model, DEFAULT_MODEL_ENV, DEFAULT_MODEL),
            resolve_timeout(DEFAULT_TIMEOUT if timeout is None else timeout),
            httpx2.Headers(default_headers),
        )
