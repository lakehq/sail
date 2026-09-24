"""Exceptions and server message extraction."""

import math
import time
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from typing import Any

import httpx2
from typing_extensions import override

from typesafe_sdk._core.constants import MAX_ERROR_BODY_LENGTH, REQUEST_ID_HEADER, RETRY_AFTER_HEADER, RETRY_AFTER_MS_HEADER
from typesafe_sdk._core.json import serialize


def parse_retry_after(headers: httpx2.Headers) -> float | None:
    for name, multiplier in ((RETRY_AFTER_MS_HEADER, 1), (RETRY_AFTER_HEADER, 1000)):
        raw = headers.get(name)
        if raw is None:
            continue
        try:
            value = float(raw.strip() or "0")
        except ValueError:
            if name == RETRY_AFTER_HEADER:
                try:
                    return max(0.0, (parsedate_to_datetime(raw).timestamp() - time.time()) * 1000)
                except (ValueError, TypeError, OverflowError):
                    pass
        else:
            if math.isfinite(value):
                if value >= 0:
                    delay = value * multiplier
                    if math.isfinite(delay):
                        return delay
                elif name == RETRY_AFTER_HEADER:
                    return None
    return None


def extract_message(body: Any) -> str | None:
    if isinstance(body, str):
        return body or None
    elif not isinstance(body, dict):
        return None
    error, message, detail = body.get("error"), body.get("message"), body.get("detail")
    if isinstance(error, str):
        return error
    elif isinstance(error, dict) and isinstance(error.get("message"), str):
        return error["message"]
    elif isinstance(message, str):
        return message
    elif isinstance(detail, str):
        return detail
    elif isinstance(detail, dict) and isinstance(detail.get("message"), str):
        return detail["message"]
    elif isinstance(detail, list):
        parts = []
        for entry in detail:
            if not isinstance(entry, dict) or not isinstance(entry.get("msg"), str):
                continue
            location = entry.get("loc")
            path = ".".join(str(item) for item in location if item != "body") if isinstance(location, list) else ""
            parts.append(f"{path}: {entry['msg']}" if path else entry["msg"])
        return "; ".join(parts) or None
    return None


class TypeSafeError(Exception):
    """Base exception for SDK failures."""


class TypeSafeAPIError(TypeSafeError):
    """An unsuccessful HTTP response with its body and request metadata."""

    def __init__(self, status: int, body: Any, headers: httpx2.Headers, message: str | None = None, endpoint: str | None = None) -> None:
        """Describe an HTTP failure with an optional message override."""
        super().__init__(status, body, headers, message, endpoint)
        self.status = status
        """HTTP response status code."""
        self.body = body
        """The server's JSON error body, plain response text, or `None` for an empty body."""
        self.headers = headers
        """HTTP response headers."""
        self.endpoint = endpoint
        """The request method and URL, without credentials, query parameters, or fragment, when available."""
        if message is None:
            detail = extract_message(body)
            if detail:
                message = detail
            elif body is None:
                message = "status code (no body)"
            else:
                raw = body if isinstance(body, str) else serialize(body).decode()
                message = raw[:MAX_ERROR_BODY_LENGTH] + "…" if len(raw) > MAX_ERROR_BODY_LENGTH else raw
        self._message = message

    @override
    def __str__(self) -> str:
        """Return the status and error message with available request context."""
        message = f"{self.status} {self._message}" if self._message else str(self.status)
        if self.endpoint is not None:
            message = f"{self.endpoint}: {message}"
        if self.request_id is not None:
            message += f" (request_id={self.request_id})"
        return message

    @override
    def __repr__(self) -> str:
        """Represent the error without including its response body and headers."""
        return f"{type(self).__name__}({str(self)!r})"

    @property
    def request_id(self) -> str | None:
        """The `x-typesafe-request-id` response header, or `None` if absent."""
        return self.headers.get(REQUEST_ID_HEADER)


class TypeSafeBadRequestError(TypeSafeAPIError):
    """The request was invalid (400)."""


class TypeSafeAuthenticationError(TypeSafeAPIError):
    """Authentication failed (401)."""


class TypeSafePermissionDeniedError(TypeSafeAPIError):
    """Access was denied (403)."""


class TypeSafeNotFoundError(TypeSafeAPIError):
    """The resource was not found (404)."""


class TypeSafeUnprocessableEntityError(TypeSafeAPIError):
    """The request failed server validation (422)."""


class TypeSafeRateLimitError(TypeSafeAPIError):
    """The rate limit was exceeded (429)."""

    def __init__(self, status: int, body: object, headers: httpx2.Headers, message: str | None = None, endpoint: str | None = None) -> None:
        """Describe a rate-limit response with an optional message override."""
        super().__init__(status, body, headers, message, endpoint)
        self.retry_after_ms = parse_retry_after(headers)
        """The server's requested wait in milliseconds, or `None` if unavailable."""


class TypeSafeInternalServerError(TypeSafeAPIError):
    """The server failed to process the request (5xx)."""


class TypeSafeAPIConnectionError(TypeSafeError, ConnectionError):
    """A request failed without an HTTP response."""


class TypeSafeAPITimeoutError(TypeSafeAPIConnectionError, TimeoutError):
    """A request exceeded its configured timeout."""

    def __init__(self, timeout: float | httpx2.Timeout) -> None:
        """Describe a request timeout with its configured timeout setting."""
        super().__init__(timeout)
        self.timeout = timeout
        """The timeout setting used for the request, in seconds or as an `httpx2.Timeout`."""

    @override
    def __str__(self) -> str:
        """Return the formatted timeout message."""
        return f"Request timed out (timeout={self.timeout})."

    @override
    def __repr__(self) -> str:
        """Represent the error using its formatted message."""
        return f"{type(self).__name__}({str(self)!r})"


class TypeSafeAPIResponseValidationError(TypeSafeAPIError):
    """A successful HTTP response whose body was missing or structurally invalid required data."""

    def __init__(self, status: int, body: Any, headers: httpx2.Headers, field_path: str, endpoint: str | None = None) -> None:
        """Describe an unparseable response, naming the first missing or structurally invalid field."""
        self.field_path = field_path
        """Dotted path to the offending field, such as `answers.tone.confidence`."""
        super().__init__(status, body, headers, f"Invalid response data at {field_path!r}.", endpoint)
        # This constructor takes a field path as its fourth argument, not a message.
        self.args = (status, body, headers, field_path, endpoint)


STATUS_ERROR_TYPES: dict[int, type[TypeSafeAPIError]] = {
    400: TypeSafeBadRequestError,
    401: TypeSafeAuthenticationError,
    403: TypeSafePermissionDeniedError,
    404: TypeSafeNotFoundError,
    422: TypeSafeUnprocessableEntityError,
    429: TypeSafeRateLimitError,
}


def api_error(status: int, body: object, headers: httpx2.Headers, endpoint: str | None = None) -> TypeSafeAPIError:
    error_type = STATUS_ERROR_TYPES.get(status, TypeSafeInternalServerError if status >= HTTPStatus.INTERNAL_SERVER_ERROR else TypeSafeAPIError)
    return error_type(status, body, headers, endpoint=endpoint)
