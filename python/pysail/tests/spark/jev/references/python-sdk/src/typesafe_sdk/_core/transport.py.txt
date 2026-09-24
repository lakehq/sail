"""Shared HTTP request preparation, logging, and dispatch to response types."""

import logging
import platform
import sys
import time
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generic

import httpx2
from pydantic_core import PydanticSerializationError
from tenacity import AsyncRetrying, Retrying

from typesafe_sdk._core.config import Config, resolve_timeout
from typesafe_sdk._core.constants import (
    ACCEPT_HEADER,
    AUTHORIZATION_HEADER,
    CONTENT_TYPE_HEADER,
    JSON_CONTENT_TYPE,
    REQUEST_ID_HEADER,
    RETRY_COUNT_HEADER,
    RUNTIME_HEADER,
    SDK_HEADER,
    SDK_NAME,
    USER_AGENT_HEADER,
)
from typesafe_sdk._core.errors import TypeSafeAPIConnectionError, TypeSafeAPITimeoutError, TypeSafeError
from typesafe_sdk._core.json import serialize
from typesafe_sdk._core.logging import logger, redact_exception
from typesafe_sdk._core.retry import RetryPolicy, build_tenacity, build_tenacity_async
from typesafe_sdk._core.schemas.base import ResponseT, parse_response
from typesafe_sdk._version import __version__

RUNTIME = f"python/{platform.python_version()} ({sys.platform}; {platform.machine()})"


@dataclass(frozen=True, repr=False)
class Request(Generic[ResponseT]):
    """An immutable description of a single HTTP request to send."""

    method: str
    url: str
    headers: httpx2.Headers
    content: bytes | None
    timeout: float | httpx2.Timeout
    response_type: type[ResponseT]


class RequestState(Generic[ResponseT]):
    """Per-send mutable state: tracks attempt count and timing across retries of one `Request`."""

    def __init__(self, request: Request[ResponseT]) -> None:
        self._request = request
        self.attempts = 0
        self.started = 0.0

    def _log_wire(self, arrow: str, headers: httpx2.Headers, body: bytes | None) -> None:
        # Headers go through `args["headers"]` so SensitiveHeadersFilter redacts them.
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "%(method)s %(url)s %(arrow)s headers=%(headers)s body=%(body)r",
                {"method": self._request.method, "url": self._request.url, "arrow": arrow, "headers": dict(headers), "body": body},
            )

    @contextmanager
    def attempt(self) -> Generator[httpx2.Headers, None, None]:
        request = self._request
        headers = httpx2.Headers(request.headers)
        if self.attempts:
            headers[RETRY_COUNT_HEADER] = str(self.attempts)
            logger.info("%s %s retry %s", request.method, request.url, self.attempts)
        self.attempts += 1
        self.started = time.monotonic()
        self._log_wire("->", headers, request.content)
        try:
            yield headers
        except httpx2.RequestError as error:
            logger.info("%s %s <- %s", request.method, request.url, type(error).__name__)
            safe_error = redact_exception(error, headers)
            sdk_error: TypeSafeAPIConnectionError
            if isinstance(error, httpx2.TimeoutException):
                sdk_error = TypeSafeAPITimeoutError(request.timeout)
            else:
                sdk_error = TypeSafeAPIConnectionError(f"Connection error: {safe_error}")
            try:
                raise sdk_error from safe_error
            finally:
                # Raising inside this handler implicitly attaches the unredacted original.
                sdk_error.__context__ = None

    def parse(self, response: httpx2.Response) -> ResponseT:
        request = self._request
        logger.info(
            "%s %s <- %s in %.0fms (request %s)",
            request.method,
            request.url,
            response.status_code,
            (time.monotonic() - self.started) * 1000,
            response.headers.get(REQUEST_ID_HEADER, "-"),
        )
        self._log_wire("<-", response.headers, response.content)
        return parse_response(response, request.response_type)


def prepare(
    config: Config,
    method: str,
    path: str,
    body: object,
    timeout: float | httpx2.Timeout | None,
    headers: Mapping[str, str] | None,
    response_type: type[ResponseT],
) -> Request[ResponseT]:
    merged = httpx2.Headers(config.default_headers)
    merged.update(headers or {})
    merged.pop(RETRY_COUNT_HEADER, None)
    merged.update(
        {
            AUTHORIZATION_HEADER: f"Bearer {config.api_key}",
            ACCEPT_HEADER: JSON_CONTENT_TYPE,
            USER_AGENT_HEADER: f"{SDK_NAME}/{__version__}",
            SDK_HEADER: f"{SDK_NAME}/{__version__}",
            RUNTIME_HEADER: RUNTIME,
        }
    )
    content: bytes | None = None
    if body is not None:
        content = _encode_body(body)
        merged[CONTENT_TYPE_HEADER] = JSON_CONTENT_TYPE
    return Request(
        method,
        config.base_url + path,
        merged,
        content,
        resolve_timeout(config.timeout if timeout is None else timeout),
        response_type,
    )


def _encode_body(body: object) -> bytes:
    try:
        return serialize(body)
    except (PydanticSerializationError, TypeError, ValueError) as error:
        raise TypeSafeError("The request body could not be encoded as JSON") from error


def send(http_client: httpx2.Client, retry: Retrying, request: Request[ResponseT], override: RetryPolicy | None = None) -> ResponseT:
    state = RequestState(request)

    def attempt() -> ResponseT:
        with state.attempt() as headers:
            response = http_client.request(
                request.method, request.url, content=request.content, headers=headers, timeout=request.timeout, auth=None
            )
            return state.parse(response)

    policy = retry if override is None else build_tenacity(override)
    return policy.copy()(attempt)


async def send_async(
    http_client: httpx2.AsyncClient, retry: AsyncRetrying, request: Request[ResponseT], override: RetryPolicy | None = None
) -> ResponseT:
    state = RequestState(request)

    async def attempt() -> ResponseT:
        with state.attempt() as headers:
            response = await http_client.request(
                request.method, request.url, content=request.content, headers=headers, timeout=request.timeout, auth=None
            )
            return state.parse(response)

    policy = retry if override is None else build_tenacity_async(override)
    return await policy.copy()(attempt)
