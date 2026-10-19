"""Integration tests for the endpoint proxy."""

from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from pysail.testing.utils.proxy import (
    Close,
    ConnectionAccepted,
    ConnectionClosed,
    ConnectionOpened,
    ConnectionRule,
    EndpointProxy,
    Forward,
    FrameDecoder,
    FrameReceived,
    FrameRule,
    ProxyCodec,
    ProxyEventStore,
    Replace,
    RuleApplied,
)

if TYPE_CHECKING:
    from collections.abc import Callable


pytestmark = pytest.mark.integration


_FRAME_COUNT = 2
_MAX_TEST_FRAME_LENGTH = 255


@dataclass(frozen=True)
class _TestFrame:
    payload: bytes


class _TestFrameDecoder(FrameDecoder[_TestFrame]):
    """Decode a one-byte payload length followed by that payload."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> list[_TestFrame]:
        self._buffer.extend(data)
        frames: list[_TestFrame] = []
        while self._buffer:
            length = self._buffer[0]
            if len(self._buffer) < length + 1:
                break
            frames.append(_TestFrame(bytes(self._buffer[1 : length + 1])))
            del self._buffer[: length + 1]
        return frames


class _TestCodec(ProxyCodec[_TestFrame]):
    def decoder(self, direction: str) -> _TestFrameDecoder:
        del direction
        return _TestFrameDecoder()

    def encode(self, frame: _TestFrame) -> bytes:
        if len(frame.payload) > _MAX_TEST_FRAME_LENGTH:
            msg = "test frame payload exceeds one-byte length"
            raise ValueError(msg)
        return bytes([len(frame.payload)]) + frame.payload


def _wait_until(predicate: Callable[[], bool], timeout: float = 1) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    msg = "condition was not met before timeout"
    raise AssertionError(msg)


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    received = bytearray()
    while len(received) < size:
        data = sock.recv(size - len(received))
        if not data:
            msg = "socket closed before receiving expected bytes"
            raise AssertionError(msg)
        received.extend(data)
    return bytes(received)


class _EchoServer:
    def __init__(self) -> None:
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        self._closed = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    @property
    def host(self) -> str:
        return "127.0.0.1"

    @property
    def port(self) -> int:
        return int(self._listener.getsockname()[1])

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._closed.set()
        self._listener.close()
        self._thread.join(timeout=1)

    def _serve(self) -> None:
        try:
            connection, _ = self._listener.accept()
        except OSError:
            return
        with connection:
            while not self._closed.is_set():
                try:
                    data = connection.recv(64 * 1024)
                except OSError:
                    return
                if not data:
                    return
                connection.sendall(data)


def test_proxy_codec_handles_fragmented_and_combined_frames() -> None:
    codec = _TestCodec()
    first = _TestFrame(b"one")
    second = _TestFrame(b"two")
    encoded = codec.encode(first) + codec.encode(second)
    decoder = codec.decoder("client_to_server")

    assert decoder.feed(encoded[:2]) == []
    assert decoder.feed(encoded[2:]) == [first, second]


def test_event_store_does_not_match_missing_attributes() -> None:
    events = ProxyEventStore()
    events.add(ConnectionOpened(connection_id=1))

    assert events.count(ConnectionOpened, missing=None) == 0


def test_endpoint_proxy_records_frames_and_replaces_a_response() -> None:
    codec = _TestCodec()
    upstream = _EchoServer()
    proxy = EndpointProxy("echo", upstream.host, upstream.port, codec=codec)
    response = _TestFrame(b"mutated")
    proxy.rules.add(
        FrameRule(
            _TestFrame,
            condition=lambda event: event.direction == "server_to_client",
            action=lambda _: Replace(response),
        )
    )
    upstream.start()
    proxy.start()

    request = _TestFrame(b"original")
    try:
        with socket.create_connection((proxy.host, proxy.port), timeout=1) as client:
            client.sendall(codec.encode(request))
            assert _recv_exact(client, len(codec.encode(response))) == codec.encode(response)

        _wait_until(lambda: proxy.events.count(FrameReceived) == _FRAME_COUNT)
        assert proxy.events.count(ConnectionAccepted) == 1
        assert proxy.events.count(ConnectionOpened) == 1
        assert proxy.events.count(FrameReceived, direction="client_to_server") == 1
        assert proxy.events.count(FrameReceived, direction="server_to_client") == 1
        assert proxy.events.count(RuleApplied) == 1
    finally:
        proxy.close()
        upstream.close()


def test_connection_rule_closes_the_next_accepted_socket() -> None:
    upstream = _EchoServer()
    proxy = EndpointProxy("echo", upstream.host, upstream.port)
    proxy.rules.add(ConnectionRule(action=lambda _: Close("injected")))
    upstream.start()
    proxy.start()

    try:
        with socket.create_connection((proxy.host, proxy.port), timeout=1) as client:
            client.settimeout(1)
            assert client.recv(1) == b""

        _wait_until(lambda: proxy.events.count(RuleApplied) == 1)
        assert proxy.events.count(ConnectionAccepted) == 1
        assert proxy.events.count(ConnectionOpened) == 0
    finally:
        proxy.close()
        upstream.close()


def test_close_active_connections_closes_delayed_connections() -> None:
    upstream = _EchoServer()
    proxy = EndpointProxy("echo", upstream.host, upstream.port)
    proxy.rules.add(ConnectionRule(action=lambda _: Forward(delay_seconds=1)))
    upstream.start()
    proxy.start()

    try:
        with socket.create_connection((proxy.host, proxy.port), timeout=1) as client:
            _wait_until(lambda: proxy.events.count(ConnectionAccepted) == 1)
            assert proxy.close_active_connections(reason="test") == 1
            client.settimeout(1)
            assert client.recv(1) == b""

        assert proxy.events.count(ConnectionClosed, reason="test") == 1
        assert proxy.events.count(ConnectionOpened) == 0
    finally:
        proxy.close()
        upstream.close()
