"""TCP endpoint proxies for integration tests.

An :class:`EndpointProxy` forwards one listening endpoint to one upstream
endpoint.  Every accepted socket is represented by an
:class:`EndpointProxyConnection`. Rules and observations remain owned by the
endpoint proxy so tests can inject faults without coupling them to a specific
transport protocol.
"""

from __future__ import annotations

import socket
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar, cast

FrameT = TypeVar("FrameT")
_MISSING = object()


@dataclass(frozen=True, kw_only=True)
class ProxyEvent:
    """An event emitted by an endpoint proxy."""

    connection_id: int
    attributes: Mapping[str, object] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class ConnectionAccepted(ProxyEvent):
    """A client connected to the proxy listener."""

    peer: tuple[str, int]


@dataclass(frozen=True)
class ConnectionOpened(ProxyEvent):
    """The proxy established the paired upstream connection."""


@dataclass(frozen=True)
class ConnectionClosed(ProxyEvent):
    """A proxied connection was closed."""

    reason: str


@dataclass(frozen=True)
class FrameReceived(ProxyEvent, Generic[FrameT]):
    """A complete codec frame was received on one direction of a connection."""

    direction: str
    frame: FrameT


@dataclass(frozen=True)
class RuleApplied(ProxyEvent):
    """A rule selected a non-default forwarding decision."""

    rule_name: str
    decision_name: str


class ProxyEventStore:
    """Thread-safe, append-only proxy observations."""

    def __init__(self) -> None:
        self._events: list[ProxyEvent] = []
        self._lock = threading.Lock()

    def add(self, event: ProxyEvent) -> None:
        with self._lock:
            self._events.append(event)

    def count(self, event_type: type[ProxyEvent], /, **attributes: object) -> int:
        """Count events of ``event_type`` whose fields or attributes match."""
        with self._lock:
            events = tuple(self._events)
        return sum(self._matches(event, event_type, attributes) for event in events)

    def snapshot(self) -> tuple[ProxyEvent, ...]:
        """Return a stable event snapshot for assertions needing event details."""
        with self._lock:
            return tuple(self._events)

    @staticmethod
    def _matches(
        event: ProxyEvent,
        event_type: type[ProxyEvent],
        attributes: Mapping[str, object],
    ) -> bool:
        if not isinstance(event, event_type):
            return False
        for name, expected in attributes.items():
            actual = getattr(event, name, _MISSING)
            if actual is _MISSING:
                actual = event.attributes.get(name, _MISSING)
            if actual is _MISSING or actual != expected:
                return False
        return True


@dataclass(frozen=True)
class Forward:
    """Forward the original value, optionally after a delay."""

    delay_seconds: float = 0


@dataclass(frozen=True)
class Replace(Generic[FrameT]):
    """Forward a replacement frame, optionally after a delay."""

    frame: FrameT
    delay_seconds: float = 0


@dataclass(frozen=True)
class Discard:
    """Do not forward the current frame."""


@dataclass(frozen=True)
class Close:
    """Close the proxied connection."""

    reason: str


Decision = Forward | Replace[Any] | Discard | Close


class ProxyRule(ABC):
    """A stateful rule which may decide what happens to an observed event."""

    def observe(self, event: ProxyEvent) -> None:
        """Advance rule state for every observed connection or frame event."""

    @abstractmethod
    def apply(self, event: ProxyEvent) -> Decision | None:
        """Return a decision for this event, or ``None`` when it does not apply."""


ConnectionCondition = Callable[[ConnectionAccepted], bool]
ConnectionAction = Callable[[ConnectionAccepted], Decision]
FrameCondition = Callable[[FrameReceived[FrameT]], bool]
FrameAction = Callable[[FrameReceived[FrameT]], Decision]


def _always(_: object) -> bool:
    return True


@dataclass
class ConnectionRule(ProxyRule):
    """Apply a typed action when a client connection is accepted.

    The lock permits one rule instance to be shared by multiple endpoint proxies.
    """

    action: ConnectionAction
    condition: ConnectionCondition = _always
    count: int | None = 1
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def apply(self, event: ProxyEvent) -> Decision | None:
        if not isinstance(event, ConnectionAccepted):
            return None
        with self._lock:
            if self.count == 0 or not self.condition(event):
                return None
            decision = self.action(event)
            if self.count is not None:
                self.count -= 1
            return decision


@dataclass
class FrameRule(ProxyRule, Generic[FrameT]):
    """Apply a typed action to codec frames of ``frame_type``.

    The lock permits one rule instance to be shared by multiple endpoint proxies.
    """

    frame_type: type[FrameT]
    action: FrameAction[FrameT]
    condition: FrameCondition[FrameT] = _always
    count: int | None = 1
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def apply(self, event: ProxyEvent) -> Decision | None:
        if not isinstance(event, FrameReceived) or not isinstance(event.frame, self.frame_type):
            return None
        typed_event = cast("FrameReceived[FrameT]", event)
        with self._lock:
            if self.count == 0 or not self.condition(typed_event):
                return None
            decision = self.action(typed_event)
            if self.count is not None:
                self.count -= 1
            return decision


class ProxyRuleStore:
    """Thread-safe rule registration for an endpoint proxy."""

    def __init__(self) -> None:
        self._rules: list[ProxyRule] = []
        self._lock = threading.Lock()

    def add(self, rule: ProxyRule) -> None:
        """Append a rule. Earlier rules have priority over later rules."""
        with self._lock:
            self._rules.append(rule)

    def snapshot(self) -> tuple[ProxyRule, ...]:
        with self._lock:
            return tuple(self._rules)


class FrameDecoder(ABC, Generic[FrameT]):
    """Incrementally decode complete frames from one TCP direction."""

    @abstractmethod
    def feed(self, data: bytes) -> Iterable[FrameT]:
        """Return every complete frame now available after receiving ``data``."""


class ProxyCodec(ABC, Generic[FrameT]):
    """A connection-scoped framing codec used by an endpoint proxy."""

    @abstractmethod
    def decoder(self, direction: str) -> FrameDecoder[FrameT]:
        """Create an independent incremental decoder for one TCP direction."""

    @abstractmethod
    def encode(self, frame: FrameT) -> bytes:
        """Encode a frame for forwarding."""


class EndpointProxy:
    """A local TCP listener that forwards one endpoint to an upstream service."""

    def __init__(
        self,
        name: str,
        target_host: str,
        target_port: int,
        *,
        codec: ProxyCodec[Any] | None = None,
    ) -> None:
        self.name = name
        self.target = (target_host, target_port)
        self.codec = codec
        self.events = ProxyEventStore()
        self.rules = ProxyRuleStore()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        self._listener.settimeout(0.1)
        self._connections: dict[int, EndpointProxyConnection] = {}
        self._connection_lock = threading.Lock()
        self._dispatch_lock = threading.RLock()
        self._next_connection_id = 1
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

    def close_active_connections(self, *, reason: str = "") -> int:
        """Close every accepted connection active when this method is called."""
        with self._connection_lock:
            connections = tuple(self._connections.values())
        for connection in connections:
            connection.close(reason)
        return len(connections)

    def close(self) -> None:
        self._closed.set()
        self._listener.close()
        self.close_active_connections(reason="proxy closed")
        self._thread.join(timeout=1)

    def _serve(self) -> None:
        while not self._closed.is_set():
            try:
                client, peer = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return

            connection_id = self._allocate_connection_id()
            event = ConnectionAccepted(
                connection_id=connection_id,
                peer=(str(peer[0]), int(peer[1])),
                attributes={"endpoint": self.name},
            )
            decision = self.dispatch(event)
            if isinstance(decision, (Close, Discard)):
                client.close()
                self.record(
                    ConnectionClosed(
                        connection_id=connection_id,
                        reason=(decision.reason if isinstance(decision, Close) else "discarded connection"),
                        attributes={"endpoint": self.name},
                    )
                )
                continue
            if isinstance(decision, Replace):
                client.close()
                self.record(
                    ConnectionClosed(
                        connection_id=connection_id,
                        reason="invalid replacement for connection",
                        attributes={"endpoint": self.name},
                    )
                )
                continue

            connection = EndpointProxyConnection(
                proxy=self,
                connection_id=connection_id,
                client=client,
                connect_delay=decision.delay_seconds,
            )
            if self.register_connection(connection):
                connection.start()
            else:
                connection.close("proxy closed")

    def _allocate_connection_id(self) -> int:
        with self._dispatch_lock:
            connection_id = self._next_connection_id
            self._next_connection_id += 1
            return connection_id

    def dispatch(self, event: ProxyEvent) -> Decision:
        """Record and route a connection or frame event through ordered rules."""
        with self._dispatch_lock:
            self.events.add(event)
            rules = self.rules.snapshot()
            for rule in rules:
                rule.observe(event)
            for rule in rules:
                decision = rule.apply(event)
                if decision is not None:
                    self.events.add(
                        RuleApplied(
                            connection_id=event.connection_id,
                            rule_name=type(rule).__name__,
                            decision_name=type(decision).__name__,
                            attributes={"endpoint": self.name},
                        )
                    )
                    return decision
        return Forward()

    def record(self, event: ProxyEvent) -> None:
        """Record a lifecycle event and advance stateful rules."""
        with self._dispatch_lock:
            self.events.add(event)
            for rule in self.rules.snapshot():
                rule.observe(event)

    def register_connection(self, connection: EndpointProxyConnection) -> bool:
        """Track an accepted connection unless the endpoint proxy has already closed."""
        with self._connection_lock:
            if self._closed.is_set():
                return False
            self._connections[connection.connection_id] = connection
            return True

    def remove_connection(self, connection_id: int) -> None:
        """Remove a closed endpoint-proxy connection from the active registry."""
        with self._connection_lock:
            self._connections.pop(connection_id, None)


class EndpointProxyConnection:
    """One client socket paired with one upstream socket owned by an endpoint proxy."""

    def __init__(
        self,
        *,
        proxy: EndpointProxy,
        connection_id: int,
        client: socket.socket,
        connect_delay: float,
    ) -> None:
        self.proxy = proxy
        self.connection_id = connection_id
        self._client = client
        self._upstream: socket.socket | None = None
        self._connect_delay = connect_delay
        self._closed = threading.Event()
        self._close_lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def close(self, reason: str) -> None:
        with self._close_lock:
            if self._closed.is_set():
                return
            self._closed.set()
            self._close_socket(self._client)
            if self._upstream is not None:
                self._close_socket(self._upstream)
        self.proxy.remove_connection(self.connection_id)
        self.proxy.record(
            ConnectionClosed(
                connection_id=self.connection_id,
                reason=reason,
                attributes={"endpoint": self.proxy.name},
            )
        )

    def _run(self) -> None:
        try:
            if self._closed.wait(self._connect_delay):
                return
            self._upstream = socket.create_connection(self.proxy.target, timeout=1)
            self._upstream.settimeout(None)
            if self._closed.is_set():
                self._close_socket(self._upstream)
                return
            self.proxy.record(
                ConnectionOpened(
                    connection_id=self.connection_id,
                    attributes={"endpoint": self.proxy.name},
                )
            )

            client_to_upstream = threading.Thread(
                target=self._relay,
                args=("client_to_server", self._client, self._upstream),
                daemon=True,
            )
            upstream_to_client = threading.Thread(
                target=self._relay,
                args=("server_to_client", self._upstream, self._client),
                daemon=True,
            )
            client_to_upstream.start()
            upstream_to_client.start()
            client_to_upstream.join()
            upstream_to_client.join()
            self.close("connection ended")
        except OSError as exc:
            self.close(f"upstream connection failed: {exc}")

    def _relay(self, direction: str, source: socket.socket, destination: socket.socket) -> None:
        decoder = self.proxy.codec.decoder(direction) if self.proxy.codec is not None else None
        try:
            while not self._closed.is_set():
                data = source.recv(64 * 1024)
                if not data:
                    with self._close_lock:
                        if not self._closed.is_set():
                            with suppress(OSError):
                                destination.shutdown(socket.SHUT_WR)
                    return

                if decoder is None:
                    destination.sendall(data)
                    continue

                for frame in decoder.feed(data):
                    event = FrameReceived(
                        connection_id=self.connection_id,
                        direction=direction,
                        frame=frame,
                        attributes={"endpoint": self.proxy.name},
                    )
                    decision = self.proxy.dispatch(event)
                    if isinstance(decision, Close):
                        self.close(decision.reason)
                        return
                    if isinstance(decision, Discard):
                        continue
                    outbound_frame = decision.frame if isinstance(decision, Replace) else frame
                    if decision.delay_seconds:
                        time.sleep(decision.delay_seconds)
                    destination.sendall(self.proxy.codec.encode(outbound_frame))
        except (OSError, ValueError) as exc:
            if not self._closed.is_set():
                self.close(f"relay failed: {exc}")

    @staticmethod
    def _close_socket(sock: socket.socket) -> None:
        with suppress(OSError):
            sock.shutdown(socket.SHUT_RDWR)
        sock.close()
