from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from pysail.spark import SparkConnectServer

logger = logging.getLogger(__name__)


def _parse_sail_env_marker_token(token: str) -> dict[str, str]:
    if token.startswith("sail_env__"):
        payload = token[len("sail_env__") :]
        if "__" in payload:
            k, v = payload.rsplit("__", 1)
            return {k: v}
        return {}

    if not token.startswith("sail_env_"):
        return {}

    payload = token[len("sail_env_") :]
    if "=" in payload:
        k, v = payload.split("=", 1)
        return {k: v}
    if "__" in payload:
        k, v = payload.rsplit("__", 1)
        return {k: v}
    return {}


def collect_sail_env(node) -> dict[str, str]:
    config: dict[str, str] = {}

    for marker in node.iter_markers(name="sail_env"):
        for k, v in marker.kwargs.items():
            config[k] = str(v)

    for marker in node.iter_markers():
        config.update(_parse_sail_env_marker_token(marker.name))

    keywords = getattr(node, "keywords", None)
    if keywords:
        for name in keywords:
            config.update(_parse_sail_env_marker_token(str(name)))

    return config


@dataclass
class _EnvBackup:
    original: dict[str, str | None]

    def restore(self) -> None:
        for k, v in self.original.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class SailServerManager:
    def __init__(self) -> None:
        self._server: SparkConnectServer | None = None
        self._current_env: dict[str, str] = {}

    def get_server_address(self, desired_env: dict[str, str]) -> str:
        desired_env = {k: str(v) for k, v in desired_env.items()}

        if self._server is not None:
            if not self._server.running:
                logger.info("Sail server found stopped. Restarting...")
                self._stop_server()
            elif desired_env != self._current_env:
                logger.info(
                    "Sail config changed. Restarting server.\nOld: %s\nNew: %s",
                    self._current_env,
                    desired_env,
                )
                self._stop_server()

        if self._server is None:
            self._start_server(desired_env)

        host, port = self._server.listening_address
        return f"sc://{host}:{port}"

    def _start_server(self, env_overrides: dict[str, str]) -> None:
        backup = _EnvBackup({k: os.environ.get(k) for k in env_overrides})
        os.environ.update(env_overrides)
        try:
            server = SparkConnectServer("127.0.0.1", 0)
            if os.environ.get("SAIL_TEST_INIT_TELEMETRY") == "1":
                server.init_telemetry()
            server.start(background=True)
            self._server = server
            self._current_env = env_overrides.copy()
        except Exception:
            backup.restore()
            raise
        finally:
            backup.restore()

    def _stop_server(self) -> None:
        if self._server is not None:
            self._server.stop()
            self._server = None
        self._current_env = {}

    def shutdown(self) -> None:
        self._stop_server()


SERVER_MANAGER = SailServerManager()

