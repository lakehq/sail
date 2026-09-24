"""Deterministic System One HTTP fixture shared by tests and the Jev benchmark."""

from __future__ import annotations

import json
import socket
import threading
import time
from collections import deque
from contextlib import suppress
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class JevMock:
    """A local HTTP service; no credentials or network access beyond loopback are needed.

    ``keep_requests=False`` keeps benchmark memory independent of the row count.
    Configure callbacks before starting a query, and call ``reset`` only after it ends.
    """

    def __init__(self, *, delay=0.0, keep_requests=True):
        self.delay = delay
        self.keep_requests = keep_requests
        self.lock = threading.Lock()
        self.reset()
        mock = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                super().setup()
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            def handle(self):
                # The client may cancel while the keep-alive loop awaits another request.
                with suppress(BrokenPipeError, ConnectionResetError):
                    super().handle()

            def log_message(self, *_args):
                pass

            def do_POST(self):
                self.respond()

            def do_GET(self):
                self.respond()

            def respond(self):
                encoded = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                body = json.loads(encoded) if encoded else None
                with mock.lock:
                    mock.active += 1
                    mock.peak_active = max(mock.peak_active, mock.active)
                    mock.request_count += 1
                    ordinal = mock.request_count
                    mock.request_bytes += len(encoded)
                    mock.peak_request_bytes = max(mock.peak_request_bytes, len(encoded))
                    if mock.keep_requests:
                        mock.requests.append(
                            {
                                "method": self.command,
                                "path": self.path,
                                "body": body,
                                "encoded": encoded,
                                "authorization": self.headers.get("Authorization"),
                                "started": time.monotonic(),
                            }
                        )
                    status, headers = mock.statuses.popleft() if mock.statuses else (HTTPStatus.OK, {})
                    mock.started.set()
                try:
                    delay = mock.delay(body, ordinal) if callable(mock.delay) else mock.delay
                    if delay:
                        time.sleep(delay)
                    if self.path not in {"/v1/systemone", "/v1/models"}:
                        status = HTTPStatus.NOT_FOUND
                    response = mock.response(body) if status == HTTPStatus.OK else mock.error_response
                    if mock.transform is not None and status == HTTPStatus.OK:
                        response = mock.transform(response, body)
                    output = json.dumps(response).encode()
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(output)))
                    self.send_header("x-typesafe-request-id", f"mock-{ordinal}")
                    for key, value in headers.items():
                        self.send_header(key, value)
                    self.end_headers()
                    self.wfile.write(output)
                except (BrokenPipeError, ConnectionResetError):
                    # Cancellation or an attempt deadline closes the client connection.
                    pass
                finally:
                    with mock.lock:
                        mock.active -= 1

        class Server(ThreadingHTTPServer):
            # Keep the listen backlog from serializing concurrent client connects.
            request_queue_size = 128

        self.server = Server(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_port}"

    def reset(self):
        self.requests = []
        self.statuses = deque()
        self.transform = None
        self.error_response = {"detail": "mock HTTP failure"}
        self.active = 0
        self.peak_active = 0
        self.request_count = 0
        self.request_bytes = 0
        self.peak_request_bytes = 0
        self.started = threading.Event()

    @staticmethod
    def response(body):
        if body is None:
            return {"models": [{"name": "jev-test", "description": "Mock model", "release_date": "2026-09-15"}]}
        answers = {}
        for key, question in body["questions"].items():
            kind = question["type"]
            if kind == "noul":
                # Numeric states let tests verify row ownership after out-of-order completion.
                state = body["state"]
                noul = int(state) / 100 if isinstance(state, str) and state.isdigit() else 0.75
                answer = {"type": kind, "noul": noul}
            elif kind == "choice":
                choices = list(question["criteria"])
                answer = {
                    "type": kind,
                    "choice": choices[0],
                    "confidence": 0.8,
                    "probabilities": {key: 1 / len(choices) for key in choices},
                }
            else:
                levels = question["criteria"]
                answer = {
                    "type": kind,
                    "score": (len(levels) - 1) / 2,
                    "confidence": 0.8,
                    "probabilities": {str(i): 1 / len(levels) for i in range(len(levels))},
                    "legend": {str(i): level for i, level in enumerate(levels)},
                }
            answer["provider_extra"] = {"preserved": True}
            answers[key] = answer
        return {"model": body["model"], "usage": {"input_tokens": 17, "output_tokens": 3}, "answers": answers}

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
