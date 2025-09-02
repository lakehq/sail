import contextlib
import platform
import socket
import threading
import time

import pytest

from pysail.tests.spark.utils import is_jvm_spark

if is_jvm_spark():
    pytest.skip("JVM spark does not support the socket format in SQL", allow_module_level=True)


class SocketServer:
    def __init__(self, host="localhost", port=0, data=()):
        self.host = host
        self.port = port
        self.data = list(data)
        self._socket = None
        self._thread = None
        self._running = False

    def start(self):
        if self._running:
            return
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        (_, self.port) = self._socket.getsockname()
        self._socket.listen(1)

        self._running = True
        self._thread = threading.Thread(target=self._run_server)
        self._thread.daemon = True
        self._thread.start()

    def stop(self):
        self._running = False
        if self._socket is not None:
            # We need to explicitly shut down the socket to unblock `accept()`,
            # otherwise the thread may hang, and we will wait indefinitely in `join()`.
            with contextlib.suppress(OSError):
                self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
            self._socket = None
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    def _run_server(self):
        while self._running:
            try:
                client_socket, _ = self._socket.accept()
                client_socket.settimeout(1)
                try:
                    for line in self.data:
                        if not self._running:
                            break
                        client_socket.send(f"{line}\n".encode())
                finally:
                    client_socket.close()
            except OSError:
                continue

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def probe(self, attempt=3, interval=0.1) -> bool:
        for _ in range(attempt):
            try:
                with socket.create_connection((self.host, self.port)):
                    return True
            except (ConnectionRefusedError, OSError):
                time.sleep(interval)
        return False


@pytest.fixture(scope="module", autouse=True)
def tables(spark):
    with SocketServer(data=["hello", "world"]) as server:
        if not server.probe():
            msg = "the socket server is not ready"
            raise RuntimeError(msg)

        spark.sql(f"""
            CREATE TABLE t1
            USING socket
            OPTIONS (
                host '{server.host}',
                port '{server.port}'
            )
        """)
        spark.sql("""
            CREATE TABLE t2_invalid_port
            USING socket
            OPTIONS (
                host 'localhost',
                port '0'
            )
        """)
        spark.sql("""
            CREATE TABLE t3_connection_failure
            USING socket
            OPTIONS (
                host 'localhost',
                port '65535'
            )
        """)
        yield
        spark.sql("DROP TABLE t1")
        spark.sql("DROP TABLE t2_invalid_port")
        spark.sql("DROP TABLE t3_connection_failure")


def test_socket_basic(spark):
    actual = spark.sql("SELECT * FROM t1 LIMIT 2").collect()
    assert actual == [("hello",), ("world",)]


def test_socket_with_projection(spark):
    actual = spark.sql("SELECT value, value FROM t1 LIMIT 1").collect()
    assert actual == [("hello", "hello")]


def test_socket_with_filtering(spark):
    actual = spark.sql("SELECT * FROM t1 WHERE value LIKE '%w%' LIMIT 1").collect()
    assert actual == [("world",)]


def test_socket_invalid_port(spark):
    with pytest.raises(Exception, match="port"):
        spark.sql("SELECT * FROM t2_invalid_port LIMIT 1").collect()


@pytest.mark.skipif(platform.system() == "Windows", reason="not working on Windows")
def test_socket_connection_failure(spark):
    with pytest.raises(Exception, match="refused"):
        spark.sql("SELECT * FROM t3_connection_failure LIMIT 1").collect()
