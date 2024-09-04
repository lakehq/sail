import code
import platform
import readline
from rlcompleter import Completer

from pysail.spark import SparkConnectServer


def run_spark_shell(port: int):
    import pyspark
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    namespace = {"spark": spark}
    readline.parse_and_bind("tab: complete")
    readline.set_completer(Completer(namespace).complete)

    python_version = platform.python_version()
    (build_number, build_date) = platform.python_build()
    # Simulate the messages similar to `shell.py` in PySpark.
    banner = rf"""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version {pyspark.__version__}
      /_/

Using Python version {python_version} ({build_number}, {build_date})
Client connected to the Sail Spark Connect server at localhost:{port}
SparkSession available as 'spark'."""
    code.interact(local=namespace, banner=banner, exitmsg="")


def main():
    import argparse
    import os

    parser = argparse.ArgumentParser(prog="spark.py")
    subparsers = parser.add_subparsers(dest="command", description="the command to run", required=True)
    subparser = subparsers.add_parser("server", description="start the Spark Connect server")
    subparser.add_argument(
        "--ip",
        type=str,
        default="127.0.0.1",
        help="the IP address to bind the server to",
    )
    subparser.add_argument("--port", type=int, default=50051, help="the port to bind the server to")
    subparser.add_argument("-C", "--directory", type=str, help="the directory to change to before starting the server")
    _ = subparsers.add_parser(
        "shell", description="start the PySpark shell with a Spark Connect server running in the background"
    )
    args = parser.parse_args()

    if args.command == "server":
        if args.directory:
            os.chdir(args.directory)
        server = SparkConnectServer(args.ip, args.port)
        server.init_telemetry()
        server.start(background=False)
    elif args.command == "shell":
        # Listen on only the loopback interface for security.
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        run_spark_shell(port)


if __name__ == "__main__":
    main()
