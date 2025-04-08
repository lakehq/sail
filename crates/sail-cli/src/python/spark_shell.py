import code
import platform
import readline
from rlcompleter import Completer

import pyspark
from pyspark.sql import SparkSession


def run_pyspark_shell(port: int):
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
