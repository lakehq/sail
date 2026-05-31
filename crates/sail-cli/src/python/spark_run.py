import sys
from pathlib import Path

from pyspark.sql import SparkSession


def read_script(file: str) -> tuple[str, str]:
    if file == "-":
        return (sys.stdin.read(), "<stdin>")

    path = Path(file)
    if not path.is_absolute():
        path = Path.cwd() / path
    path = path.resolve()

    return (path.read_text(), str(path))


def run_pyspark_script(port: int, file: str):
    source, filename = read_script(file)
    spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    scope = {
        "__name__": "__main__",
        "__file__": filename,
        "__package__": None,
        "spark": spark,
    }
    try:
        exec(compile(source, filename, "exec"), scope)  # noqa: S102
    finally:
        spark.stop()
