from __future__ import annotations

import argparse
import contextlib
import os
from pathlib import Path

import pyarrow.parquet as pq
from deltalake import write_deltalake
from pyiceberg.catalog.sql import SqlCatalog

TABLE_NAMES = ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")


def convert_to_delta(input_path: str, output_path: str):
    """Convert TPC-H Parquet files to Delta Lake format using deltalake-rs."""
    os.makedirs(output_path, exist_ok=True)

    for table in TABLE_NAMES:
        print(f"Converting {table}...")
        parquet_path = f"{input_path}/{table}.parquet"
        delta_path = f"{output_path}/{table}"

        arrow_table = pq.read_table(parquet_path)
        write_deltalake(delta_path, arrow_table, mode="overwrite")
        print(f"  -> {delta_path}")

    print("\nConversion complete!")


def convert_to_iceberg(input_path: str, output_path: str):
    """Convert TPC-H Parquet files to Iceberg format using pyiceberg."""
    input_dir = Path(input_path).resolve()
    output_dir = Path(output_path).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use fsspec file IO which handles local paths better on Windows
    warehouse_path = str(output_dir).replace("\\", "/")
    catalog = SqlCatalog(
        "tpch",
        **{
            "uri": f"sqlite:///{output_dir}/catalog.db",
            "warehouse": f"file:///{warehouse_path}",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )

    with contextlib.suppress(Exception):
        catalog.create_namespace("tpch")

    for table in TABLE_NAMES:
        print(f"Converting {table}...")
        parquet_path = input_dir / f"{table}.parquet"
        arrow_table = pq.read_table(str(parquet_path))

        with contextlib.suppress(Exception):
            catalog.drop_table(f"tpch.{table}")

        iceberg_table = catalog.create_table(f"tpch.{table}", schema=arrow_table.schema)
        iceberg_table.append(arrow_table)
        print(f"  -> tpch.{table}")

    print("\nConversion complete!")


def main():
    parser = argparse.ArgumentParser(description="Convert TPC-H Parquet files to Delta Lake or Iceberg format")
    parser.add_argument("--input-path", type=str, required=True, help="Path to directory containing Parquet files")
    parser.add_argument("--output-path", type=str, required=True, help="Path to output tables")
    parser.add_argument("--format", type=str, required=True, choices=["delta", "iceberg"], help="Output format")
    args = parser.parse_args()

    if args.format == "delta":
        convert_to_delta(args.input_path, args.output_path)
    else:
        convert_to_iceberg(args.input_path, args.output_path)


if __name__ == "__main__":
    main()
