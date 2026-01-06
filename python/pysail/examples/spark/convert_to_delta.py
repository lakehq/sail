from __future__ import annotations

import argparse
import os

import pyarrow.parquet as pq
from deltalake import write_deltalake

TABLE_NAMES = ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")


def convert_to_delta(input_path: str, output_path: str):
    """Convert TPC-H Parquet files to Delta Lake format."""
    os.makedirs(output_path, exist_ok=True)

    for table in TABLE_NAMES:
        print(f"Converting {table}...")
        parquet_path = f"{input_path}/{table}.parquet"
        delta_path = f"{output_path}/{table}"

        # Read parquet file
        arrow_table = pq.read_table(parquet_path)

        # Write as Delta Lake
        write_deltalake(delta_path, arrow_table, mode="overwrite")
        print(f"  -> {delta_path}")

    print("\nConversion complete!")


def main():
    parser = argparse.ArgumentParser(description="Convert TPC-H Parquet files to Delta Lake format")
    parser.add_argument("--input-path", type=str, required=True, help="Path to directory containing Parquet files")
    parser.add_argument("--output-path", type=str, required=True, help="Path to output Delta Lake tables")
    args = parser.parse_args()

    convert_to_delta(args.input_path, args.output_path)


if __name__ == "__main__":
    main()
