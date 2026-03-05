import os
import subprocess

from pyspark.sql import SparkSession

from pysail.spark import SparkConnectServer


def main():
    # Get Azure access token - use environment variable if set, otherwise use Azure CLI
    token = os.environ.get("AZURE_STORAGE_TOKEN")
    if not token:
        print("AZURE_STORAGE_TOKEN not set, fetching from Azure CLI...")
        result = subprocess.run(
            [  # noqa: S607
                "az.cmd",
                "account",
                "get-access-token",
                "--resource",
                "https://storage.azure.com/",
                "--query",
                "accessToken",
                "-o",
                "tsv",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        token = result.stdout.strip()
    else:
        print("Using AZURE_STORAGE_TOKEN from environment")

    # Set up environment with the token
    os.environ["AZURE_STORAGE_TOKEN"] = token
    os.environ["SAIL_CATALOG__LIST"] = '[{type="onelake", name="onelake", url="duckrun/data.Lakehouse"}]'
    os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "onelake"

    # Start Sail server
    print("Starting Sail server...")
    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    _, port = server.listening_address
    print(f"Server running on port {port}")

    try:
        # Connect via PySpark
        spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()

        # Test: Show schemas in OneLake
        print("\n=== SHOW SCHEMAS ===")
        spark.sql("SHOW SCHEMAS").show()

        # Test: Show tables in aemo schema (without USE)
        print("\n=== SHOW TABLES IN aemo ===")
        spark.sql("SHOW TABLES IN aemo").show()

        # Test: Query a table with fully qualified name
        print("\n=== SELECT * FROM aemo.calendar LIMIT 5 ===")
        spark.sql("SELECT * FROM aemo.calendar LIMIT 5").show()

        spark.stop()

    finally:
        print("\nStopping server...")
        server.stop()
        print("Done!")


if __name__ == "__main__":
    main()
