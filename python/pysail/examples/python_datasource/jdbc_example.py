"""
Example: Using JDBC data source with Lakesail.

This demonstrates how to read from a JDBC database using Lakesail's JDBC format.
The JDBC format is automatically available on the Lakesail server - no imports needed!
"""

from pyspark.sql import SparkSession


def example_basic_jdbc_read():
    """Basic JDBC read using format("jdbc")."""
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    # Read from PostgreSQL using the JDBC format
    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/mydb")
        .option("dbtable", "orders")
        .option("user", "admin")
        .option("password", "secret")
        .option("engine", "connectorx")
        .load()
    )

    df.show()
    spark.stop()


def example_partitioned_jdbc_read():
    """JDBC read with partitioning for parallel execution."""
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    # Read with range partitioning on order_id column
    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/mydb")
        .option("dbtable", "orders")
        .option("user", "admin")
        .option("password", "secret")
        .option("engine", "connectorx")
        .option("partitionColumn", "order_id")
        .option("lowerBound", "1")
        .option("upperBound", "1000000")
        .option("numPartitions", "10")
        .load()
    )

    # This will be executed in parallel across 10 partitions
    df.show()
    spark.stop()


def example_sqlite():
    """Read from SQLite database."""
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:sqlite:/tmp/mydata.db")
        .option("dbtable", "users")
        .option("engine", "connectorx")
        .load()
    )

    df.show()
    spark.stop()


def example_python_format():
    """Direct Python format usage (advanced)."""
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    # Use the generic Python data source format explicitly
    df = (
        spark.read.format("python")
        .option("python_module", "pysail.jdbc.datasource")
        .option("python_class", "JDBCArrowDataSource")
        .option("url", "jdbc:postgresql://localhost:5432/mydb")
        .option("dbtable", "orders")
        .option("user", "admin")
        .option("password", "secret")
        .option("partitionColumn", "order_id")
        .option("lowerBound", "1")
        .option("upperBound", "1000000")
        .option("numPartitions", "10")
        .load()
    )

    df.show()
    spark.stop()


if __name__ == "__main__":
    print("JDBC Data Source Examples")
    print("=" * 70)
    print("\n1. Basic JDBC read:")
    print("   Use spark.read.format('jdbc')")
    print("\n2. Partitioned JDBC read:")
    print("   Add partitioning options for parallel execution")
    print("\n3. SQLite example:")
    print("   Works with any JDBC-compatible database")
    print("\n4. Direct Python format usage:")
    print("   Advanced usage with generic Python data source format")
    print("\n" + "=" * 70)
    print("\nUncomment one of the example functions above to run it.")

    # Uncomment to run an example:
    # example_basic_jdbc_read()
    # example_partitioned_jdbc_read()
    # example_sqlite()
    # example_python_format()
