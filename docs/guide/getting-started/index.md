---
title: Getting Started
rank: 20
---

# Getting Started

In this guide, you will see how to use Sail as the compute engine for PySpark.

Install the required packages in your Python environment.

```bash
pip install 'pysail[spark]'
```

## Using the Sail PySpark Shell

You can start a PySpark shell from Sail, using the following command.

```bash
python -m pysail.spark shell
```

You will see the banner and prompt similar to a regular PySpark shell.
The `SparkSession` is available as the `spark` local variable.
You can run Spark SQL queries or use the DataFrame API in the shell.

::: info

Spark uses the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) protocol to communicate with Sail, which runs as a Spark Connect server in the background, in the same process.

:::

## Using the Sail Library

Alternatively, you can use the Sail library to start a Spark Connect server and connect to it using PySpark.
Here is an example Python script.

```python
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

server = SparkConnectServer("127.0.0.1", 0)
server.start(background=True)
host, port = server.listening_address

spark = SparkSession.builder.remote(f"sc://{host}:{port}").getOrCreate()

spark.sql("SELECT 1").show()
```
