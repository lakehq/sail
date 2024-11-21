---
title: Getting Started
rank: 20
---

# Getting Started

In this guide, you will see how to use Sail as the compute engine for PySpark.

Install the required packages in your Python environment.

```bash-vue
pip install "pysail[spark]=={{ libVersion }}"
```

::: info
Please refer to the [Installation Guide](/guide/installation/) for more information about installing Sail.
:::

::: info
The way to invoke Sail has changed since Sail 0.1. In Sail 0.2, we have introduced the `sail` command-line interface (CLI) to interact with Sail.
:::

## Using the Sail PySpark Shell

You can start a PySpark shell from Sail, using the following command.

```bash
sail spark shell
```

::: info
The `sail` command is installed as an executable script as part of the `pysail` Python package. You can also invoke the Sail CLI via `python -m pysail`.
:::

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

server = SparkConnectServer()
server.start()
_, port = server.listening_address

spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()

spark.sql("SELECT 1 + 1").show()
```

## Running the Sail Spark Connect Server

You can use the following command to start a Spark Connect server powered by Sail.

```bash
env RUST_LOG=info sail spark server
```

By default, the server listens on port `50051` on `127.0.0.1`. You can change the listening address and port using the
`--ip` and `--port` options.
The `--help` option shows the available options for the server.

::: info
Currently, we use the `RUST_LOG` environment variable to control the logging level of the server.
This may change in the future.
:::

In another terminal, you can connect to the Sail Spark Connect server using PySpark.

```bash
env SPARK_CONNECT_MODE_ENABLED=1 SPARK_REMOTE="sc://localhost:50051" pyspark
```

::: info
You can refer to the [Spark documentation](https://spark.apache.org/docs/latest/spark-connect-overview.html)
for more information about Spark Connect.
:::

<script setup>
import { useData } from "vitepress";
import { computed } from "vue";

const { site } = useData();

const libVersion = computed(() => site.value.contentProps?.libVersion);
</script>
