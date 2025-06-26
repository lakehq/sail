---
title: Getting Started
rank: 3
---

# Getting Started

In this guide, you will see how to use Sail as the compute engine for PySpark.
The Spark session communicates with the Sail server using the Spark Connect protocol.
You can refer to the [Spark documentation](https://spark.apache.org/docs/latest/spark-connect-overview.html) for more information about Spark Connect.

## Package Installation

Install the required packages in your Python environment.
You can choose the Spark version you want.

::: code-group

```bash-vue [Spark 4.0 (Client) ]
pip install "pysail=={{ libVersion }}"
pip install "pyspark-client==4.0.0"
```

```bash-vue [Spark 4.0]
pip install "pysail=={{ libVersion }}"
pip install "pyspark[connect]==4.0.0"
```

```bash-vue [Spark 3.5]
pip install "pysail=={{ libVersion }}"
pip install "pyspark[connect]==3.5.5
```

:::

::: info

- `pyspark-client` is a lightweight PySpark client introduced in Spark 4.0 while `pyspark` remains as the full PySpark package containing all the JARs. The lightweight client cannot execute queries by itself, and can only connect to a Spark Connect server.
- `pyspark[connect]` installs extra dependencies needed for Spark Connect. This is supported since Spark 3.4.
- Since Spark 4.0, there is also a wrapper package `pyspark-connect` that you can use, which is equivalent to `pyspark[connect]`.

:::

You can refer to the [Installation](/introduction/installation/) page for more information about installing Sail.

::: details Migrating from Earlier Versions of Sail

- Since Sail 0.2, the `sail` command-line interface (CLI) became the new way to interact with Sail.
- Since Sail 0.3, you can no longer run `pip install pysail[spark]` to install PySail along with PySpark (the `spark` "extra"). You must explicitly install PySpark and choose the version you want to use.
  :::

## Using the Sail PySpark Shell

You can start a PySpark shell from Sail, using the following command.

```bash
sail spark shell
```

You will see the banner and prompt similar to a regular PySpark shell.
The `SparkSession` instance is available as the `spark` local variable.
You can run Spark SQL queries or use the DataFrame API in the shell.
The `SparkSession` instance communicates with the Sail server started in the same Python interpreter process. The Sail server runs in the background.

::: info
The `sail` command is installed as an executable script as part of the `pysail` Python package. You can also invoke the Sail CLI via `python -m pysail`.
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

::: warning

The `pyspark` shell is not available if PySpark is installed via `pyspark-client`. To use the `pyspark` shell, you need to install `pyspark[connect]`.

:::

<script setup>
import { useData } from "vitepress";
import { computed } from "vue";

const { site } = useData();

const libVersion = computed(() => site.value.contentProps?.libVersion);
</script>
