---
title: Ibis
rank: 4
---

# Ibis

[Ibis](https://ibis-project.org/) is a portable Python dataframe library.
Because Sail is a Spark Connect server, Ibis can run your existing PySpark-backend code against Sail with a one-line change.

## Installation

Install Ibis with the PySpark backend, along with a Spark Connect client.

```bash
pip install "ibis-framework[pyspark]>=12,<13"
```

Then install and run Sail by following the [Getting Started](/introduction/getting-started/) guide.

## Connecting to Sail

Ibis's PySpark backend accepts any `SparkSession`.
Build one that points at your running Sail server using the `sc://` URL, and pass it to `ibis.pyspark.connect`.

```python
import ibis
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
con = ibis.pyspark.connect(session=spark)
```

If you already use Ibis against a local JVM Spark or a Spark cluster, this is the only change required.
The rest of your Ibis code works unchanged.

::: info
Replace `localhost:50051` with the address of your Sail server.
:::

::: tip
For exploratory work in a notebook or REPL, enable Ibis's interactive mode so expressions render as tables instead of unevaluated objects.

```python
ibis.options.interactive = True
```

:::

::: info
Sail is tested against Ibis 12.x. Newer versions may work but are not yet verified.
:::

## Example

```python
import ibis
from ibis import _
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
con = ibis.pyspark.connect(session=spark)

users = con.read_parquet("/data/users.parquet", table_name="users")

result = (
    users
    .filter(_.age > 30)
    .group_by("country")
    .aggregate(n=_.count(), avg_age=_.age.mean())
    .order_by(ibis.desc("n"))
    .execute()
)

print(result)
```

Ibis compiles the expression to PySpark operations, ships it over Spark Connect, and Sail executes it.
`execute()` returns a pandas DataFrame.

You can also run raw SQL through the same connection.

```python
con.sql("SELECT country, COUNT(*) AS n FROM users GROUP BY country").execute()
```

## Deploying in Production

For production use, run Sail on your own infrastructure and have your Ibis client connect to it over the network.
See the [Deployment](/guide/deployment/) guide for how to run Sail on Kubernetes or in a container.

Once Sail is running, point Ibis at its address. The client code is the same as local use — only the `sc://` URL changes.

```python
import ibis
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://sail.internal:50051").getOrCreate()
con = ibis.pyspark.connect(session=spark)
```

Your Ibis client can run anywhere that has network access to the Sail server — an application server, an Airflow worker, a notebook environment, or a data science workstation. Ibis itself is a thin client over Spark Connect, so it does not need to be co-located with Sail.

::: info
Sail serves Spark Connect over gRPC on the configured address. Network-level access control (private networking, ingress rules, VPN, port-forwarding for Kubernetes) is handled at your deployment layer, not inside Ibis or the Spark Connect client.
:::

## Why Use Ibis with Sail

- **Keep your code portable.** The same Ibis expressions can target DuckDB, BigQuery, Snowflake, and other backends. Using Sail does not lock you in.
- **Skip the JVM.** `sail spark server` plus Ibis gives you the full dataframe experience locally with no JVM to manage.
- **Drop-in for existing PySpark-backend users.** Point your `SparkSession` at Sail and your Ibis workflow runs on Sail's engine.
