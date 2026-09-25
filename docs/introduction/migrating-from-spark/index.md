---
title: Migrating from Spark
rank: 4
---

# Migrating from Spark

Sail is a drop-in replacement for Apache Spark.
Suppose you have a Sail server running on `localhost:50051`, you only need to change the way you create a `SparkSession` in your PySpark code.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()  # [!code --]
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()  # [!code ++]
```

If you use [Ibis](https://ibis-project.org/) with its PySpark backend, the same one-line change applies. See the [Ibis](/guide/integrations/ibis) integration guide.

## Check Your Code for Compatibility

The [Spark-to-Sail scanner](https://migrate.lakesail.com/?from=sail-migrating) reports which Spark features your code uses, where each one appears, and whether Sail supports them today. It runs as WebAssembly in your browser or offline from the CLI.

To install the CLI:

::: code-group

```bash [uv]
uv tool install https://migrate.lakesail.com/dist/spark_to_sail-0.1.0-py3-none-any.whl
```

```bash [pip]
pip install https://migrate.lakesail.com/dist/spark_to_sail-0.1.0-py3-none-any.whl
```

```bash [venv]
python3 -m venv .venv && source .venv/bin/activate && pip install https://migrate.lakesail.com/dist/spark_to_sail-0.1.0-py3-none-any.whl
```

:::

To scan a file or directory:

<SyntaxBlock>
  <SyntaxText raw="'spark-to-sail scan '<directory>" />
</SyntaxBlock>

Run `spark-to-sail scan --help` to see what else it can do.

Sail also bundles an older utility script (experimental :construction:) that scans a folder for `*.py` and `*.ipynb` files and reports the Sail support status of the PySpark functions it finds. It only checks whether functions are _implemented_ in Sail and does not read Spark SQL strings. Prefer the scanner above. The script remains for offline use after installing Sail.

<SyntaxBlock>
  <SyntaxText
    raw="'python -m pysail.examples.spark.compatibility_check '<directory>"
  />
</SyntaxBlock>

The command also allows you to specify the desired output format using `--output=text` (human-readable), `--output=json`, or `--output=csv`.

::: info
Use either tool as a first pass. Neither runs your code, so they check which Spark features you reference but do **not** verify behavioral parity.
:::

## Considerations

Here we recommend some practices to help you adopt Sail in your production environment.

- Make sure you PySpark code works with a recent Spark version. Sail is designed to be compatible with Spark 3.5.x, Spark 4.x, and later versions. If your existing code was developed for Spark 2.x or earlier Spark 3.x versions, you may need to update it to make it working with newer versions of Spark.
- Prepare a small dataset to test your PySpark code with Sail. This will help you identify any potential issues before running it on a larger dataset.
- Run your PySpark code with Sail using larger datasets and compare the output with those from Spark. You may want to collect statistics or samples from the output to ensure the results are consistent.
- Do not overwrite existing data. Always write to a new location for each data processing job. This ensures that no data loss occurs if something goes wrong.

## Notable Differences

1. Sail has a different way to configure external data storage. The configuration options for Hadoop file systems (e.g. `s3a`) will not have effects in Sail. Instead, refer to the [Data Storage](/guide/storage/) guide for how to configure data storage in Sail.
1. Error messages returned by Sail may differ from those in Spark.
1. Many Spark configuration options do not have effects in Sail. They are either unsupported or irrelevant to Sail.
1. The `pyspark.sql.DataFrame.explain` method and the `EXPLAIN` SQL statement show the Sail logical and physical plans.

## Supported Features

Here is a summary of Spark features that are supported in Sail (:white_check_mark:), planned in our roadmap (:construction:), or unsupported due to technical limitations or low priorities (:x:).

Sail focuses on SQL and the DataFrame API, which are the most commonly used features in Spark applications.

There is no support for Spark RDD in Sail since it relies on the JVM implementation of Spark internals and is not covered by the Spark Connect protocol.

| Feature              | PySpark API                | Supported                    |
| -------------------- | -------------------------- | ---------------------------- |
| SQL                  | `pyspark.sql.SparkSession` | :white_check_mark:           |
| DataFrame            | `pyspark.sql.SparkSession` | :white_check_mark:           |
| Structured Streaming | `pyspark.sql.streaming`    | :white_check_mark: (partial) |
| Pandas on Spark      | `pyspark.pandas`           | :construction:               |
| RDD                  | `pyspark.SparkContext`     | :x:                          |

As you go through the rest of the documentation, you will find more details about the supported features as we cover different aspects of Sail.

<script setup>
import SyntaxBlock from "@theme/components/SyntaxBlock.vue";
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
