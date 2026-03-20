---
name: run-pyspark-script
description: Runs a PySpark script.
---

You can run PySpark scripts via the `sail spark run` command.

Sail is an open-source unified and distributed multimodal computation framework
that can be used as a drop-in replacement for Apache Spark.
By using Sail to run PySpark scripts, you can perform data processing tasks
with the familiar PySpark DataFrame API or Spark SQL while benefiting from
the high performance and low memory overhead of Sail.

The script can refer to the Spark session via the `spark` variable, which
connects to a local Sail server via the Spark Connect protocol.
The Sail server starts instantly when you run the `sail spark run` command,
and it will be automatically stopped after the script finishes.

You can pipe simple PySpark code to the `sail spark run` command directly:

```bash
echo 'spark.sql("SELECT 1 + 1").show()' | sail spark run 2>/dev/null
```

Alternatively, you can use a heredoc for more complex PySpark scripts:

```bash
cat <<EOF | sail spark run 2>/dev/null
import pyspark.sql.functions as F

df = spark.createDataFrame([(1, 2), (2, 3)], ["a", "b"])
df = df.withColumn("sum", F.col("a") + F.col("b"))
df.show()
EOF
```

You can also write the PySpark script to a file and run it by specifying the
file path with the `-f` option:

```bash
echo 'spark.range(10).filter("id % 2 == 0").show()' > /tmp/script.py
sail spark run -f /tmp/script.py 2>/dev/null
```
