---
title: CLI
rank: 10
---

# CLI

The Sail CLI provides commands for interacting with Sail from the terminal.
You can start and manage Sail servers, run PySpark scripts, and more.

## One-Shot Execution

Starting with Sail 0.5.3, you can use the `sail spark run` command to run any PySpark script without explicitly provisioning a server.
A local Sail server starts instantly when you run the command and automatically stops when the script finishes.

The script can access the Spark session through the `spark` variable, which connects to the local Sail server using the Spark Connect protocol.

### Piping a Script

You can pipe simple PySpark code to the `sail spark run` command directly.

```bash
echo 'spark.sql("SELECT 1 + 1").show()' | sail spark run
```

### Using a Heredoc

For more complex scripts, you can use a heredoc.

```bash
cat <<EOF | sail spark run
import pyspark.sql.functions as F

df = spark.createDataFrame([(1, 2), (2, 3)], ["a", "b"])
df = df.withColumn("sum", F.col("a") + F.col("b"))
df.show()
EOF
```

### Running a Script File

You can also write the PySpark script to a file and run it by specifying the file path with the `-f` option.

```bash
sail spark run -f script.py
```

## Using with Agents

The `sail spark run` command can also be exposed as an agent skill, enabling LLM agents to perform data processing tasks using PySpark.
See the [Agent Skills](/guide/integrations/agent-skills) page for more details.
