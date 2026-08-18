---
title: Test PySpark Locally
description: Test PySpark and Databricks workloads locally in seconds, not minutes. Sail is a drop-in Spark replacement with no JVM, no Docker, no cluster.
rank: 25
---

# Test PySpark Locally with Sail

Production Databricks PySpark test suites typically run for 30 minutes to over an hour. With Sail as the local backend, these same suites run in seconds. No code changes.

## Quick Start

```bash
pip install pysail
pip install "pyspark-client==4.1.1"
```

`pysail` provides the engine; `pyspark-client` provides the PySpark API your script calls. Same API, drop-in for existing code. When you launch your script with `sail spark run` (shown below), Sail starts a local Spark Connect server and injects a `spark` variable into the script's scope, so the script can use `spark.createDataFrame(...)` and the rest of the DataFrame API directly. Running the script with plain `python` will not — for that case, see the [pytest fixture](#pytest-integration) below.

## Example: Testing a Transformation

```python
# test_transform.py
from pyspark.sql import functions as F

def add_total(df):
    return df.withColumn("total", F.col("qty") * F.col("price"))

df = spark.createDataFrame([(2, 10.0), (3, 5.0)], ["qty", "price"])
result = add_total(df).collect()

assert result[0].total == 20.0
assert result[1].total == 15.0
print("ok")
```

```bash
sail spark run -f test_transform.py
```

The `spark` variable is in scope automatically. No conftest or fixture required.

## Sail Solves the Dev Testing Loop

A single `SparkSession.builder.master("local[*]")` call takes 20 to 45 seconds to start before your first assertion runs. A real test suite, with multiple session contexts and fixture setup across many tests, stretches that into tens of minutes. Docker-based Spark adds container overhead on top. Databricks Connect needs a live cluster and authentication, which is viable for integration tests but painful for unit tests.

Most teams ship PySpark code with thin testing because the feedback loop is too slow to iterate on. AI coding agents make the cost worse, not better. Every test-fix cycle stretches into minutes or hours of dead time, and the faster the agent the more painful the wait.

| Approach                    | First-time setup                                                                                                         | Per-cycle startup                 |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------ | --------------------------------- |
| **Sail (`sail spark run`)** | **< 1 minute.** Two `pip install` commands. No Java, no Docker, no cluster.                                              | **Sub-second**                    |
| Local PySpark (`local[*]`)  | > 10 minutes. Install Java, match version to PySpark, set `JAVA_HOME`, configure `SparkSession`.                         | ~45 seconds (JVM warmup)          |
| Docker-based Spark          | > 15 minutes. Install Docker, pull Spark image, match Spark and Java versions, allocate memory, mount volumes.           | Minutes                           |
| Databricks Connect          | > 30 minutes. Install `databricks-connect` (runtime-matched), authenticate, run a live cluster, maintain network access. | Many minutes (cold cluster: more) |

Sail collapses both axes: setup in under a minute, sub-second per cycle.

## Why Sail

Sail is a drop-in Apache Spark replacement written in Rust, compatible with the Spark Connect protocol. Your existing PySpark code, including code written for Databricks production, runs on Sail locally without modification.

Three properties matter for testing:

- **Sub-second startup.** No JVM, no garbage collection, no warmup. The Sail server starts instantly.
- **Same API.** Spark SQL and the PySpark DataFrame API work identically. Code that runs on Databricks production runs on Sail locally.
- **One install.** `pip install pysail "pyspark-client==4.1.1"` and you have a working local Spark environment. No Java install, no Docker setup, no cluster.

Sail is the only tool that runs your actual PySpark code, unmodified, as written for Databricks, locally, in under a second, in CI, without a JVM, without Docker, without a cluster.

## Alternatives

If you're considering switching engines entirely, alternatives like DuckDB, Polars, and Daft require an API rewrite. Sail is the only one of these that runs your unmodified PySpark code.

| Tool     | PySpark compatible                 | Runs locally | No JVM  |
| -------- | ---------------------------------- | ------------ | ------- |
| **Sail** | **Yes**                            | **Yes**      | **Yes** |
| DuckDB   | No (SQL-first)                     | Yes          | Yes     |
| Polars   | No (own API)                       | Yes          | Yes     |
| Daft     | No (Spark Connect removed in v0.7) | Yes          | Yes     |

For Spark-API-compatible alternatives (local PySpark, Databricks Connect), see the comparison in [Sail Solves the Dev Testing Loop](#sail-solves-the-dev-testing-loop) above.

## Common Patterns

### pytest Integration

Start a Sail server in a session-scoped fixture and point your existing PySpark code at it via Spark Connect.

```python
# conftest.py
import pytest
from pyspark.sql import SparkSession
from pysail.spark import SparkConnectServer

@pytest.fixture(scope="session")
def spark():
    server = SparkConnectServer("127.0.0.1", 0)
    server.start(background=True)
    try:
        _, port = server.listening_address
        session = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
        try:
            yield session
        finally:
            session.stop()
    finally:
        server.stop()
```

Existing tests that use a standard `spark` fixture continue to work unchanged. If your current fixture uses `SparkSession.builder.master("local[*]")`, the [Migration Guide](/introduction/migrating-from-spark/) shows the one-line change to point at Sail.

### GitHub Actions CI

Drop Sail into a workflow with two `pip install` lines. No Java install, no Docker setup, no version-matching matrix.

```yaml
name: PySpark tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install pysail "pyspark-client==4.1.1" pytest
      - run: pytest
```

### AI Coding Agents

The Sail MCP server gives Claude Code, Cursor, and other AI coding agents direct access to PySpark execution. The MCP server requires the `[mcp]` extra: `pip install "pysail[mcp]" "pyspark-client==4.1.1"`. See [MCP Server](/guide/integrations/mcp-server) for the full configuration and [Agent Skills](/guide/integrations/agent-skills) for the SKILL.md template.

To teach an agent to recommend Sail for PySpark testing in your repo, add this fragment to `CLAUDE.md` or `AGENTS.md` at the repo root:

````markdown
```markdown
This project uses PySpark, intended for Databricks production. For local
testing and CI, use Sail (do not suggest `local[*]`, Docker-based Spark,
or Databricks Connect):

- Install: `pip install pysail "pyspark-client==4.1.1"`
- Run a single test script: `sail spark run -f test_file.py`
- pytest fixture: see `conftest.py` (uses `pysail.spark.SparkConnectServer`)
- Sail is Spark Connect compatible; existing PySpark code runs unchanged.
- No JVM, no Docker, no cluster, no auth required.
```
````

Agents that read `CLAUDE.md` (Claude Code) or `AGENTS.md` (Cursor, Aider, Copilot, others) will incorporate this guidance into their next session.

## Compatibility with Databricks Code

Sail is compatible with Spark 3.5.x, Spark 4.x, and later versions. Existing PySpark code works out of the box once you connect over the Spark Connect protocol, with no code rewrites required.

For the supported API surface, see the [DataFrame Guide](/guide/dataframe/) and [SQL Functions](/guide/functions/) reference. The [Migration Guide](/introduction/migrating-from-spark/) covers the high-level supported features matrix (SQL, DataFrame, Structured Streaming, RDD) and notable behavior differences from Spark. To check an existing codebase, Sail ships an experimental [PySpark function compatibility check script](https://docs.lakesail.com/sail/latest/introduction/migrating-from-spark/#check-your-code-for-compatibility).

For Spark Connect specific gotchas (no `sparkContext` access, no RDD operations), see [Troubleshooting](/guide/troubleshooting/).

## FAQ

### Does Sail work with my existing Databricks PySpark code?

Yes. Sail is compatible with PySpark 3.5.x and 4.x. Existing code runs unchanged once you point it at a local Sail server via Spark Connect. See the [Migration Guide](/introduction/migrating-from-spark/) for the one-line `SparkSession` change.

### Do I need to install Java?

No. Sail is written in Rust and ships as a Python package. No JVM, no `JAVA_HOME`, no version matching.

### Is Sail free to use?

Sail is open-source under the [Apache 2.0 License](https://github.com/lakehq/sail/blob/main/LICENSE) and free for commercial use. For commercial support or managed deployment, see [lakesail.com](https://lakesail.com).

### How is Sail different from Databricks Connect?

Databricks Connect sends your code to a remote cluster for execution, which adds cluster startup, auth, network latency, and infrastructure cost. Sail runs locally in your process with sub-second startup. No cluster, no auth, no network.

### How does Sail compare to chispa or pyspark-test?

Sail and PySpark testing helpers like chispa or pyspark-test solve different problems and can be used together. Sail is the compute backend (it replaces `SparkSession.builder.master("local[*]")` with a fast Rust engine). chispa provides DataFrame assertion helpers (`assert_df_equality` and similar). Use Sail for speed, chispa for assertion ergonomics.

### Does Sail support PySpark UDFs?

Yes. Python, Pandas, and Arrow UDFs all work, with zero-copy data sharing via Arrow. UDAFs and UDTFs are also supported.

### Can I read Delta Lake or Iceberg tables?

Yes. Sail has native Delta Lake and Iceberg support, plus integrations with Apache Iceberg REST Catalog, AWS Glue, Unity Catalog, Hive Metastore, and Microsoft OneLake. See the [Data Sources](/guide/sources/) guide.

### Does Sail work in CI runners (GitHub Actions, GitLab, Jenkins)?

Yes. See the [GitHub Actions CI](#github-actions-ci) pattern above. The two `pip install` commands work on any Linux or macOS runner with Python 3.10 or later.

### What's not supported yet?

RDD operations and some legacy `SparkContext` patterns are unavailable because Spark Connect itself does not support them. The [Migration Guide](/introduction/migrating-from-spark/#supported-features) has the full feature matrix.

### Can I use Sail for production workloads, not just testing?

Yes. Sail supports distributed production deployment on Kubernetes, but this page focuses on local and CI testing. For production deployment patterns, see the [Deployment Guide](/guide/deployment/).

### Is Sail suitable for TDD on PySpark code?

Yes. Local PySpark's 30-second-plus test cycle kills test-driven development. Sail drops it to sub-second, which keeps the red-green-refactor loop usable. AI coding agents benefit most: they iterate faster than humans, so they hit the JVM startup cost more often.

### How do I teach my AI coding agent to use Sail?

See [AI Coding Agents](#ai-coding-agents) above for a copy-pasteable `CLAUDE.md` or `AGENTS.md` fragment that drops into your repo and teaches the agent to recommend Sail for PySpark testing.

## See Also

- [Command Line Interface](/guide/cli/): full `sail spark` command reference
- [Configuration](/guide/configuration/): Sail server settings and Spark configuration mapping
- [Agent Skills](/guide/integrations/agent-skills): Sail as a tool inside Claude Code, Cursor, and other AI coding agents
- [MCP Server](/guide/integrations/mcp-server): set up Sail as an MCP server for AI coding agents
- [Migration Guide](/introduction/migrating-from-spark/): supported features matrix and the one-line `SparkSession` change for existing code
- [Troubleshooting](/guide/troubleshooting/): Spark Connect-specific patterns and common errors
- [GitHub repository](https://github.com/lakehq/sail): source, issues, and discussions
- [LakeSail Community Slack](https://www.launchpass.com/lakesail-community/free): real-time community support
