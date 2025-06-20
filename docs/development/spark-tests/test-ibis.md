---
title: Running Ibis Tests
rank: 25
---

# Running Ibis Tests

You can run Ibis unit tests with the Spark backend connected to the Sail server.

::: info
The Ibis tests are not run in GitHub pull requests yet since the setup is not fully working.
But you can run them locally to work on missing Ibis features in Sail.
:::

## Running the Spark Connect Server

Use the following command to build and run the Sail Spark Connect server in the `test-ibis` environment.

```bash
hatch run test-ibis:scripts/spark-tests/run-server.sh
```

## Running the Tests

After running the server, start another terminal and use the following command to run the Ibis tests.

```bash
hatch run test-ibis:env \
  SPARK_REMOTE="sc://localhost:50051" \
  scripts/spark-tests/run-tests.sh
```

The test logs will be written to `tmp/spark-tests/<name>` where `<name>` is defined by
the `TEST_RUN_NAME` environment variable whose default value is `ibis`.

### Test Selection

You can run selected tests by passing `pytest` arguments to the script.

```bash
hatch run test-ibis:env \
  SPARK_REMOTE="sc://localhost:50051" \
  TEST_RUN_NAME=ibis-selected \
  scripts/spark-tests/run-tests.sh \
  --pyargs ibis.backends -m pyspark -k "test_array"
```

Note that for the above command, the test logs are written to the directory `tmp/spark-tests/ibis-selected`, due to the `TEST_RUN_NAME` environment variable.
