---
title: Analyzing Spark Test Logs
rank: 30
---

# Analyzing Spark Test Logs

Here are some useful commands to analyze Spark test logs.
You can replace `test.jsonl` with a different log file name if you are analyzing a different test suite.

- Get the error counts for failed tests.
  ```bash
  # You can remove the `--slurpfile baseline tmp/spark-tests/baseline/test.jsonl` arguments
  # if you do not have baseline test logs.
  jq -r -f scripts/spark-tests/count-errors.jq \
    --slurpfile baseline tmp/spark-tests/baseline/test.jsonl \
    tmp/spark-tests/latest/test.jsonl | less
  ```
- Show a sorted list of passed tests.
  ```bash
  jq -r -f scripts/spark-tests/show-passed-tests.jq \
    tmp/spark-tests/latest/test.jsonl | less
  ```
- Show the differences of passed tests between two runs.
  ```bash
  diff -U 0 \
    <(jq -r -f scripts/spark-tests/show-passed-tests.jq tmp/spark-tests/baseline/test.jsonl) \
    <(jq -r -f scripts/spark-tests/show-passed-tests.jq tmp/spark-tests/latest/test.jsonl)
  ```
