---
title: Spark Gold Data
rank: 32
---

# Working with Spark Gold Data

We use gold data to record Sail's behavior for Spark SQL test cases.

The `scripts/spark-gold-data` directory contains scripts to generate gold data for Spark tests. The gold data files are stored in the `crates/sail-spark-connect/tests/gold_data` directory.

## Overview

The idea of gold data is to keep track of the **actual** output for a set of test cases.
We can then review test output changes when we modify the code.
This ensures that we do not alter the behavior of the code in unintended ways.

This is similar to `sqllogictest`, which was designed by SQLite and whose Rust implementation
is used by DataFusion and various other projects.
However, we choose to use JSON format for the gold data (as opposed to the text format proposed by
`sqllogictest`), so that we can serialize text containing special characters reliably.
The JSON format also makes it easier to manipulate the data programmatically.

The Spark project leverages the idea of gold data in its tests as well.

## Updating Test Output

As part of the daily developer workflow, you need to run the following command to
save the test output every time you modify the behavior of the code.

Please commit the gold data changes so that they can be part of the code review.

```bash
env SAIL_UPDATE_GOLD_DATA=1 cargo test
```

Now you can run `cargo test` without the `SAIL_UPDATE_GOLD_DATA` environment variable
to make sure the code produces the same test output as the gold data.
