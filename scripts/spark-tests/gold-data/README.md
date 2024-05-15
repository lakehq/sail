# Gold Data Generation for Spark Tests

This directory contains scripts to generate gold data for Spark tests.

The idea of gold data is to keep track of the **actual** output for a set of test cases.
We can then review test output changes when we modify the code.
This ensures that we do not alter the behavior of the code in unintended ways.

This is similar to `sqllogictest`, which is designed by SQLite, and whose Rust implementation
is used by DataFusion and various other projects.
However, we choose to use JSON format for the gold data (as opposed to the text format proposed by
`sqllogictest`), so that we can serialize text containing special characters reliably.
The JSON format also makes it easier to manipulate the data programmatically.

The Spark project leverages the idea of gold data in its tests as well.

## Bootstrapping Gold Data

In the project root directory, run the following command to collect test cases
from the Spark project.
This will rewrite the gold data files with only the test input.
You should then use the command in the next section to save the test output.

```bash
scripts/spark-tests/gold-data/bootstrap.sh
```

This is not part of the daily developer workflow.
This only needs to be done once, when we update the bootstrap scripts, or when we
upgrade the Spark version.

## Updating Test Output in Gold Data

As part of the daily developer workflow, you need to run the following command to
save the test output every time you modify the behavior of the code.

Please commit the gold data changes so that they can be part of the code review.

```bash
env SPARK_UPDATE_GOLD_DATA=1 cargo test -p spark-connect-server --lib
```

Now you can run `cargo test` without the `SPARK_UPDATE_GOLD_DATA` environment variable
to make sure the code produces the same test output as the gold data.
