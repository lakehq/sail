# Running Benchmarks

You can use the following command to run individual queries from the TPC-H benchmark.

```shell
python -m pysail.examples.spark.tpch \
  --data-path "$BENCHMARK_PATH"/tpch/data \
  --query-path "$BENCHMARK_PATH"/tpch/queries \
  --query 1
```

You need to run this in a Python virtual environment with PySail and PySpark installed.
If you are in the project source directory, you can use Hatch to set up the environment.
Run `hatch run maturin develop` to build and install PySail for the default Hatch environment,
and then run `hatch shell` to enter the environment.

`BENCHMARK_PATH` is the absolute path of the [DataFusion benchmarks repository](https://github.com/apache/datafusion-benchmarks).
Please follow the README in that repository to generate the TPC-H data.

- You can use `--console` instead of `--query [N]` to start a PySpark console and explore the TPC-H data interactively.
- You can use `--url` to specify the Spark URL. Use `local` for Spark local mode (JVM) and use `sc://localhost:50051` for Spark Connect (the default).
- You can use `--help` to see all the supported arguments.

Please use the following command to run the Spark Connect server.
It runs the server in release mode without debug logs.

```shell
env RUST_LOG=info BENCHMARK=1 scripts/spark-tests/run-server.sh
```
