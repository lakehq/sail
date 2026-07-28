::: warning

Due to a recent issue with the mimalloc library, the `scripts/spark-tests/run-server.sh` script may abort with a stack overflow error whenever Python code is involved.
This happens when the script runs the Sail server via `cargo run` on macOS.

To work around this issue, build the package first via `hatch run maturin develop` and then run the script with the `CI` environment variable set, which runs the server using the built package.

```bash
env CI=1 hatch run scripts/spark-tests/run-server.sh
```

:::
