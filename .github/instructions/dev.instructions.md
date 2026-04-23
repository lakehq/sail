---
applyTo: "**"
excludeAgent: "code-review"
---

## Development

### Rust Development

- **Formatting**: `cargo +nightly fmt`
- **Linting (Clippy)**: `cargo clippy --all-targets --all-features -- -D warnings`
- **Build**: `cargo build`
- **Test**: `cargo nextest run`

Before committing changes, make sure to format your code and fix all Clippy warnings. Note that formatting uses the nightly Rust toolchain.

Tests use "gold data" files for validation. If you change logic that affects output, you may need to update these files:

```bash
env SAIL_UPDATE_GOLD_DATA=1 cargo nextest run
```

### Python Development

- **Formatting**: `hatch fmt`
- **Test**:
  1. Build the native extension and install the `pysail` package in editable mode for the `default` Hatch environment: `hatch run maturin develop`. Re-run this command if you modify Rust code. Python code changes are reflected immediately.
  2. Run tests using `pytest` via Hatch: `hatch run pytest`. You can pass additional arguments to `pytest` if needed. For example, use the `-k` flag to filter tests by keywords. The keywords can match tests by function names or file names and allow predicates with `and`, `or`, and `not`. In this way you do not need to select tests by the full path.

You must run `pytest` to ensure the relevant tests pass when making changes to Rust or Python code.

### Documentation Development

If you are working on documentation, run the following command first to install the dependencies:

```bash
pnpm install
```

- **Formatting**: `pnpm run format`
- **Linting**: `pnpm run lint`
- **Build**:
  1. Build the `pysail` package to prepare for API documentation generation: `hatch run docs:maturin develop`
  2. Build the API documentation using Sphinx: `hatch run docs:build`
  3. Build the documentation site using VitePress: `pnpm run docs:build`

Before committing changes, make sure to format and lint the files, and ensure the documentation site builds without errors.

You can skip API documentation generation if you are only working on files inside the `docs/` directory.

## Conventions and Practices

### Codec

When adding functions (`ScalarUDF` or `AggregateUDF`), physical expressions (`PhysicalExpr`), or physical plan nodes (`ExecutionPlan`), make sure to update `crates/sail-execution/src/codec.rs` so that the query plan can work in cluster mode.

### Test Style

Most functionalities in Sail can be described via SQL, and we prefer BDD tests in `.feature` files (Gherkin syntax) for them.
Such tests are loaded and executed in `pytest`.
We describe scenarios with `Given`, `When`, and `Then`. We assert user-facing outcomes rather than internal states.

The BDD tests are structured as follows:

- `python/pysail/tests/**/*.feature` (test case definitions)
- `python/pysail/tests/**/test*_features.py` (test loaders)
- `python/pysail/testing/**/steps/*.py` (step implementations)

Python tests are suitable for Spark DataFrame APIs. The BDD tests should already cover Spark SQL features comprehensively, and Python unit tests additionally ensure that the _interface_ is working properly.

Some BDD tests or Python tests rely on snapshot files. Run the `pytest` command with the `--snapshot-update` flag to update the snapshot files when necessary. You can combine it with the `-k` flag to only update snapshots for specific tests.

Rust tests are strongly discouraged unless you are testing internal utilities whose inputs are easy to construct in Rust and whose outputs are straightforward to verify. All testing around query execution and Arrow data should be done via BDD tests or Python tests.

## Contributing

Please make sure the pull request title follows the Conventional Commits specification: `<type>[(<scope>)]: <description>`.

1. Use `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, etc. for `<type>`.
2. Do not add `<scope>`. The optional `<scope>` is only allowed for dependabot pull requests.
3. `<description>` must start with a lowercase letter. You may use backticks to refer to code elements. But try to keep the description at a high level and only refer to code elements sparingly.
   If `<description>` contains words that should start with an uppercase letter (e.g., "SQL"),
   consider rephrasing it so that such words are not at the start.
