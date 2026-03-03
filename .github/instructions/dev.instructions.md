---
applyTo: "**"
excludeAgent: "code-review"
---

## Rust Development

- **Formatting**: `cargo +nightly fmt`
- **Linting (Clippy)**: `cargo clippy --all-targets --all-features -- -D warnings`
- **Build**: `cargo build`
- **Test**: `cargo nextest run`

Before committing changes, make sure to format your code and fix all Clippy warnings. Note that formatting uses the nightly Rust toolchain.

Tests use "gold data" files for validation. If you change logic that affects output, you may need to update these files:

```bash
env SAIL_UPDATE_GOLD_DATA=1 cargo nextest run
```

## Python Development

- **Formatting**: `hatch fmt`
- **Test**:
  1. Build the native extension and install the `pysail` package in editable mode for the `default` Hatch environment: `hatch run maturin develop`. Re-run this command if you modify Rust code. Python code changes are reflected immediately.
  2. Run Python tests using `pytest` via Hatch: `hatch run pytest`. You can pass additional arguments to `pytest` if needed. For example, use the `-k` flag to run specific tests.

## Documentation Development

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

### Test Style

- Prefer **BDD-style SQL integration tests** when behavior can be expressed in SQL.
- Organize scenarios with `Given / When / Then`; assert user-visible results (result/schema/error), not internals.
- Use unit tests mainly for logic that is hard to cover via SQL scenarios.
- Reference locations:
  - `python/pysail/tests/spark/**/features/*.feature` (BDD scenario definitions)
  - `python/pysail/tests/spark/**/test_features.py` (scenario loaders / test entrypoints)
  - `python/pysail/tests/spark/steps/*` (shared Given/When/Then step implementations)

## Contributing

Please make sure the pull request title follows the Conventional Commits specification: `<type>[(<scope>)]: <description>`.
1. Use `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, etc. for `<type>`.
2. Do not add `<scope>`. The optional `<scope>` is only allowed for dependabot pull requests.
3. `<description>` must start with a lowercase letter. You may use backticks to refer to code elements.
   If `<description>` contains words that should start with an uppercase letter (e.g., "SQL"),
   consider rephrasing it so that such words are not at the start.
