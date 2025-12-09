set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

# Glossary: check=type/compile only; lint=style/static (clippy/ruff/docs); test=pytest/nextest; build=artifacts (cargo build, wheels). Cost: low=fast, med=interactive ok, high=save for later.

# priority:info | scope:guide | goal:list curated workflows
default:
  @just --list --unsorted

# ---------- Daily inner loop (per-crate, ergonomic) ----------
# quick guide: `just status-short` → pick touched crate(s) → `just crate-check <crate>` (fast, no clippy) → edit/add pytest under python/pysail/tests → `just py-dev` → `just py-test-k <expr>` → iterate → `just pre-commit` before finish.

# priority:high | scope:git | when:before targeting crates | cost:low | note:see touched files
status-short:
  git status --short

# priority:high | scope:rust | when:per-crate change | cost:low | note:fast check; prefer before full clippy/build
[positional-arguments]
crate-check crate='':
  cargo check -p "{{crate}}"

# priority:high | scope:python/integration | when:targeted | cost:med | note:tests live in python/pysail/tests; remote fixture auto-starts server unless SPARK_REMOTE set; use -k expr
[positional-arguments]
py-test-k *args='':
  hatch run pytest -k "$@"

# ---------- Fast gates (daily / commit) ----------
# priority:high | scope:full | when:pre-commit | goal:local parity with ci-lite
pre-commit:
  cargo +nightly fmt -- --check
  cargo clippy --all-targets --all-features -- -D warnings
  hatch fmt --check
  FORMAT_OPTIONS=--check pnpm run format
  pnpm run lint

# priority:high | scope:full | when:pre-commit | goal:fast local gate
check: py-check rust-check

# priority:high | scope:python/integration | when:pre-commit/ci | goal:fmt+tests
py-check: fmt-python-check py-test-installed

# priority:high | scope:rust | when:pre-commit/ci | goal:fmt+lint+tests | note:runs after py-check
rust-check: fmt-rust-check clippy rust-test

# priority:med | scope:docs | when:pre-commit/ci | goal:format+lint
docs-check: fmt-docs-check docs-lint

# priority:high | scope:full | when:ci | goal:ci-equivalent locally
ci:
  cargo +nightly fmt -- --check
  cargo clippy --all-targets --all-features -- -D warnings
  hatch fmt --check
  hatch run pytest --pyargs pysail
  cargo nextest run
  cargo nextest run --run-ignored ignored-only -j 6
  FORMAT_OPTIONS=--check pnpm run format
  pnpm run lint

# ---------- Formatting / linting ----------
# priority:high | scope:rust/python/docs | when:pre-commit
fmt: fmt-rust fmt-python fmt-docs

# priority:high | scope:rust | when:pre-commit | goal:fmt
fmt-rust:
  cargo +nightly fmt

# priority:high | scope:rust | when:ci/pre-commit | goal:fmt-check
fmt-rust-check:
  cargo +nightly fmt -- --check

# priority:high | scope:rust | when:ci/pre-commit | goal:lint fatal
clippy:
  cargo clippy --all-targets --all-features -- -D warnings

# priority:high | scope:python | when:pre-commit | goal:fmt+lint (ruff)
fmt-python:
  hatch fmt

# priority:high | scope:python | when:ci/pre-commit | goal:fmt-check
fmt-python-check:
  hatch fmt --check

# priority:med | scope:docs | when:pre-commit | goal:format md/vitepress
fmt-docs:
  pnpm run format

# priority:med | scope:docs | when:ci/pre-commit | goal:format-check
fmt-docs-check:
  FORMAT_OPTIONS=--check pnpm run format

# priority:med | scope:docs | when:ci/pre-commit | goal:lint
docs-lint:
  pnpm run lint

# ---------- Python build & tests (primary integration, hatch-heavy) ----------
# priority:high | scope:python/integration | goal:pytest (local); default remote fixture auto-starts server unless SPARK_REMOTE set
[positional-arguments]
py-test *args='':
  hatch run pytest "$@"

# priority:high | scope:python/integration | goal:pytest installed wheel; tests in python/pysail/tests
[positional-arguments]
py-test-installed *args='':
  hatch run pytest --pyargs pysail "$@"

# priority:high | scope:python | when:after rust change | cost:med | note:rebuild ext for pytest
py-dev:
  hatch run maturin develop

# priority:med | scope:python | goal:build wheel
py-wheel:
  hatch run maturin build -o target/wheels

# priority:med | scope:python | goal:install built wheel into envs
py-install-wheel:
  hatch run install-pysail

# ---------- Rust build & tests ----------
# priority:med | scope:rust | goal:nextest suite (secondary to py tests)
rust-test:
  cargo nextest run

# priority:med | scope:rust | goal:ignored tests (slow)
rust-test-ignored:
  cargo nextest run --run-ignored ignored-only -j 6

# priority:med | scope:rust | goal:debug build
rust-build:
  cargo build

# priority:med | scope:rust | goal:release build
rust-release:
  cargo build --release

# priority:med | scope:rust | when:update-gold | goal:refresh expected data; set SAIL_UPDATE_GOLD_DATA=1
rust-gold:
  SAIL_UPDATE_GOLD_DATA=1 cargo test

# ---------- Docs ----------
# priority:med | scope:docs | goal:live preview
docs-dev:
  pnpm run docs:dev

# priority:med | scope:docs | goal:build static docs
docs-build:
  pnpm run docs:build

# ---------- Spark / Ibis parity tests ----------
# priority:med | scope:spark | goal:build pyspark connector
spark-build-pyspark version="4.0.0":
  SPARK_VERSION={{version}} scripts/spark-tests/build-pyspark.sh

# priority:med | scope:spark | goal:start parity server (env-selectable)
spark-server env="default":
  HATCH_ENV={{env}} hatch run scripts/spark-tests/run-server.sh

# priority:med | scope:spark | goal:run spark parity suites
[positional-arguments]
spark-tests version="4.0.0" name="latest" *args='':
  HATCH_ENV=test-spark.spark-{{version}} TEST_RUN_NAME={{name}} hatch run scripts/spark-tests/run-tests.sh "$@"

# priority:med | scope:ibis | goal:run ibis suites (needs server running)
ibis-tests:
  @echo "Requires a running Spark Connect server on sc://localhost:50051"
  HATCH_ENV=test-ibis hatch run test-ibis:env SPARK_REMOTE="sc://localhost:50051" scripts/spark-tests/run-tests.sh

# ---------- Bootstrap (one-time / rare) ----------
# priority:high | scope:bootstrap | goal:ready dev envs fast
setup: setup-python setup-docs

# priority:high | scope:python/rust | when:first-run | note:hatch env
setup-python:
  hatch env create
  hatch run maturin develop

# priority:med | scope:docs | when:touching-docs | note:pnpm deps
setup-docs:
  if command -v corepack >/dev/null 2>&1; then corepack enable; fi
  pnpm install
