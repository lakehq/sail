set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
  @just --list --unsorted

# ---------- Setup ----------
setup: setup-python setup-docs

setup-python:
  hatch env create
  hatch run maturin develop

setup-docs:
  if command -v corepack >/dev/null 2>&1; then corepack enable; fi
  pnpm install

# ---------- Formatting / linting ----------
fmt: fmt-rust fmt-python fmt-docs

fmt-rust:
  cargo +nightly fmt

fmt-rust-check:
  cargo +nightly fmt -- --check

clippy:
  cargo clippy --all-targets --all-features -- -D warnings

fmt-python:
  hatch fmt

fmt-python-check:
  hatch fmt --check

fmt-docs:
  pnpm run format

fmt-docs-check:
  FORMAT_OPTIONS=--check pnpm run format

docs-lint:
  pnpm run lint

# ---------- Rust build & tests ----------
rust-build:
  cargo build

rust-release:
  cargo build --release

rust-test:
  cargo nextest run

rust-test-ignored:
  cargo nextest run --run-ignored ignored-only -j 6

rust-gold:
  SAIL_UPDATE_GOLD_DATA=1 cargo test

# ---------- Python build & tests ----------
py-dev:
  hatch run maturin develop

py-wheel:
  hatch run maturin build -o target/wheels

py-install-wheel:
  hatch run install-pysail

[positional-arguments]
py-test *args='':
  hatch run pytest "$@"

[positional-arguments]
py-test-installed *args='':
  hatch run pytest --pyargs pysail "$@"

# ---------- Docs ----------
docs-dev:
  pnpm run docs:dev

docs-build:
  pnpm run docs:build

# ---------- Combined check loops ----------
rust-check: fmt-rust-check clippy rust-test

py-check: fmt-python-check py-test-installed

docs-check: fmt-docs-check docs-lint

check: rust-check py-check

pre-commit:
  cargo +nightly fmt -- --check
  cargo clippy --all-targets --all-features -- -D warnings
  hatch fmt --check
  FORMAT_OPTIONS=--check pnpm run format
  pnpm run lint

ci:
  cargo +nightly fmt -- --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo nextest run
  cargo nextest run --run-ignored ignored-only -j 6
  hatch fmt --check
  hatch run pytest --pyargs pysail
  FORMAT_OPTIONS=--check pnpm run format
  pnpm run lint

# ---------- Spark / Ibis parity tests ----------
spark-build-pyspark version="4.0.0":
  SPARK_VERSION={{version}} scripts/spark-tests/build-pyspark.sh

spark-server env="default":
  HATCH_ENV={{env}} hatch run scripts/spark-tests/run-server.sh

[positional-arguments]
spark-tests version="4.0.0" name="latest" *args='':
  HATCH_ENV=test-spark.spark-{{version}} TEST_RUN_NAME={{name}} hatch run scripts/spark-tests/run-tests.sh "$@"

ibis-tests:
  @echo "Requires a running Spark Connect server on sc://localhost:50051"
  HATCH_ENV=test-ibis hatch run test-ibis:env SPARK_REMOTE="sc://localhost:50051" scripts/spark-tests/run-tests.sh
