# HMS Testcontainers Integration Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add Docker-backed integration tests for the pure Hive Metastore catalog provider in `crates/sail-catalog-hms/tests` using `testcontainers`.

**Architecture:** Use the official `apache/hive` image in standalone metastore mode as the external service under test. Keep the scope small and metadata-only: start an HMS container, connect `HmsCatalogProvider` to it over Thrift, and verify database, table, and view CRUD through the provider API without adding any repo-level `docker compose` support.

**Tech Stack:** Rust, `tokio`, `testcontainers`, official `apache/hive` Docker image, Sail catalog provider traits.

---

### Task 1: Add the HMS integration-test crate scaffolding

**Files:**
- Modify: `crates/sail-catalog-hms/Cargo.toml`
- Create: `crates/sail-catalog-hms/tests/common/mod.rs`

**Step 1: Write the failing test harness compile target**

Create `crates/sail-catalog-hms/tests/common/mod.rs` with:

```rust
pub async fn setup_hms_catalog(
    test_name: &str,
) -> (sail_catalog_hms::HmsCatalogProvider, testcontainers::ContainerAsync<testcontainers::GenericImage>) {
    todo!()
}
```

Add dev-dependencies to `crates/sail-catalog-hms/Cargo.toml`:

```toml
[dev-dependencies]
arrow = { workspace = true }
testcontainers = { workspace = true }
```

**Step 2: Run test compile to verify it fails**

Run: `cargo +1.91.0-aarch64-apple-darwin test -p sail-catalog-hms --test database_tests --no-run`

Expected: FAIL because `database_tests` does not exist yet or the harness is incomplete.

**Step 3: Write the minimal harness implementation**

Implement `setup_hms_catalog` to:
- start `apache/hive:<pinned-version>` with `SERVICE_NAME=metastore`
- expose port `9083`
- wait for a reliable startup signal
- build `HmsCatalogProvider` with `uri="<host>:<mapped-port>"`
- create a `RuntimeHandle` from `tokio::runtime::Handle::current()`

**Step 4: Run compile check for the harness**

Run: `cargo +1.91.0-aarch64-apple-darwin test -p sail-catalog-hms --test database_tests --no-run`

Expected: either compiles or fails only because the actual test file is still missing.

### Task 2: Add database CRUD smoke tests

**Files:**
- Create: `crates/sail-catalog-hms/tests/database_tests.rs`
- Test: `crates/sail-catalog-hms/tests/common/mod.rs`

**Step 1: Write the failing database tests**

Create tests for:
- `create_database` then `get_database`
- `list_databases`
- `drop_database`

Use:

```rust
#[tokio::test]
async fn test_create_get_drop_database() { /* ... */ }
```

and a helper:

```rust
fn simple_database_options() -> CreateDatabaseOptions {
    CreateDatabaseOptions {
        if_not_exists: false,
        comment: None,
        location: None,
        properties: vec![],
    }
}
```

**Step 2: Run the database test to verify it fails**

Run: `cargo +1.91.0-aarch64-apple-darwin test -p sail-catalog-hms --test database_tests -- --nocapture`

Expected: FAIL because the container wait strategy or provider wiring is not fully correct yet.

**Step 3: Write minimal implementation fixes**

Adjust the harness or provider-facing expectations only as needed to make the database tests pass.

**Step 4: Re-run the database tests**

Run: `cargo +1.91.0-aarch64-apple-darwin test -p sail-catalog-hms --test database_tests -- --nocapture`

Expected: PASS.

### Task 3: Add table and view smoke tests

**Files:**
- Create: `crates/sail-catalog-hms/tests/table_tests.rs`
- Create: `crates/sail-catalog-hms/tests/view_tests.rs`
- Test: `crates/sail-catalog-hms/tests/common/mod.rs`

**Step 1: Write the failing table and view tests**

Add:
- one table CRUD flow using `format = "parquet"`
- one view CRUD flow using `VIRTUAL_VIEW` through `create_view` / `get_view` / `drop_view`

Use a simple table helper:

```rust
pub fn col(name: &str, data_type: DataType) -> CreateTableColumnOptions { /* ... */ }
```

and:

```rust
pub fn simple_table_options(columns: Vec<CreateTableColumnOptions>) -> CreateTableOptions {
    CreateTableOptions {
        columns,
        comment: None,
        constraints: vec![],
        location: Some("/tmp/hms-test".to_string()),
        format: "parquet".to_string(),
        partition_by: vec![],
        sort_by: vec![],
        bucket_by: None,
        if_not_exists: false,
        replace: false,
        options: vec![],
        properties: vec![],
    }
}
```

**Step 2: Run the table and view tests to verify they fail**

Run:
- `cargo +1.91.0-aarch64-apple-darwin test -p sail-catalog-hms --test table_tests -- --nocapture`
- `cargo +1.91.0-aarch64-apple-darwin test -p sail-catalog-hms --test view_tests -- --nocapture`

Expected: FAIL until the harness and test assumptions match real HMS behavior.

**Step 3: Make the minimal fixes**

Only adjust test helpers, wait strategy, or metadata expectations as needed. Do not add non-HMS behavior.

**Step 4: Re-run the table and view tests**

Run:
- `cargo +1.91.0-aarch64-apple-darwin test -p sail-catalog-hms --test table_tests -- --nocapture`
- `cargo +1.91.0-aarch64-apple-darwin test -p sail-catalog-hms --test view_tests -- --nocapture`

Expected: PASS.

### Task 4: Run full verification

**Files:**
- Verify: `crates/sail-catalog-hms/tests/*`
- Verify: `crates/sail-catalog-hms/src/*`

**Step 1: Run the HMS crate test suite**

Run: `cargo +1.91.0-aarch64-apple-darwin test -p sail-catalog-hms`

Expected: PASS.

**Step 2: Run a session-level compile check**

Run: `cargo +1.91.0-aarch64-apple-darwin check -p sail-session --lib`

Expected: PASS.

**Step 3: Review the diff for scope**

Run: `git diff --stat`

Expected: only HMS test/dependency changes plus any necessary plan/doc updates.
