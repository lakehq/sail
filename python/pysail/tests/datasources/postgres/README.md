# PostgreSQL DataSource Manual Tests

This directory contains manual integration tests for the PostgreSQL DataSource implementation.

## Prerequisites

- Docker and Docker Compose installed
- PostgreSQL client tools (psql) - optional, for verification

## Running the Tests

1. **Start the PostgreSQL container using the root compose.yml:**
   ```bash
   # From the repository root
   docker compose --profile datasources up -d postgres
   ```

2. **Wait for the database to be ready:**
   ```bash
   docker exec sail-postgres-dev pg_isready -U testuser -d testdb
   ```

3. **Run the manual tests:**
   ```bash
   # From the repository root
   hatch run python python/pysail/tests/datasources/postgres/manual_test_postgres.py
   ```

4. **Stop the container when done:**
   ```bash
   docker compose --profile datasources down
   ```

## Note

These tests are **not** part of the automated test suite because they require a running PostgreSQL container. They are intended for manual verification during development.

## Test Coverage

The manual test suite includes 20 comprehensive tests covering:
- Basic read operations
- Filter pushdown (EqualTo, GreaterThan, LessThan, In, Like, And, Or, Not)
- Column projection
- Partitioned reading
- NULL value handling
- Unicode string support
- Large dataset performance (10K rows)
- Table joins
- SQL injection protection
- All PostgreSQL data types
- Empty table handling
- Complex filter combinations
- Concurrent reads
- Connection cleanup
