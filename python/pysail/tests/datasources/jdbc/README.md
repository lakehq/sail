# JDBC Datasource Tests

Manual integration tests for `pysail.datasources.jdbc`.

## Prerequisites

1. **Install dependencies**

   ```bash
   pip install pysail[jdbc]
   ```

2. **Start PostgreSQL** (using the project Docker Compose)

   ```bash
   docker compose --profile datasources up -d
   ```

   This starts a PostgreSQL container at `localhost:5432` with:
   - User: `testuser`
   - Password: `testpass`
   - Database: `testdb`

3. **Start the Sail server** (in a separate terminal or set `SPARK_REMOTE`)

## Running the tests

```bash
python python/pysail/tests/datasources/jdbc/manual_test_jdbc.py
```

Each test prints `PASS` or `FAIL` with a brief description.
