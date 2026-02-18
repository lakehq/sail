from __future__ import annotations

import pyarrow as pa
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    InputPartition,
)


class PostgresInputPartition(InputPartition):
    def __init__(self, partition_id: int, query: str, connection_params: dict, params: list | None = None):
        super().__init__(partition_id)
        self.query = query
        self.connection_params = connection_params
        self.params = params or []


class PostgresDataSourceReader(DataSourceReader):
    def __init__(
        self,
        connection_params: dict,
        table: str,
        num_partitions: int = 1,
        partition_column: str | None = None,
        schema=None,
        batch_size: int = 8192,
    ):
        self.connection_params = connection_params
        self.table = table
        self.num_partitions = max(1, num_partitions)
        self.partition_column = partition_column
        self.schema = schema
        self.batch_size = batch_size
        self._filters = []

    def pushFilters(self, filters):  # noqa: N802
        for f in filters:
            filter_type = type(f).__name__
            # Store as dict instead of object to avoid pickle issues
            if filter_type in ["EqualTo", "GreaterThan", "GreaterThanOrEqual", "LessThan", "LessThanOrEqual"]:
                self._filters.append(
                    {
                        "type": filter_type,
                        "attribute": f.attribute,
                        "value": f.value,
                    }
                )
            else:
                yield f

    def partitions(self):
        where_clause, where_params = self._build_where_clause()
        table = self._quote_table_reference(self.table)
        if self.num_partitions == 1 or not self.partition_column:
            query = f"SELECT * FROM {table}"  # noqa: S608
            if where_clause:
                query += f" WHERE {where_clause}"
            return [PostgresInputPartition(0, query, self.connection_params, where_params)]

        partition_column = self._quote_identifier(self.partition_column)  # column name only, never schema-qualified
        partitions = []
        for i in range(self.num_partitions):
            query = f"SELECT * FROM {table}"  # noqa: S608
            conditions = []
            params = list(where_params)
            if where_clause:
                conditions.append(where_clause)
            # num_partitions and i are integers from our own code, safe to interpolate
            conditions.append(f"MOD({partition_column}, {self.num_partitions}) = {i}")
            query += f" WHERE {' AND '.join(conditions)}"
            partitions.append(PostgresInputPartition(i, query, self.connection_params, params))
        return partitions

    def read(self, partition):
        import psycopg2

        conn = psycopg2.connect(**partition.connection_params)
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(partition.query, partition.params or None)
                columns = [desc[0] for desc in cursor.description]

                while True:
                    rows = cursor.fetchmany(self.batch_size)
                    if not rows:
                        break

                    # Build arrays with explicit types from schema
                    arrays = []
                    for i, col in enumerate(columns):
                        col_data = [row[i] for row in rows]
                        field = self.schema.field(col)

                        # Convert data types that PyArrow can't handle directly
                        converted_data = []
                        for val in col_data:
                            if val is None:
                                converted_data.append(None)
                            elif pa.types.is_string(field.type):
                                # Convert to string for string fields (handles Decimal, UUID, etc.)
                                converted_data.append(str(val))
                            else:
                                converted_data.append(val)

                        # Create array with explicit type to ensure correct type conversion
                        arrays.append(pa.array(converted_data, type=field.type))

                    # Create RecordBatch from arrays with schema
                    yield pa.RecordBatch.from_arrays(arrays, schema=self.schema)
            finally:
                cursor.close()
        finally:
            conn.close()

    def _build_where_clause(self) -> tuple[str, list]:
        """Return (clause, params) where clause uses %s placeholders for all filter values."""
        conditions = []
        params = []
        for f in self._filters:
            col = f["attribute"][0] if f["attribute"] else None
            if not col:
                continue

            filter_type = f["type"]
            quoted_col = self._quote_identifier(col)

            if filter_type == "EqualTo":
                conditions.append(f"{quoted_col} = %s")
            elif filter_type == "GreaterThan":
                conditions.append(f"{quoted_col} > %s")
            elif filter_type == "GreaterThanOrEqual":
                conditions.append(f"{quoted_col} >= %s")
            elif filter_type == "LessThan":
                conditions.append(f"{quoted_col} < %s")
            elif filter_type == "LessThanOrEqual":
                conditions.append(f"{quoted_col} <= %s")
            else:
                continue
            params.append(f["value"])

        return " AND ".join(conditions) if conditions else "", params

    def _quote_identifier(self, identifier):
        return '"' + identifier.replace('"', '""') + '"'

    def _quote_table_reference(self, table):
        """Quote a table reference, handling an optional 'schema.table' prefix."""
        parts = table.split(".", 1)
        return ".".join(self._quote_identifier(p) for p in parts)


class PostgresDataSource(DataSource):
    @classmethod
    def name(cls):
        return "postgres"

    def schema(self):
        import psycopg2

        conn_params = self._get_connection_params()
        table = self.options.get("dbtable")
        table_schema = self.options.get("tableSchema", "public")
        if not table:
            msg = "dbtable option is required"
            raise ValueError(msg)

        conn = psycopg2.connect(**conn_params)
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = %s
                    ORDER BY ordinal_position
                """,
                    (table, table_schema),
                )

                fields = []
                for col_name, data_type in cursor.fetchall():
                    arrow_type = self._pg_to_arrow(data_type)
                    fields.append((col_name, arrow_type))

                if not fields:
                    msg = f"Table '{table}' not found"
                    raise ValueError(msg)
                return pa.schema(fields)
            finally:
                cursor.close()
        finally:
            conn.close()

    def reader(self, schema):
        conn_params = self._get_connection_params()
        table = self.options.get("dbtable")
        table_schema = self.options.get("tableSchema", "public")
        num_partitions = int(self.options.get("numPartitions", "1"))
        partition_column = self.options.get("partitionColumn")
        batch_size = int(self.options.get("fetchsize", "8192"))

        if num_partitions < 1:
            msg = "numPartitions must be a positive integer"
            raise ValueError(msg)
        if batch_size < 1:
            msg = "fetchsize must be a positive integer"
            raise ValueError(msg)

        qualified_table = f"{table_schema}.{table}"
        return PostgresDataSourceReader(
            conn_params, qualified_table, num_partitions, partition_column, schema, batch_size
        )

    def _get_connection_params(self):
        from urllib.parse import urlparse

        url = self.options.get("url")
        user = self.options.get("user")
        password = self.options.get("password")

        if not all([url, user, password]):
            msg = "url, user, and password are required"
            raise ValueError(msg)

        if not url.startswith("jdbc:postgresql://"):
            msg = "url must be a PostgreSQL JDBC URL (e.g. jdbc:postgresql://localhost:5432/mydb)"
            raise ValueError(msg)

        parsed = urlparse(url[5:])  # strip "jdbc:" → "postgresql://host:port/db"
        host = parsed.hostname or "localhost"
        port = parsed.port or 5432
        database = parsed.path.lstrip("/")

        if not database:
            msg = "url must include a database name (e.g. jdbc:postgresql://localhost:5432/mydb)"
            raise ValueError(msg)

        return {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }

    def _pg_to_arrow(self, pg_type):
        mapping = {
            "smallint": pa.int16(),
            "integer": pa.int32(),
            "bigint": pa.int64(),
            "real": pa.float32(),
            "double precision": pa.float64(),
            "text": pa.string(),
            "character varying": pa.string(),
            "boolean": pa.bool_(),
            "timestamp": pa.timestamp("us"),
            "date": pa.date32(),
            "timestamp without time zone": pa.timestamp("us"),
            "timestamp with time zone": pa.timestamp("us", tz="UTC"),
        }
        return mapping.get(pg_type.lower(), pa.string())
