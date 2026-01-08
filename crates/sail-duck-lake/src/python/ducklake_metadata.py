from __future__ import annotations

from typing import Any
from urllib.parse import urlparse, urlunparse

from sqlalchemy import and_, bindparam, column, create_engine, or_, select, table, text

_ENGINES: dict[str, Any] = {}


def _normalize_sqlalchemy_url(url: str) -> str:
    parsed = urlparse(url)
    scheme_lower = parsed.scheme.lower()

    if scheme_lower in ("postgres", "postgresql"):
        return urlunparse(parsed._replace(scheme="postgresql+psycopg"))

    if scheme_lower == "sqlite" and parsed.netloc:
        # Legacy sqlite URLs sometimes encode the path inside the netloc portion.
        new_path = f"/{parsed.netloc}{parsed.path}"
        return urlunparse(parsed._replace(netloc="", path=new_path))

    return url


def _get_engine(url: str):
    url = _normalize_sqlalchemy_url(url)
    eng = _ENGINES.get(url)
    if eng is None:
        eng = create_engine(url, future=True)
        _ENGINES[url] = eng
    return eng


def current_snapshot(url: str) -> dict[str, Any]:
    with _get_engine(url).connect() as conn:
        row = conn.execute(
            text(
                """
                select snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id
                from ducklake_snapshot
                order by snapshot_id desc
                limit 1
                """
            )
        ).first()
        if row is None:
            msg = "No snapshots found in metadata"
            raise ValueError(msg)
        return {
            "snapshot_id": int(row[0]),
            "snapshot_time": str(row[1]),
            "schema_version": int(row[2]),
            "next_catalog_id": int(row[3]),
            "next_file_id": int(row[4]),
            "changes_made": None,
            "author": None,
            "commit_message": None,
            "commit_extra_info": None,
        }


def snapshot_by_id(url: str, snapshot_id: int) -> dict[str, Any]:
    with _get_engine(url).connect() as conn:
        row = conn.execute(
            text(
                """
                select snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id
                from ducklake_snapshot
                where snapshot_id = :sid
                """
            ),
            {"sid": int(snapshot_id)},
        ).first()
        if row is None:
            msg = f"Snapshot not found: {snapshot_id}"
            raise ValueError(msg)
        return {
            "snapshot_id": int(row[0]),
            "snapshot_time": str(row[1]),
            "schema_version": int(row[2]),
            "next_catalog_id": int(row[3]),
            "next_file_id": int(row[4]),
            "changes_made": None,
            "author": None,
            "commit_message": None,
            "commit_extra_info": None,
        }


def load_table(url: str, table_name: str, schema_name: str | None) -> dict[str, Any]:
    schema_name = schema_name or "main"
    with _get_engine(url).connect() as conn:
        srow = conn.execute(
            text(
                """
                select schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative
                from ducklake_schema
                where schema_name = :sname and end_snapshot is null
                limit 1
                """
            ),
            {"sname": schema_name},
        ).first()
        if srow is None:
            msg = f"Schema not found: {schema_name}"
            raise ValueError(msg)
        schema_id = int(srow[0])
        schema_info = {
            "schema_id": schema_id,
            "schema_uuid": str(srow[1]),
            "begin_snapshot": int(srow[2]) if srow[2] is not None else None,
            "end_snapshot": int(srow[3]) if srow[3] is not None else None,
            "schema_name": str(srow[4]),
            "path": str(srow[5]),
            "path_is_relative": bool(srow[6]),
        }

        trow = conn.execute(
            text(
                """
                select table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative
                from ducklake_table
                where table_name = :tname and schema_id = :sid and end_snapshot is null
                limit 1
                """
            ),
            {"tname": table_name, "sid": schema_id},
        ).first()
        if trow is None:
            msg = f"Table not found: {schema_name}.{table_name}"
            raise ValueError(msg)
        table_id = int(trow[0])
        table_info = {
            "table_id": table_id,
            "table_uuid": str(trow[1]),
            "begin_snapshot": int(trow[2]) if trow[2] is not None else None,
            "end_snapshot": int(trow[3]) if trow[3] is not None else None,
            "schema_id": schema_id,
            "table_name": str(trow[5]),
            "path": str(trow[6]),
            "path_is_relative": bool(trow[7]),
            "columns": [],
            "inlined_data_tables": [],
        }

        cols = conn.execute(
            text(
                """
                select column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name,
                       column_type, initial_default, default_value, nulls_allowed, parent_column
                from ducklake_column
                where table_id = :tid and end_snapshot is null
                order by column_order asc
                """
            ),
            {"tid": table_id},
        ).all()
        columns: list[dict[str, Any]] = [
            {
                "column_id": int(row[0]),
                "begin_snapshot": int(row[1]) if row[1] is not None else None,
                "end_snapshot": int(row[2]) if row[2] is not None else None,
                "table_id": int(row[3]),
                "column_order": int(row[4]),
                "column_name": str(row[5]),
                "column_type": str(row[6]),
                "initial_default": str(row[7]) if row[7] is not None else None,
                "default_value": str(row[8]) if row[8] is not None else None,
                "nulls_allowed": bool(row[9]),
                "parent_column": int(row[10]) if row[10] is not None else None,
            }
            for row in cols
        ]

        # Load partition fields (if any)
        pf_rows = conn.execute(
            text(
                """
                SELECT pc.partition_key_index, pc.column_id, pc.transform
                FROM ducklake_partition_column pc
                JOIN ducklake_partition_info pi USING (partition_id, table_id)
                WHERE pc.table_id = :tid AND (pi.end_snapshot IS NULL)
                ORDER BY pc.partition_key_index
                """
            ),
            {"tid": table_id},
        ).all()
        partition_fields = [
            {
                "partition_key_index": int(r[0]),
                "column_id": int(r[1]),
                "transform": str(r[2]) if r[2] is not None else "identity",
            }
            for r in pf_rows
        ]

        return {
            "schema_info": schema_info,
            "table_info": table_info,
            "columns": columns,
            "partition_fields": partition_fields,
        }


def list_data_files(
    url: str,
    table_id: int,
    snapshot_id: int | None,
    partition_filters: list[tuple[int, list[str]]] | None = None,
    required_column_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    # TODO: Add optional stats-based filter pushdown using ducklake_file_column_stats.
    # TODO: Add iterator-based API for lazy or paginated data file loading.
    data_file_table = table(
        "ducklake_data_file",
        column("data_file_id"),
        column("table_id"),
        column("begin_snapshot"),
        column("end_snapshot"),
        column("file_order"),
        column("path"),
        column("path_is_relative"),
        column("file_format"),
        column("record_count"),
        column("file_size_bytes"),
        column("footer_size"),
        column("row_id_start"),
        column("partition_id"),
        column("encryption_key"),
        column("partial_file_info"),
        column("mapping_id"),
    )

    partition_value_table = table(
        "ducklake_file_partition_value",
        column("data_file_id"),
        column("partition_key_index"),
        column("partition_value"),
    )

    where_clauses_active: list[Any] = [
        data_file_table.c.table_id == bindparam("tid"),
        data_file_table.c.end_snapshot.is_(None),
    ]
    where_clauses_asof: list[Any] = [
        data_file_table.c.table_id == bindparam("tid"),
        data_file_table.c.begin_snapshot <= bindparam("sid"),
        or_(
            data_file_table.c.end_snapshot.is_(None),
            data_file_table.c.end_snapshot > bindparam("sid"),
        ),
    ]
    params_active: dict[str, Any] = {"tid": int(table_id)}
    params_asof: dict[str, Any] = {"tid": int(table_id)}
    if snapshot_id is not None:
        params_asof["sid"] = int(snapshot_id)

    if partition_filters:
        for idx, (partition_key_index, values) in enumerate(partition_filters):
            if not values:
                continue
            key_param = f"pf_{idx}_key"
            values_param = f"pf_{idx}_values"
            partition_exists = (
                select(1)
                .select_from(partition_value_table)
                .where(
                    and_(
                        partition_value_table.c.data_file_id == data_file_table.c.data_file_id,
                        partition_value_table.c.partition_key_index == bindparam(key_param),
                        partition_value_table.c.partition_value.in_(bindparam(values_param, expanding=True)),
                    )
                )
                .exists()
            )
            where_clauses_active.append(partition_exists)
            where_clauses_asof.append(partition_exists)
            params_active[key_param] = int(partition_key_index)
            params_asof[key_param] = int(partition_key_index)
            params_active[values_param] = list(values)
            params_asof[values_param] = list(values)
    stmt_active = (
        select(
            data_file_table.c.data_file_id,
            data_file_table.c.table_id,
            data_file_table.c.begin_snapshot,
            data_file_table.c.end_snapshot,
            data_file_table.c.file_order,
            data_file_table.c.path,
            data_file_table.c.path_is_relative,
            data_file_table.c.file_format,
            data_file_table.c.record_count,
            data_file_table.c.file_size_bytes,
            data_file_table.c.footer_size,
            data_file_table.c.row_id_start,
            data_file_table.c.partition_id,
            data_file_table.c.encryption_key,
            data_file_table.c.partial_file_info,
            data_file_table.c.mapping_id,
        )
        .where(and_(*where_clauses_active))
        .order_by(data_file_table.c.file_order.asc())
    )
    stmt_asof = (
        select(
            data_file_table.c.data_file_id,
            data_file_table.c.table_id,
            data_file_table.c.begin_snapshot,
            data_file_table.c.end_snapshot,
            data_file_table.c.file_order,
            data_file_table.c.path,
            data_file_table.c.path_is_relative,
            data_file_table.c.file_format,
            data_file_table.c.record_count,
            data_file_table.c.file_size_bytes,
            data_file_table.c.footer_size,
            data_file_table.c.row_id_start,
            data_file_table.c.partition_id,
            data_file_table.c.encryption_key,
            data_file_table.c.partial_file_info,
            data_file_table.c.mapping_id,
        )
        .where(and_(*where_clauses_asof))
        .order_by(data_file_table.c.file_order.asc())
    )
    with _get_engine(url).connect() as conn:
        if snapshot_id is None:
            rows = conn.execute(stmt_active, params_active).all()
        else:
            rows = conn.execute(stmt_asof, params_asof).all()
        out: list[dict[str, Any]] = [
            {
                "data_file_id": int(row[0]),
                "table_id": int(row[1]),
                "begin_snapshot": int(row[2]) if row[2] is not None else None,
                "end_snapshot": int(row[3]) if row[3] is not None else None,
                "file_order": int(row[4]) if row[4] is not None else 0,
                "path": str(row[5]),
                "path_is_relative": bool(row[6]),
                "file_format": str(row[7]) if row[7] is not None else None,
                "record_count": int(row[8]),
                "file_size_bytes": int(row[9]),
                "footer_size": int(row[10]) if row[10] is not None else None,
                "row_id_start": int(row[11]) if row[11] is not None else None,
                "partition_id": int(row[12]) if row[12] is not None else None,
                "encryption_key": str(row[13]) if row[13] is not None else "",
                "partial_file_info": str(row[14]) if row[14] is not None else None,
                "mapping_id": int(row[15]) if row[15] is not None else 0,
                "column_stats": [],
                "partition_values": [],
            }
            for row in rows
        ]
        if not out:
            return out
        file_ids = [int(item["data_file_id"]) for item in out]
        stats_rows: list[tuple[Any, ...]] = []
        if required_column_ids is None or required_column_ids:
            stats_query = """
                select data_file_id, column_id, column_size_bytes, value_count, null_count,
                       min_value, max_value, contains_nan, extra_stats
                from ducklake_file_column_stats
                where data_file_id in :file_ids
            """
            stats_params: dict[str, Any] = {"file_ids": list(file_ids)}
            if required_column_ids is not None:
                stats_query += " and column_id in :column_ids"
                stats_params["column_ids"] = [int(cid) for cid in required_column_ids]
            stats_sql = text(stats_query).bindparams(bindparam("file_ids", expanding=True))
            if required_column_ids is not None:
                stats_sql = stats_sql.bindparams(bindparam("column_ids", expanding=True))
            stats_rows = conn.execute(stats_sql, stats_params).all()
        pv_sql = text(
            """
            select data_file_id, partition_key_index, partition_value
            from ducklake_file_partition_value
            where data_file_id in :file_ids
            """
        ).bindparams(bindparam("file_ids", expanding=True))
        pv_params = {"file_ids": list(file_ids)}
        pv_rows = conn.execute(pv_sql, pv_params).all()

        stats_map: dict[int, list[dict[str, Any]]] = {}
        for r in stats_rows:
            fid = int(r[0])
            lst = stats_map.setdefault(fid, [])
            lst.append(
                {
                    "column_id": int(r[1]),
                    "column_size_bytes": int(r[2]) if r[2] is not None else None,
                    "value_count": int(r[3]) if r[3] is not None else None,
                    "null_count": int(r[4]) if r[4] is not None else None,
                    "min_value": str(r[5]) if r[5] is not None else None,
                    "max_value": str(r[6]) if r[6] is not None else None,
                    "contains_nan": bool(r[7]) if r[7] is not None else None,
                    "extra_stats": str(r[8]) if r[8] is not None else None,
                }
            )

        pv_map: dict[int, list[dict[str, Any]]] = {}
        for r in pv_rows:
            fid = int(r[0])
            lst = pv_map.setdefault(fid, [])
            lst.append(
                {
                    "partition_key_index": int(r[1]),
                    "partition_value": str(r[2]),
                }
            )

        for item in out:
            fid = item["data_file_id"]
            item["column_stats"] = stats_map.get(fid, [])
            item["partition_values"] = pv_map.get(fid, [])
        return out


def scan_data_files_arrow(
    url: str,
    table_id: int,
    snapshot_id: int | None,
    partition_filters: list[tuple[int, list[str]]] | None = None,
    required_column_ids: list[int] | None = None,
    batch_size: int = 10000,
):
    """
    Stream DuckLake data file metadata as Arrow RecordBatches.

    This is the Arrow-native alternative to `list_data_files`. It avoids returning
    a huge `list[dict]` to Rust, and instead yields `pyarrow.RecordBatch` chunks.
    """
    import pyarrow as pa

    # Keep the schema stable so Rust can decode it as a RecordBatch stream.
    # NOTE: These types intentionally use "large" variants to match serde_arrow defaults.
    # serde_arrow maps `Vec<T>` -> LargeList(non-null element field), because `T` itself is not optional.
    partition_values_element = pa.field(
        "element",
        pa.struct(
            [
                pa.field("partition_key_index", pa.uint64(), nullable=False),
                pa.field("partition_value", pa.large_string(), nullable=False),
            ]
        ),
        nullable=False,
    )
    partition_values_type = pa.large_list(partition_values_element)

    column_stats_element = pa.field(
        "element",
        pa.struct(
            [
                pa.field("column_id", pa.uint64(), nullable=False),
                pa.field("column_size_bytes", pa.uint64(), nullable=True),
                pa.field("value_count", pa.uint64(), nullable=True),
                pa.field("null_count", pa.uint64(), nullable=True),
                pa.field("min_value", pa.large_string(), nullable=True),
                pa.field("max_value", pa.large_string(), nullable=True),
                pa.field("contains_nan", pa.bool_(), nullable=True),
                pa.field("extra_stats", pa.large_string(), nullable=True),
            ]
        ),
        nullable=False,
    )
    column_stats_type = pa.large_list(column_stats_element)
    schema = pa.schema(
        [
            pa.field("data_file_id", pa.uint64(), nullable=False),
            pa.field("table_id", pa.uint64(), nullable=False),
            pa.field("begin_snapshot", pa.uint64(), nullable=True),
            pa.field("end_snapshot", pa.uint64(), nullable=True),
            pa.field("file_order", pa.uint64(), nullable=False),
            pa.field("path", pa.large_string(), nullable=False),
            pa.field("path_is_relative", pa.bool_(), nullable=False),
            pa.field("file_format", pa.large_string(), nullable=True),
            pa.field("record_count", pa.uint64(), nullable=False),
            pa.field("file_size_bytes", pa.uint64(), nullable=False),
            pa.field("footer_size", pa.uint64(), nullable=True),
            pa.field("row_id_start", pa.uint64(), nullable=True),
            pa.field("partition_id", pa.uint64(), nullable=True),
            pa.field("encryption_key", pa.large_string(), nullable=False),
            pa.field("partial_file_info", pa.large_string(), nullable=True),
            pa.field("mapping_id", pa.uint64(), nullable=False),
            pa.field("column_stats", column_stats_type, nullable=False),
            pa.field("partition_values", partition_values_type, nullable=False),
        ]
    )

    data_file_table = table(
        "ducklake_data_file",
        column("data_file_id"),
        column("table_id"),
        column("begin_snapshot"),
        column("end_snapshot"),
        column("file_order"),
        column("path"),
        column("path_is_relative"),
        column("file_format"),
        column("record_count"),
        column("file_size_bytes"),
        column("footer_size"),
        column("row_id_start"),
        column("partition_id"),
        column("encryption_key"),
        column("partial_file_info"),
        column("mapping_id"),
    )

    partition_value_table = table(
        "ducklake_file_partition_value",
        column("data_file_id"),
        column("partition_key_index"),
        column("partition_value"),
    )

    where_clauses_active: list[Any] = [
        data_file_table.c.table_id == bindparam("tid"),
        data_file_table.c.end_snapshot.is_(None),
    ]
    where_clauses_asof: list[Any] = [
        data_file_table.c.table_id == bindparam("tid"),
        data_file_table.c.begin_snapshot <= bindparam("sid"),
        or_(
            data_file_table.c.end_snapshot.is_(None),
            data_file_table.c.end_snapshot > bindparam("sid"),
        ),
    ]
    params_active: dict[str, Any] = {"tid": int(table_id)}
    params_asof: dict[str, Any] = {"tid": int(table_id)}
    if snapshot_id is not None:
        params_asof["sid"] = int(snapshot_id)

    if partition_filters:
        for idx, (partition_key_index, values) in enumerate(partition_filters):
            if not values:
                continue
            key_param = f"pf_{idx}_key"
            values_param = f"pf_{idx}_values"
            partition_exists = (
                select(1)
                .select_from(partition_value_table)
                .where(
                    and_(
                        partition_value_table.c.data_file_id == data_file_table.c.data_file_id,
                        partition_value_table.c.partition_key_index == bindparam(key_param),
                        partition_value_table.c.partition_value.in_(
                            bindparam(values_param, expanding=True)
                        ),
                    )
                )
                .exists()
            )
            where_clauses_active.append(partition_exists)
            where_clauses_asof.append(partition_exists)
            params_active[key_param] = int(partition_key_index)
            params_asof[key_param] = int(partition_key_index)
            params_active[values_param] = list(values)
            params_asof[values_param] = list(values)

    stmt_active = (
        select(
            data_file_table.c.data_file_id,
            data_file_table.c.table_id,
            data_file_table.c.begin_snapshot,
            data_file_table.c.end_snapshot,
            data_file_table.c.file_order,
            data_file_table.c.path,
            data_file_table.c.path_is_relative,
            data_file_table.c.file_format,
            data_file_table.c.record_count,
            data_file_table.c.file_size_bytes,
            data_file_table.c.footer_size,
            data_file_table.c.row_id_start,
            data_file_table.c.partition_id,
            data_file_table.c.encryption_key,
            data_file_table.c.partial_file_info,
            data_file_table.c.mapping_id,
        )
        .where(and_(*where_clauses_active))
        .order_by(data_file_table.c.file_order.asc())
    )
    stmt_asof = (
        select(
            data_file_table.c.data_file_id,
            data_file_table.c.table_id,
            data_file_table.c.begin_snapshot,
            data_file_table.c.end_snapshot,
            data_file_table.c.file_order,
            data_file_table.c.path,
            data_file_table.c.path_is_relative,
            data_file_table.c.file_format,
            data_file_table.c.record_count,
            data_file_table.c.file_size_bytes,
            data_file_table.c.footer_size,
            data_file_table.c.row_id_start,
            data_file_table.c.partition_id,
            data_file_table.c.encryption_key,
            data_file_table.c.partial_file_info,
            data_file_table.c.mapping_id,
        )
        .where(and_(*where_clauses_asof))
        .order_by(data_file_table.c.file_order.asc())
    )

    bs = int(batch_size) if batch_size and int(batch_size) > 0 else 10000

    with _get_engine(url).connect() as conn:
        if snapshot_id is None:
            result = conn.execute(stmt_active, params_active)
        else:
            result = conn.execute(stmt_asof, params_asof)

        while True:
            rows = result.fetchmany(bs)
            if not rows:
                break

            data_file_ids: list[int] = []
            table_ids: list[int] = []
            begin_snapshots: list[int | None] = []
            end_snapshots: list[int | None] = []
            file_orders: list[int] = []
            paths: list[str] = []
            path_is_relative: list[bool] = []
            file_formats: list[str | None] = []
            record_counts: list[int] = []
            file_sizes: list[int] = []
            footer_sizes: list[int | None] = []
            row_id_starts: list[int | None] = []
            partition_ids: list[int | None] = []
            encryption_keys: list[str] = []
            partial_file_infos: list[str | None] = []
            mapping_ids: list[int] = []

            for row in rows:
                # Column order matches the SELECT above.
                data_file_ids.append(int(row[0]))
                table_ids.append(int(row[1]))
                begin_snapshots.append(int(row[2]) if row[2] is not None else None)
                end_snapshots.append(int(row[3]) if row[3] is not None else None)
                file_orders.append(int(row[4]) if row[4] is not None else 0)
                paths.append(str(row[5]))
                path_is_relative.append(bool(row[6]))
                file_formats.append(str(row[7]) if row[7] is not None else None)
                record_counts.append(int(row[8]))
                file_sizes.append(int(row[9]))
                footer_sizes.append(int(row[10]) if row[10] is not None else None)
                row_id_starts.append(int(row[11]) if row[11] is not None else None)
                partition_ids.append(int(row[12]) if row[12] is not None else None)
                encryption_keys.append(str(row[13]) if row[13] is not None else "")
                partial_file_infos.append(str(row[14]) if row[14] is not None else None)
                mapping_ids.append(int(row[15]) if row[15] is not None else 0)

            stats_map: dict[int, list[dict[str, Any]]] = {}
            if required_column_ids is None or required_column_ids:
                stats_query = """
                    select data_file_id, column_id, column_size_bytes, value_count, null_count,
                           min_value, max_value, contains_nan, extra_stats
                    from ducklake_file_column_stats
                    where data_file_id in :file_ids
                """
                stats_params: dict[str, Any] = {"file_ids": list(data_file_ids)}
                if required_column_ids is not None:
                    stats_query += " and column_id in :column_ids"
                    stats_params["column_ids"] = [int(cid) for cid in required_column_ids]
                stats_sql = text(stats_query).bindparams(bindparam("file_ids", expanding=True))
                if required_column_ids is not None:
                    stats_sql = stats_sql.bindparams(bindparam("column_ids", expanding=True))
                stats_rows = conn.execute(stats_sql, stats_params).all()
                for r in stats_rows:
                    fid = int(r[0])
                    lst = stats_map.setdefault(fid, [])
                    lst.append(
                        {
                            "column_id": int(r[1]),
                            "column_size_bytes": int(r[2]) if r[2] is not None else None,
                            "value_count": int(r[3]) if r[3] is not None else None,
                            "null_count": int(r[4]) if r[4] is not None else None,
                            "min_value": str(r[5]) if r[5] is not None else None,
                            "max_value": str(r[6]) if r[6] is not None else None,
                            "contains_nan": bool(r[7]) if r[7] is not None else None,
                            "extra_stats": str(r[8]) if r[8] is not None else None,
                        }
                    )

            pv_map: dict[int, list[dict[str, Any]]] = {}
            pv_sql = text(
                """
                select data_file_id, partition_key_index, partition_value
                from ducklake_file_partition_value
                where data_file_id in :file_ids
                """
            ).bindparams(bindparam("file_ids", expanding=True))
            pv_rows = conn.execute(pv_sql, {"file_ids": list(data_file_ids)}).all()
            for r in pv_rows:
                fid = int(r[0])
                lst = pv_map.setdefault(fid, [])
                lst.append(
                    {
                        "partition_key_index": int(r[1]),
                        "partition_value": str(r[2]),
                    }
                )

            column_stats: list[list[dict[str, Any]]] = [
                stats_map.get(fid, []) for fid in data_file_ids
            ]
            partition_values: list[list[dict[str, Any]]] = [
                pv_map.get(fid, []) for fid in data_file_ids
            ]

            arrays = [
                pa.array(data_file_ids, type=pa.uint64()),
                pa.array(table_ids, type=pa.uint64()),
                pa.array(begin_snapshots, type=pa.uint64()),
                pa.array(end_snapshots, type=pa.uint64()),
                pa.array(file_orders, type=pa.uint64()),
                pa.array(paths, type=pa.large_string()),
                pa.array(path_is_relative, type=pa.bool_()),
                pa.array(file_formats, type=pa.large_string()),
                pa.array(record_counts, type=pa.uint64()),
                pa.array(file_sizes, type=pa.uint64()),
                pa.array(footer_sizes, type=pa.uint64()),
                pa.array(row_id_starts, type=pa.uint64()),
                pa.array(partition_ids, type=pa.uint64()),
                pa.array(encryption_keys, type=pa.large_string()),
                pa.array(partial_file_infos, type=pa.large_string()),
                pa.array(mapping_ids, type=pa.uint64()),
                pa.array(column_stats, type=column_stats_type),
                pa.array(partition_values, type=partition_values_type),
            ]

            yield pa.RecordBatch.from_arrays(arrays, schema=schema)
