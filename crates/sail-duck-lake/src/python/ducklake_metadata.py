from __future__ import annotations

from typing import Any

from sqlalchemy import bindparam, create_engine, text

_ENGINES: dict[str, Any] = {}


def _normalize_sqlalchemy_url(url: str) -> str:
    # Normalize Postgres URLs to use psycopg driver for SQLAlchemy.
    # Accept postgres:// or postgresql:// and coerce to postgresql+psycopg://
    if url.startswith(("postgres://", "postgresql://")):
        # If already has an explicit driver, preserve it
        if url.startswith("postgresql+"):
            return url
        return "postgresql+psycopg://" + url.split("://", 1)[1]
    # Support legacy "sqlite://<path>" by converting to SQLAlchemy format.
    if url.startswith("sqlite://"):
        if url.startswith(("sqlite:////", "sqlite:///")):
            return url
        rest = url[len("sqlite://") :]
        if not rest:
            return "sqlite://"
        # Absolute path (starts with '/'): prefer four slashes for absolute
        if rest.startswith("/"):
            return "sqlite:////" + rest.lstrip("/")
        # Relative path
        return "sqlite:///" + rest
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


def list_data_files(url: str, table_id: int, snapshot_id: int | None) -> list[dict[str, Any]]:
    sql_active = text(
        """
        select data_file_id, table_id, begin_snapshot, end_snapshot, file_order,
               path, path_is_relative, file_format, record_count, file_size_bytes,
               footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id
        from ducklake_data_file
        where table_id = :tid and end_snapshot is null
        order by file_order asc
        """
    )
    sql_asof = text(
        """
        select data_file_id, table_id, begin_snapshot, end_snapshot, file_order,
               path, path_is_relative, file_format, record_count, file_size_bytes,
               footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id
        from ducklake_data_file
        where table_id = :tid
          and begin_snapshot <= :sid
          and (end_snapshot is null or end_snapshot > :sid)
        order by file_order asc
        """
    )
    with _get_engine(url).connect() as conn:
        if snapshot_id is None:
            rows = conn.execute(sql_active, {"tid": int(table_id)}).all()
        else:
            rows = conn.execute(sql_asof, {"tid": int(table_id), "sid": int(snapshot_id)}).all()
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
        stats_sql = text(
            """
            select data_file_id, column_id, column_size_bytes, value_count, null_count,
                   min_value, max_value, contains_nan, extra_stats
            from ducklake_file_column_stats
            where data_file_id in :file_ids
            """
        ).bindparams(bindparam("file_ids", expanding=True))
        pv_sql = text(
            """
            select data_file_id, partition_key_index, partition_value
            from ducklake_file_partition_value
            where data_file_id in :file_ids
            """
        ).bindparams(bindparam("file_ids", expanding=True))
        params = {"file_ids": file_ids}
        stats_rows = conn.execute(stats_sql, params).all()
        pv_rows = conn.execute(pv_sql, params).all()

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
