import duckdb
import pandas as pd
import pyarrow as pa
import pytest


@pytest.fixture(scope="module")
def duckdb_conn():
	conn = duckdb.connect(":memory:")
	try:
		conn.execute("INSTALL ducklake")
		conn.execute("LOAD ducklake")
	except (RuntimeError, duckdb.Error) as e:
		pytest.skip(f"DuckLake extension not available: {e}")
	return conn


def _attach(conn: duckdb.DuckDBPyConnection, meta_path: str, data_path: str, schema_name: str = "dl"):
	conn.execute(
		f"ATTACH 'ducklake:sqlite:{meta_path}' AS {schema_name} (DATA_PATH '{data_path}/')"
	)


def _detach(conn: duckdb.DuckDBPyConnection, schema_name: str = "dl"):
	conn.execute(f"DETACH {schema_name}")


def _append_batches(conn: duckdb.DuckDBPyConnection, table_ident: str, batches: list[pd.DataFrame]):
	for df in batches:
		tbl = pa.Table.from_pandas(df)
		conn.register("tmp_append", tbl)
		conn.execute(f"INSERT INTO {table_ident} SELECT * FROM tmp_append")
		conn.unregister("tmp_append")


def _mk_table_eq_in(conn: duckdb.DuckDBPyConnection, meta_path, data_path, ident="dl.prune_eq_in"):
	_attach(conn, meta_path, data_path)
	try:
		conn.execute(
			f"""
			CREATE TABLE {ident}(
				id BIGINT,
				year INTEGER,
				month INTEGER,
				value VARCHAR
			)
			"""
		)
		batches = [
			pd.DataFrame({"id": [1, 2], "year": [2023, 2023], "month": [1, 1], "value": ["a", "b"]}).astype(
				{"id": "int64", "year": "int32", "month": "int32"}
			),
			pd.DataFrame({"id": [3, 4], "year": [2023, 2023], "month": [2, 2], "value": ["c", "d"]}).astype(
				{"id": "int64", "year": "int32", "month": "int32"}
			),
			pd.DataFrame({"id": [5, 6], "year": [2024, 2024], "month": [1, 1], "value": ["e", "f"]}).astype(
				{"id": "int64", "year": "int32", "month": "int32"}
			),
			pd.DataFrame({"id": [7, 8], "year": [2024, 2024], "month": [2, 2], "value": ["g", "h"]}).astype(
				{"id": "int64", "year": "int32", "month": "int32"}
			),
		]
		_append_batches(conn, ident, batches)
	finally:
		_detach(conn)
	return ident.split(".")[-1]


def _mk_table_cmp(conn: duckdb.DuckDBPyConnection, meta_path, data_path, ident="dl.prune_cmp"):
	_attach(conn, meta_path, data_path)
	try:
		conn.execute(
			f"""
			CREATE TABLE {ident}(
				id BIGINT,
				year INTEGER,
				month INTEGER
			)
			"""
		)
		data = []
		for year in [2021, 2022, 2023, 2024]:
			for month in [1, 6, 12]:
				data.append({"id": len(data) + 1, "year": year, "month": month})
		for i in range(0, len(data), 6):
			df = pd.DataFrame(data[i:i+6]).astype({"id": "int64", "year": "int32", "month": "int32"})
			_append_batches(conn, ident, [df])
	finally:
		_detach(conn)
	return ident.split(".")[-1]


def _mk_table_null_bool(conn: duckdb.DuckDBPyConnection, meta_path, data_path, ident="dl.prune_null_bool"):
	_attach(conn, meta_path, data_path)
	try:
		conn.execute(
			f"""
			CREATE TABLE {ident}(
				id BIGINT,
				region VARCHAR,
				active BOOLEAN
			)
			"""
		)
		_append_batches(
			conn,
			ident,
			[
				pd.DataFrame([{"id": 1, "region": None, "active": True}, {"id": 2, "region": None, "active": True}]).astype({"id": "int64"}),
				pd.DataFrame([{"id": 3, "region": "US", "active": False}, {"id": 4, "region": "EU", "active": False}]).astype({"id": "int64"}),
			],
		)
	finally:
		_detach(conn)
	return ident.split(".")[-1]


def _mk_table_string(conn: duckdb.DuckDBPyConnection, meta_path, data_path, ident="dl.prune_string"):
	_attach(conn, meta_path, data_path)
	try:
		conn.execute(
			f"""
			CREATE TABLE {ident}(
				id BIGINT,
				dept VARCHAR,
				team VARCHAR
			)
			"""
		)
		_append_batches(
			conn,
			ident,
			[
				pd.DataFrame(
					[
						{"id": 1, "dept": "engineering", "team": "backend"},
						{"id": 2, "dept": "engineering", "team": "frontend"},
						{"id": 3, "dept": "marketing", "team": "growth"},
						{"id": 4, "dept": "sales", "team": "enterprise"},
					]
				).astype({"id": "int64"})
			],
		)
	finally:
		_detach(conn)
	return ident.split(".")[-1]


def _mk_table_limit(conn: duckdb.DuckDBPyConnection, meta_path, data_path, ident="dl.prune_limit"):
	_attach(conn, meta_path, data_path)
	try:
		conn.execute(
			f"""
			CREATE TABLE {ident}(
				id BIGINT,
				flag BOOLEAN
			)
			"""
		)
		rows = [{"id": i, "flag": i % 2 == 0} for i in range(100)]
		for i in range(0, len(rows), 20):
			df = pd.DataFrame(rows[i:i+20]).astype({"id": "int64"})
			_append_batches(conn, ident, [df])
	finally:
		_detach(conn)
	return ident.split(".")[-1]


@pytest.fixture
def ducklake_paths(tmp_path):
	meta = tmp_path / "meta.ducklake"
	data = tmp_path / "data"
	data.mkdir(exist_ok=True)
	return str(meta), str(data)


def test_pruning_equality_filters(spark, duckdb_conn, ducklake_paths):
	meta_path, data_path = ducklake_paths
	table = _mk_table_eq_in(duckdb_conn, meta_path, data_path)
	df = (
		spark.read.format("ducklake")
		.options(url=f"sqlite:///{meta_path}", table=table, base_path=f"file://{data_path}/")
		.load()
	)
	assert df.filter("year = 2023").count() == 4
	assert df.filter("year = 2023 AND month = 1").count() == 2


def test_pruning_in_clause(spark, duckdb_conn, ducklake_paths):
	meta_path, data_path = ducklake_paths
	table = _mk_table_eq_in(duckdb_conn, meta_path, data_path)
	df = (
		spark.read.format("ducklake")
		.options(url=f"sqlite:///{meta_path}", table=table, base_path=f"file://{data_path}/")
		.load()
	)
	assert df.filter("month IN (2)").count() == 4


def test_comparison_and_between(spark, duckdb_conn, ducklake_paths):
	meta_path, data_path = ducklake_paths
	table = _mk_table_cmp(duckdb_conn, meta_path, data_path)
	df = (
		spark.read.format("ducklake")
		.options(url=f"sqlite:///{meta_path}", table=table, base_path=f"file://{data_path}/")
		.load()
	)
	assert df.filter("year > 2022").count() == 6
	assert df.filter("year BETWEEN 2022 AND 2023").count() == 12 // 2
	assert df.filter("year >= 2023 AND month >= 6").count() == 4


def test_null_and_boolean(spark, duckdb_conn, ducklake_paths):
	meta_path, data_path = ducklake_paths
	table = _mk_table_null_bool(duckdb_conn, meta_path, data_path)
	df = (
		spark.read.format("ducklake")
		.options(url=f"sqlite:///{meta_path}", table=table, base_path=f"file://{data_path}/")
		.load()
	)
	assert df.filter("region IS NULL").count() == 2
	assert df.filter("active = true").count() == 2


def test_string_in_and_range_pruning(spark, duckdb_conn, ducklake_paths):
	meta_path, data_path = ducklake_paths
	table = _mk_table_string(duckdb_conn, meta_path, data_path)
	df = (
		spark.read.format("ducklake")
		.options(url=f"sqlite:///{meta_path}", table=table, base_path=f"file://{data_path}/")
		.load()
	)
	assert df.filter("team IN ('backend','frontend')").count() == 2
	assert df.filter("dept > 'engineering'").count() == 2


def test_limit_pushdown_behavior(spark, duckdb_conn, ducklake_paths):
	meta_path, data_path = ducklake_paths
	table = _mk_table_limit(duckdb_conn, meta_path, data_path)
	df = (
		spark.read.format("ducklake")
		.options(url=f"sqlite:///{meta_path}", table=table, base_path=f"file://{data_path}/")
		.load()
	)
	assert df.filter("flag = true").limit(7).count() == 7


