# SPDX-License-Identifier: Apache-2.0

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.sql import escape_sql_string_literal


def test_dml_result_counts_server_default_and_session_override(tmp_path):
    key = "sail.spark.dml_result_counts"
    envs = {"SAIL_SPARK__DML_RESULT_COUNTS": "true"}
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        spark = sessions.create()
        other = sessions.create()
        location = escape_sql_string_literal(str(tmp_path / "items"))
        spark.sql(f"CREATE TABLE items (id INT) USING delta LOCATION '{location}'")
        insert = "INSERT INTO items VALUES (1)"
        assert spark.conf.isModifiable(key)
        assert spark.conf.get(key) == "true"
        assert spark.sql(insert).collect() == [(1,)]

        spark.conf.set(key, "false")
        with pytest.raises(Exception, match=r"INVALID_CONF_VALUE\.TYPE_MISMATCH"):
            spark.conf.set(key, "invalid")
        assert spark.conf.get(key) == "false"
        assert spark.sql(insert).collect() == []
        assert other.conf.get(key) == "true"

        spark.sql(f"RESET {key}")
        assert spark.conf.get(key) == "true"
        assert spark.sql(insert).collect() == [(1,)]
