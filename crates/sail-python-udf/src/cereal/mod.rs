pub mod partial_pyspark_udf;
pub mod partial_python_udf;
pub mod pyspark_udtf;

pub const PY_SPARK_NON_UDF: i32 = 0;
pub const PY_SPARK_SQL_BATCHED_UDF: i32 = 100;
pub const PY_SPARK_SQL_ARROW_BATCHED_UDF: i32 = 101;
pub const PY_SPARK_SQL_SCALAR_PANDAS_UDF: i32 = 200;
pub const PY_SPARK_SQL_GROUPED_MAP_PANDAS_UDF: i32 = 201;
pub const PY_SPARK_SQL_GROUPED_AGG_PANDAS_UDF: i32 = 202;
pub const PY_SPARK_SQL_WINDOW_AGG_PANDAS_UDF: i32 = 203;
pub const PY_SPARK_SQL_SCALAR_PANDAS_ITER_UDF: i32 = 204;
pub const PY_SPARK_SQL_MAP_PANDAS_ITER_UDF: i32 = 205;
pub const PY_SPARK_SQL_COGROUPED_MAP_PANDAS_UDF: i32 = 206;
pub const PY_SPARK_SQL_MAP_ARROW_ITER_UDF: i32 = 207;
pub const PY_SPARK_SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE: i32 = 208;
pub const PY_SPARK_SQL_TABLE_UDF: i32 = 300;
pub const PY_SPARK_SQL_ARROW_TABLE_UDF: i32 = 301;

pub fn is_pyspark_arrow_udf(udf_type: &i32) -> bool {
    udf_type == &PY_SPARK_SQL_ARROW_BATCHED_UDF
        || udf_type == &PY_SPARK_SQL_MAP_ARROW_ITER_UDF
        || udf_type == &PY_SPARK_SQL_ARROW_TABLE_UDF
}

pub fn is_pyspark_pandas_udf(udf_type: &i32) -> bool {
    udf_type == &PY_SPARK_SQL_SCALAR_PANDAS_UDF
        || udf_type == &PY_SPARK_SQL_GROUPED_MAP_PANDAS_UDF
        || udf_type == &PY_SPARK_SQL_GROUPED_AGG_PANDAS_UDF
        || udf_type == &PY_SPARK_SQL_WINDOW_AGG_PANDAS_UDF
        || udf_type == &PY_SPARK_SQL_SCALAR_PANDAS_ITER_UDF
        || udf_type == &PY_SPARK_SQL_MAP_PANDAS_ITER_UDF
        || udf_type == &PY_SPARK_SQL_COGROUPED_MAP_PANDAS_UDF
        || udf_type == &PY_SPARK_SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE
}
