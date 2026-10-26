use crate::function::common::ScalarFunctionBuilder as F;

pub(super) fn list_built_in_hash_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "crc32",
            F::scalar_udf(datafusion_spark::function::hash::crc32),
        ),
        (
            "hash",
            F::udf(sail_function::scalar::hash::spark_murmur3_hash::SparkMurmur3Hash::new()),
        ),
        ("md5", F::kernel("md5")),
        ("sha", F::scalar_udf(datafusion_spark::function::hash::sha1)),
        (
            "sha1",
            F::scalar_udf(datafusion_spark::function::hash::sha1),
        ),
        (
            "xxhash64",
            F::udf(datafusion_spark::function::hash::xxhash64::SparkXxhash64::new()),
        ),
    ]
}
