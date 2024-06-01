use crate::function::common::Function;

pub(super) fn list_built_in_misc_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("aes_decrypt", F::unknown("aes_decrypt")),
        ("aes_encrypt", F::unknown("aes_encrypt")),
        ("assert_true", F::unknown("assert_true")),
        ("bitmap_bit_position", F::unknown("bitmap_bit_position")),
        ("bitmap_bucket_number", F::unknown("bitmap_bucket_number")),
        ("bitmap_count", F::unknown("bitmap_count")),
        ("current_catalog", F::unknown("current_catalog")),
        ("current_database", F::unknown("current_database")),
        ("current_schema", F::unknown("current_schema")),
        ("current_user", F::unknown("current_user")),
        ("equal_null", F::unknown("equal_null")),
        ("hll_sketch_estimate", F::unknown("hll_sketch_estimate")),
        ("hll_union", F::unknown("hll_union")),
        (
            "input_file_block_length",
            F::unknown("input_file_block_length"),
        ),
        (
            "input_file_block_start",
            F::unknown("input_file_block_start"),
        ),
        ("input_file_name", F::unknown("input_file_name")),
        ("java_method", F::unknown("java_method")),
        (
            "monotonically_increasing_id",
            F::unknown("monotonically_increasing_id"),
        ),
        ("raise_error", F::unknown("raise_error")),
        ("reflect", F::unknown("reflect")),
        ("spark_partition_id", F::unknown("spark_partition_id")),
        ("try_aes_decrypt", F::unknown("try_aes_decrypt")),
        ("typeof", F::unknown("typeof")),
        ("user", F::unknown("user")),
        ("uuid", F::unknown("uuid")),
        ("version", F::unknown("version")),
    ]
}
