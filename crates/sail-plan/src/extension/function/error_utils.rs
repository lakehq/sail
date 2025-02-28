use datafusion::arrow::datatypes::DataType;
// TODO: https://github.com/apache/spark/tree/master/common/utils/src/main/resources/error
use datafusion_common::{exec_datafusion_err, DataFusionError};

pub fn invalid_arg_count_exec_err(
    function_name: &str,
    required: i32,
    provided: usize,
) -> DataFusionError {
    let plural_suffix = if required == 1 { "" } else { "s" };
    exec_datafusion_err!("Spark `{function_name}` function requires {required} argument{plural_suffix}, got {provided}")
}

pub fn unsupported_data_type_exec_err(
    function_name: &str,
    required: &DataType,
    provided: &DataType,
) -> DataFusionError {
    exec_datafusion_err!("Unsupported Data Type: Spark `{function_name}` function expects {required}, got {provided}")
}
