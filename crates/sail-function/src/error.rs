use datafusion::arrow::datatypes::{DataType, Fields};
// TODO: https://github.com/apache/spark/tree/master/common/utils/src/main/resources/error
use datafusion_common::{
    DataFusionError, exec_datafusion_err, internal_datafusion_err, plan_datafusion_err,
};
use sail_sql_analyzer::parser::to_sql_id;

pub fn invalid_arg_count_exec_err(
    function_name: &str,
    required_range: (i32, i32),
    provided: usize,
) -> DataFusionError {
    let (min_required, max_required) = required_range;
    let required = if min_required == max_required {
        format!(
            "{min_required} argument{}",
            if min_required == 1 { "" } else { "s" }
        )
    } else {
        format!("{min_required} to {max_required} arguments")
    };
    exec_datafusion_err!("Spark `{function_name}` function requires {required}, got {provided}")
}

pub fn unsupported_data_type_exec_err(
    function_name: &str,
    required: &str,
    provided: &DataType,
) -> DataFusionError {
    exec_datafusion_err!(
        "Unsupported Data Type: Spark `{function_name}` function expects {required}, got {provided}"
    )
}

pub fn unsupported_data_types_exec_err(
    function_name: &str,
    required: &str,
    provided: &[DataType],
) -> DataFusionError {
    exec_datafusion_err!(
        "Unsupported Data Type: Spark `{function_name}` function expects {required}, got {}",
        provided
            .iter()
            .map(|dt| format!("{dt}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub fn generic_exec_err(function_name: &str, message: &str) -> DataFusionError {
    exec_datafusion_err!("Spark `{function_name}` function: {message}")
}

pub fn generic_internal_err(function_name: &str, message: &str) -> DataFusionError {
    internal_datafusion_err!("Spark `{function_name}` function: {message}")
}

/// Builds the error the analyzer raises when a level of a struct path matches no field. Spark
/// looks up every level of the path but the last with `ExtractValue`
/// (`org.apache.spark.sql.catalyst.expressions.UpdateFields#updateFieldsHelper`), so a level that
/// is not there is a missing field rather than one to create.
pub fn field_not_found_plan_err(name: &str, fields: &Fields) -> DataFusionError {
    let fields = fields
        .iter()
        .map(|x| to_sql_id(x.name()))
        .collect::<Vec<_>>()
        .join(", ");
    plan_datafusion_err!(
        "[FIELD_NOT_FOUND] No such struct field {} in {fields}.",
        to_sql_id(name)
    )
}

/// Builds the error the analyzer raises when a level of a struct path matches more than one
/// field. Spark looks up every level but the last with `ExtractValue`, whose `findField` refuses
/// a name that matches twice (`complexTypeExtractors.scala:186-189`).
pub fn ambiguous_field_plan_err(name: &str, count: usize) -> DataFusionError {
    plan_datafusion_err!(
        "[AMBIGUOUS_REFERENCE_TO_FIELDS] Ambiguous reference to the field {}. \
         It appears {count} times in the schema.",
        to_sql_id(name)
    )
}
