//! Aggregate and window call typing shares the scalar cast-field policy.

use arrow_schema::FieldRef;
use datafusion_common::Result;
use datafusion_expr::function::WindowUDFFieldArgs;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{AggregateUDF, WindowFunctionDefinition};

use super::coercion::cast_field;
use super::scalar::ScalarTyping;

pub fn resolve(function: &AggregateUDF, arguments: &[FieldRef]) -> Result<ScalarTyping> {
    // GROUPING_ID() binds to all grouping keys in the native resolver. Its
    // empty argument list is meaningful before that binding takes place.
    let targets = if function.name() == "grouping_id" && arguments.is_empty() {
        vec![]
    } else {
        fields_with_udf(arguments, function)?
    };
    super::coercion::validate_arity(function.name(), targets.len(), arguments.len())?;
    let arguments = targets
        .iter()
        .zip(arguments)
        .map(|(target, source)| cast_field(source, target.data_type()))
        .collect::<Vec<_>>();
    let result = function.return_field(&arguments)?;
    Ok(ScalarTyping { arguments, result })
}

pub fn resolve_window(
    function: &WindowFunctionDefinition,
    arguments: &[FieldRef],
) -> Result<ScalarTyping> {
    match function {
        WindowFunctionDefinition::AggregateUDF(function) => resolve(function, arguments),
        WindowFunctionDefinition::WindowUDF(function) => {
            let targets = fields_with_udf(arguments, function.as_ref())?;
            super::coercion::validate_arity(function.name(), targets.len(), arguments.len())?;
            let arguments = targets
                .iter()
                .zip(arguments)
                .map(|(target, source)| cast_field(source, target.data_type()))
                .collect::<Vec<_>>();
            let result = function.field(WindowUDFFieldArgs::new(&arguments, ""))?;
            Ok(ScalarTyping { arguments, result })
        }
    }
}
