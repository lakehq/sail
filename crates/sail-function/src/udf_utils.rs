//! Shared helpers for `ScalarUDFImpl` implementations that derive their output field from
//! the argument *fields* (types plus nullability) rather than from the argument types alone.

use datafusion::arrow::datatypes::DataType;
use datafusion_expr::ReturnFieldArgs;

/// The argument types, for UDFs whose output type is computed by an `output_type` helper.
pub(crate) fn arg_data_types(args: &ReturnFieldArgs) -> Vec<DataType> {
    args.arg_fields
        .iter()
        .map(|field| field.data_type().clone())
        .collect()
}

/// Whether any argument is nullable.
///
/// This is the Arrow-side equivalent of Spark's rule for a null-intolerant expression with no
/// `nullable` override: `nullable = children.exists(_.nullable)`. Only use it for functions
/// that Spark actually declares that way — deriving it for a function Spark declares
/// unconditionally nullable would stamp `nullable = false` on a column that really can be
/// NULL, which is the unsound direction.
pub(crate) fn any_arg_nullable(args: &ReturnFieldArgs) -> bool {
    args.arg_fields.iter().any(|field| field.is_nullable())
}

/// Emits the `ScalarUDFImpl::return_type` stub for a UDF that builds its output field in
/// `return_field_from_args`.
///
/// DataFusion only calls `return_type` from its own default `return_field_from_args`, which
/// such a UDF overrides, so reaching this method means something bypassed the field-aware
/// path — a bug, not a user error. Paths are fully qualified so the macro works in any module
/// without extra imports.
#[macro_export]
macro_rules! unused_return_type {
    () => {
        fn return_type(
            &self,
            _arg_types: &[::datafusion_common::arrow::datatypes::DataType],
        ) -> ::datafusion_common::Result<::datafusion_common::arrow::datatypes::DataType> {
            ::datafusion_common::internal_err!(
                "{}: `return_type` should not be called; `return_field_from_args` is used instead",
                self.name()
            )
        }
    };
}
