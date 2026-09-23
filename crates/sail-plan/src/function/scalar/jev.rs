use sail_function::scalar::jev::{JevFunction, JevKind};

use crate::function::common::ScalarFunction;

/// Registers Jev's typed and multi-question SQL functions.
pub(super) fn list_built_in_jev_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("jev_noul", F::udf(JevFunction::new(JevKind::Noul))),
        ("jev_choice", F::udf(JevFunction::new(JevKind::Choice))),
        ("jev_score", F::udf(JevFunction::new(JevKind::Score))),
        ("jev_evaluate", F::udf(JevFunction::new(JevKind::Evaluate))),
    ]
}
