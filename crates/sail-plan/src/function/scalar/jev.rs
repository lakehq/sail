use sail_function::scalar::jev::JevKind;

use crate::function::common::{ScalarFunction, ScalarFunctionBuilder as F};

pub(super) fn list_built_in_jev_functions() -> Vec<(&'static str, ScalarFunction)> {
    JevKind::ALL
        .into_iter()
        .map(|kind| {
            let udf = kind.udf();
            (
                kind.name(),
                F::var_arg(move |args| Ok::<_, crate::error::PlanError>(udf.call(args))),
            )
        })
        .collect()
}
