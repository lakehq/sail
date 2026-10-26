use std::sync::Arc;

use sail_function::scalar::jev::JevKind;

use crate::function::common::{ScalarFunction, ScalarFunctionBuilder as F};

pub(super) fn list_built_in_jev_functions() -> Vec<(&'static str, ScalarFunction)> {
    JevKind::ALL
        .into_iter()
        .map(|kind| (kind.name(), F::scalar_udf(move || Arc::new(kind.udf()))))
        .collect()
}
