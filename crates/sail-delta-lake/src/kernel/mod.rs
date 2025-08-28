pub mod arrow;
pub mod models;
pub mod snapshot;
pub mod transaction;

use std::sync::LazyLock;

use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;

pub(crate) static ARROW_HANDLER: LazyLock<ArrowEvaluationHandler> =
    LazyLock::new(|| ArrowEvaluationHandler {});
