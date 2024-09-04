use sail_common::spec;

use crate::error::{SparkError, SparkResult};
use crate::spark::connect::analyze_plan_request::explain::ExplainMode;

impl TryFrom<ExplainMode> for spec::ExplainMode {
    type Error = SparkError;

    fn try_from(value: ExplainMode) -> SparkResult<spec::ExplainMode> {
        match value {
            ExplainMode::Unspecified => Ok(spec::ExplainMode::Unspecified),
            ExplainMode::Simple => Ok(spec::ExplainMode::Simple),
            ExplainMode::Extended => Ok(spec::ExplainMode::Extended),
            ExplainMode::Codegen => Ok(spec::ExplainMode::Codegen),
            ExplainMode::Cost => Ok(spec::ExplainMode::Cost),
            ExplainMode::Formatted => Ok(spec::ExplainMode::Formatted),
        }
    }
}
