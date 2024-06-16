use crate::formatter::{DefaultPlanFormatter, PlanFormatter};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimestampType {
    TimestampLtz,
    TimestampNtz,
}

// The generic type parameter is used to work around the issue deriving `PartialEq` for `dyn` trait.
// See also: https://github.com/rust-lang/rust/issues/78808
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanConfig<F: ?Sized = dyn PlanFormatter> {
    /// The time zone of the session.
    pub time_zone: String,
    /// The default timestamp type.
    pub timestamp_type: TimestampType,
    /// The plan formatter.
    pub plan_formatter: Arc<F>,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            time_zone: "UTC".to_string(),
            timestamp_type: TimestampType::TimestampLtz,
            plan_formatter: Arc::new(DefaultPlanFormatter),
        }
    }
}
