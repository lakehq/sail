use crate::error::{PlanError, PlanResult};
use framework_common::object::DynObject;
use framework_common::{impl_dyn_object_traits, spec};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimestampType {
    TimestampLtz,
    TimestampNtz,
}

pub trait DataTypeFormatter: DynObject + Debug + Send + Sync {
    fn to_simple_string(&self, data_type: spec::DataType) -> PlanResult<String>;
}

impl_dyn_object_traits!(DataTypeFormatter);

#[derive(Debug, PartialEq, Eq, Hash)]
struct DefaultDataTypeFormatter;

impl DataTypeFormatter for DefaultDataTypeFormatter {
    fn to_simple_string(&self, _data_type: spec::DataType) -> PlanResult<String> {
        Err(PlanError::unsupported("default data type formatter"))
    }
}

// The generic type parameter is used to work around the issue deriving `PartialEq` for `dyn` trait.
// See also: https://github.com/rust-lang/rust/issues/78808
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanConfig<F: ?Sized = dyn DataTypeFormatter> {
    /// The time zone of the session.
    pub time_zone: String,
    /// The default timestamp type.
    pub timestamp_type: TimestampType,
    /// The data type formatter.
    pub data_type_formatter: Arc<F>,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            time_zone: "UTC".to_string(),
            timestamp_type: TimestampType::TimestampLtz,
            data_type_formatter: Arc::new(DefaultDataTypeFormatter),
        }
    }
}

impl PlanConfig {
    pub fn with_data_type_formatter(self, data_type_formatter: Arc<dyn DataTypeFormatter>) -> Self {
        Self {
            data_type_formatter,
            ..self
        }
    }
}
