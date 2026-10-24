use std::sync::Arc;

use arrow_schema as adt;
use sail_common::spec;

use crate::config::{DefaultTimestampType, PlanConfig};
use crate::error::PlanResult;

/// Resolves spec data types and literals using session configuration.
pub struct DataTypeResolver<'a> {
    pub config: &'a PlanConfig,
    pub allow_large_var_types: bool,
}
impl DataTypeResolver<'_> {
    fn shared(&self) -> sail_common_datafusion::data_type::DataTypeResolver {
        sail_common_datafusion::data_type::DataTypeResolver {
            session_timezone: Arc::clone(&self.config.session_timezone),
            default_timestamp_ltz: self.config.default_timestamp_type
                == DefaultTimestampType::TimestampLtz,
            use_large_var_types: self.config.arrow_use_large_var_types
                && self.allow_large_var_types,
        }
    }
    pub fn resolve_data_type(&self, data_type: &spec::DataType) -> PlanResult<adt::DataType> {
        Ok(self.shared().resolve_data_type(data_type)?)
    }
    pub fn resolve_field(&self, field: &spec::Field) -> PlanResult<adt::Field> {
        Ok(self.shared().resolve_field(field)?)
    }
    pub fn resolve_fields(&self, fields: &spec::Fields) -> PlanResult<adt::Fields> {
        Ok(self.shared().resolve_fields(fields)?)
    }
    pub fn resolve_schema(&self, schema: spec::Schema) -> PlanResult<adt::Schema> {
        Ok(self.shared().resolve_schema(schema)?)
    }
    pub fn resolve_timezone(
        &self,
        timestamp_type: &spec::TimestampType,
    ) -> PlanResult<Option<Arc<str>>> {
        Ok(self.shared().resolve_timezone(timestamp_type)?)
    }
    pub fn resolve_time_unit(unit: &spec::TimeUnit) -> PlanResult<adt::TimeUnit> {
        Ok(sail_common_datafusion::data_type::DataTypeResolver::resolve_time_unit(unit)?)
    }
    pub fn resolve_union_mode(mode: &spec::UnionMode) -> adt::UnionMode {
        sail_common_datafusion::data_type::DataTypeResolver::resolve_union_mode(mode)
    }
}
