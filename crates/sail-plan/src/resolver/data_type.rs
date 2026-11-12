use std::sync::Arc;

use arrow_schema as adt;
use sail_common::spec;
use sail_common_datafusion::data_type::DataTypeResolver;

use crate::config::DefaultTimestampType;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    fn data_type_resolver(&self, allow_large_var_types: bool) -> DataTypeResolver {
        DataTypeResolver {
            session_timezone: Arc::clone(&self.config.session_timezone),
            default_timestamp_ltz: self.config.default_timestamp_type
                == DefaultTimestampType::TimestampLtz,
            use_large_var_types: self.config.arrow_use_large_var_types && allow_large_var_types,
        }
    }

    pub fn resolve_data_type_for_plan(
        &self,
        data_type: &spec::DataType,
    ) -> PlanResult<adt::DataType> {
        let mut state = PlanResolverState::new();
        self.resolve_data_type(data_type, &mut state)
    }

    pub(super) fn resolve_data_type(
        &self,
        data_type: &spec::DataType,
        state: &mut PlanResolverState,
    ) -> PlanResult<adt::DataType> {
        Ok(self
            .data_type_resolver(state.config().arrow_allow_large_var_types)
            .resolve_data_type(data_type)?)
    }

    pub(super) fn resolve_field(
        &self,
        field: &spec::Field,
        state: &mut PlanResolverState,
    ) -> PlanResult<adt::Field> {
        Ok(self
            .data_type_resolver(state.config().arrow_allow_large_var_types)
            .resolve_field(field)?)
    }

    pub(super) fn resolve_fields(
        &self,
        fields: &spec::Fields,
        state: &mut PlanResolverState,
    ) -> PlanResult<adt::Fields> {
        Ok(self
            .data_type_resolver(state.config().arrow_allow_large_var_types)
            .resolve_fields(fields)?)
    }

    pub(super) fn resolve_schema(
        &self,
        schema: spec::Schema,
        state: &mut PlanResolverState,
    ) -> PlanResult<adt::Schema> {
        Ok(self
            .data_type_resolver(state.config().arrow_allow_large_var_types)
            .resolve_schema(schema)?)
    }

    pub fn resolve_time_unit(time_unit: &spec::TimeUnit) -> PlanResult<adt::TimeUnit> {
        Ok(DataTypeResolver::resolve_time_unit(time_unit)?)
    }

    pub fn resolve_union_mode(union_mode: &spec::UnionMode) -> adt::UnionMode {
        DataTypeResolver::resolve_union_mode(union_mode)
    }

    pub fn resolve_timezone(
        &self,
        timestamp_type: &spec::TimestampType,
    ) -> PlanResult<Option<Arc<str>>> {
        Ok(self
            .data_type_resolver(false)
            .resolve_timezone(timestamp_type)?)
    }
}
