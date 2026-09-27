use std::sync::Arc;

use datafusion_common::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{DFSchemaRef, ExprSchema};
use datafusion_expr::{Distinct, DistinctOn, Expr, LogicalPlan};
use sail_common::spec;
use sail_common_datafusion::variant::is_marked_variant_storage_type;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_deduplicate(
        &self,
        deduplicate: spec::Deduplicate,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Deduplicate {
            input,
            column_names,
            all_columns_as_keys,
            within_watermark,
        } = deduplicate;
        let input = self
            .resolve_query_plan_with_hidden_fields(*input, state)
            .await?;
        let schema = input.schema();
        if within_watermark {
            return Err(PlanError::todo("deduplicate within watermark"));
        }
        if !column_names.is_empty() && !all_columns_as_keys {
            // The name selects output columns, so it is matched with the resolver alone, and
            // every column that matches becomes a key.
            // A column that is named more than once is a key only once, since the same
            // expression cannot be repeated in the plan.
            let mut on_expr: Vec<Expr> = Vec::new();
            for name in &column_names {
                for column in self.resolve_columns_by_resolver(schema, name.as_ref(), state)? {
                    let expr = Expr::Column(column);
                    if !on_expr.contains(&expr) {
                        on_expr.push(expr);
                    }
                }
            }
            // Only the columns that are compared have to be ordered, so a map elsewhere in the
            // frame is not a problem.
            for expr in &on_expr {
                if let Expr::Column(column) = expr {
                    let field = schema.field_from_column(column)?;
                    self.reject_map_column_in_set_operation_for_field(field, state)?;
                }
            }
            let select_expr: Vec<Expr> = schema.columns().into_iter().map(Expr::Column).collect();
            Ok(LogicalPlan::Distinct(Distinct::On(DistinctOn::try_new(
                on_expr,
                select_expr,
                None,
                Arc::new(input),
            )?)))
        } else if column_names.is_empty() && all_columns_as_keys {
            self.reject_map_column_in_set_operation(schema, state)?;
            Ok(LogicalPlan::Distinct(Distinct::All(Arc::new(input))))
        } else {
            Err(PlanError::invalid(
                "must either specify deduplicate column names or use all columns as keys",
            ))
        }
    }
}

impl PlanResolver<'_> {
    /// A set operation compares whole rows, and maps and variants have no order of their own, so
    /// Spark rejects a column whose type contains either. `DISTINCT` is one of those operations.
    pub(in crate::resolver) fn reject_map_column_in_set_operation(
        &self,
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> PlanResult<()> {
        for field in schema.fields() {
            self.reject_map_column_in_set_operation_for_field(field, state)?;
        }
        Ok(())
    }

    /// The same check for one column.
    pub(in crate::resolver) fn reject_map_column_in_set_operation_for_field(
        &self,
        field: &FieldRef,
        state: &PlanResolverState,
    ) -> PlanResult<()> {
        if contains_map_type(field.data_type()) {
            // Falling back to the field id here would write the internal name (`#6`) into a message
            // the user reads, so a missing entry is the invariant break it is.
            let name = state.get_field_info(field.name())?.name().to_string();
            let data_type = self.spark_type_name(field.data_type())?;
            return Err(PlanError::AnalysisError(format!(
                "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: \
                 Cannot have MAP type columns in DataFrame which calls set operations (INTERSECT, \
                 EXCEPT, etc.), but the type of column `{}` is \"{}\".",
                name.replace('`', "``"),
                data_type
            )));
        }
        if contains_variant_type(field.data_type()) {
            let name = state.get_field_info(field.name())?.name().to_string();
            return Err(PlanError::AnalysisError(format!(
                "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_VARIANT_TYPE] The feature is not \
                 supported: Cannot have VARIANT type columns in DataFrame which calls set \
                 operations (INTERSECT, EXCEPT, etc.), but the type of column `{}` is \"{}\".",
                name.replace('`', "``"),
                self.spark_type_name(field.data_type())?
            )));
        }
        Ok(())
    }
}

/// Whether the type is a map or holds one at any depth, the way Spark looks for it.
fn contains_map_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(..) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => contains_map_type(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|x| contains_map_type(x.data_type())),
        _ => false,
    }
}

/// Whether a type is a variant or holds one at any depth, the way Spark's
/// `DataType.existsRecursively` does for set-operation keys.
fn contains_variant_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(_) if is_marked_variant_storage_type(data_type) => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_variant_type(field.data_type())),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => contains_variant_type(field.data_type()),
        _ => false,
    }
}
