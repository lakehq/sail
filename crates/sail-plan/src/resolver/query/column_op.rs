use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{
    Expr, ExprSchemable, LogicalPlan, Projection, UNNAMED_TABLE, cast, col, lit,
};
use indexmap::IndexMap;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_to_df(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        Self::rename_query_output(input, columns, state)
    }

    pub(super) async fn resolve_query_to_schema(
        &self,
        input: spec::QueryPlan,
        schema: spec::Schema,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let target_schema = self.resolve_schema(schema, state)?;
        let input_names = Self::get_field_names(input.schema(), state)?;
        let mut projected_exprs = Vec::new();
        for target_field in target_schema.fields() {
            let target_name = target_field.name();
            let Some(input_idx) = input_names
                .iter()
                .position(|input_name| input_name.eq_ignore_ascii_case(target_name))
            else {
                // As in Spark, a missing column is filled with nulls if the field is nullable.
                if !target_field.is_nullable() {
                    return Err(PlanError::analysis(format!(
                        "[UNRESOLVED_COLUMN] A column with name `{target_name}` cannot be resolved"
                    )));
                }
                projected_exprs.push(
                    cast(lit(ScalarValue::Null), target_field.data_type().clone())
                        .alias(state.register_field_name(target_name)),
                );
                continue;
            };
            let (input_qualifier, input_field) = input.schema().qualified_field(input_idx);
            if input_field.is_nullable() && !target_field.is_nullable() {
                return Err(PlanError::analysis(format!(
                    "[NULLABLE_COLUMN_OR_FIELD] Column or field `{target_name}` is nullable while it's required to be non-nullable"
                )));
            }
            if !can_store_assign(input_field.data_type(), target_field.data_type()) {
                return Err(PlanError::analysis(format!(
                    "[INVALID_COLUMN_OR_FIELD_DATA_TYPE] Column or field `{target_name}` is of type \"{}\" \
                     while it's required to be \"{}\".",
                    spark_sql_type_name(input_field.data_type()),
                    spark_sql_type_name(target_field.data_type()),
                )));
            }
            let expr = Expr::Column(Column::from((input_qualifier, input_field)));
            let expr = if input_field.data_type() == target_field.data_type() {
                expr
            } else {
                expr.cast_to(target_field.data_type(), &input.schema())?
                    .alias_qualified(input_qualifier.cloned(), input_field.name())
            };
            projected_exprs.push(expr);
        }
        let projected_plan =
            LogicalPlan::Projection(Projection::try_new(projected_exprs, Arc::new(input))?);
        Ok(projected_plan)
    }

    pub(super) async fn resolve_query_with_columns_renamed(
        &self,
        input: spec::QueryPlan,
        rename_columns_map: Vec<(spec::Identifier, spec::Identifier)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;

        let schema = input.schema();
        let expr = schema
            .columns()
            .into_iter()
            .map(|column| {
                let name = state.get_field_info(column.name())?.name().to_string();
                // Spark aliases each renamed column, even to its own name, so only
                // the columns that are not renamed remain the same attributes.
                let mut renamed = name.clone();
                let mut changed = false;
                for (from, to) in &rename_columns_map {
                    if self.merge_name_key(&renamed) == self.merge_name_key(from.as_ref()) {
                        renamed = to.as_ref().to_string();
                        changed = true;
                    }
                }
                let field_id = if changed {
                    state.register_field_name(renamed)
                } else {
                    state.register_identity_field(name, column.name())?
                };
                Ok(Expr::Column(column).alias(field_id))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Self::projection_reusing_input_fields(expr, input)
    }

    pub(super) async fn resolve_query_drop(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Expr>,
        column_names: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let excluded = columns
            .into_iter()
            .filter_map(|col| {
                let spec::Expr::UnresolvedAttribute {
                    name,
                    plan_id,
                    is_metadata_column: false,
                } = col
                else {
                    return Some(Err(PlanError::invalid("expecting column to drop")));
                };
                let name: Vec<String> = name.into();
                let Ok(name) = name.one() else {
                    // Ignore nested names since they cannot match a column name.
                    // This is not an error in Spark.
                    return None;
                };
                // An error is returned when there are ambiguous columns.
                self.resolve_optional_column(schema, &name, plan_id, state)
                    .transpose()
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let excluded = excluded
            .into_iter()
            .chain(column_names.into_iter().flat_map(|name| {
                let name: String = name.into();
                // The excluded column names are allow to refer to ambiguous columns,
                // so we just check the column name here.
                self.resolve_column_candidates(schema, &name, None, state)
                    .into_iter()
            }))
            .collect::<Vec<_>>();
        let expr: Vec<Expr> = schema
            .columns()
            .into_iter()
            .filter(|column| !excluded.contains(column))
            .map(Expr::Column)
            .collect();
        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }

    pub(super) async fn resolve_query_with_columns(
        &self,
        input: spec::QueryPlan,
        aliases: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // `AliasEntry` is `(resolved_expr, seen, explicit_metadata)` where `explicit_metadata` is
        // `Some(meta)` when the user explicitly provided metadata via `withMetadata` (even empty),
        // and `None` when no metadata was specified on the alias.
        type AliasEntry = (Expr, bool, Option<Vec<(String, String)>>);

        let input = self.resolve_query_plan(input, state).await?;
        let schema = Arc::clone(input.schema());
        if aliases
            .iter()
            .any(|alias| !matches!(alias, spec::Expr::Alias { .. }))
        {
            return Err(PlanError::invalid("alias expression expected for column"));
        }
        let (input, resolved) = self.resolve_projection_list(input, aliases, state).await?;
        let mut aliases: IndexMap<String, AliasEntry> = resolved
            .into_iter()
            .map(|named| {
                let name = named
                    .name
                    .one()
                    .map_err(|_| PlanError::invalid("multi-alias for column"))?;
                let expr = if matches!(named.expr, Expr::Column(_)) {
                    named.expr.alias(&name)
                } else {
                    named.expr
                };
                Ok((name, (expr, false, Some(named.metadata))))
            })
            .collect::<PlanResult<_>>()?;
        // Index replacements once instead of scanning them for every input column.
        let mut alias_indices = HashMap::with_capacity(aliases.len());
        for (index, name) in aliases.keys().enumerate() {
            alias_indices
                .entry(self.merge_name_key(name))
                .or_insert(index);
        }
        let mut expr = schema
            .columns()
            .into_iter()
            .map(|column| {
                let name = state.get_field_info(column.name())?.name();
                // Spark replaces a column whose name matches a new column (case-insensitively
                // by default), and the replacement takes the name of the new column.
                match alias_indices
                    .get(&self.merge_name_key(name))
                    .and_then(|index| aliases.get_index_mut(*index))
                {
                    Some((name, (e, exists, metadata))) => {
                        *exists = true;
                        match metadata {
                            Some(m) if !m.is_empty() => {
                                Ok(NamedExpr::new(vec![name.to_string()], e.clone())
                                    .with_metadata(m.clone()))
                            }
                            _ => Ok(NamedExpr::new(vec![name.to_string()], e.clone())),
                        }
                    }
                    None => Ok(NamedExpr::new(vec![name.to_string()], Expr::Column(column))),
                }
            })
            .collect::<PlanResult<Vec<_>>>()?;
        for (name, (e, exists, metadata)) in &aliases {
            if !exists {
                match metadata {
                    Some(m) if !m.is_empty() => {
                        expr.push(
                            NamedExpr::new(vec![name.clone()], e.clone()).with_metadata(m.clone()),
                        );
                    }
                    _ => {
                        expr.push(NamedExpr::new(vec![name.clone()], e.clone()));
                    }
                }
            }
        }
        let (input, expr) = self.rewrite_projection::<MonotonicIdRewriter>(input, expr, state)?;
        let (input, expr) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_window_projection(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let mut expr = self.rewrite_named_expressions(expr, state)?;
        // Spark preserves qualifiers only on untouched columns. Keep them on the
        // projection expressions so missing-input recovery can cross this project.
        // DataFusion's placeholder for unnamed sources is not a Spark qualifier.
        for expr in &mut expr {
            if let Expr::Alias(alias) = expr
                && !aliases.contains_key(state.get_field_info(&alias.name)?.name())
                && let Expr::Column(column) = alias.expr.as_ref()
                && column
                    .relation
                    .as_ref()
                    .is_some_and(|relation| relation.table() != UNNAMED_TABLE)
            {
                alias.relation = column.relation.clone();
            }
        }
        Self::projection_reusing_input_fields(expr, input)
    }

    pub(super) async fn resolve_query_replace(
        &self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        replacements: Vec<spec::Replacement>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let cols_to_change: Vec<String> = columns
            .into_iter()
            .map(|ident| ident.as_ref().to_ascii_lowercase())
            .collect();
        let replacements: Vec<(Expr, Expr)> = replacements
            .into_iter()
            .map(|r| {
                Ok((
                    lit(self.resolve_literal(r.old_value, state)?),
                    lit(self.resolve_literal(r.new_value, state)?),
                ))
            })
            .collect::<PlanResult<_>>()?;

        let existing_cols_info = schema
            .iter()
            .map(|(qualifier, field)| {
                let field_info = state.get_field_info(field.name())?;
                Ok::<_, PlanError>((
                    col((qualifier, field)),
                    field.data_type(),
                    field_info.name().to_ascii_lowercase(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let existing_cols_set: HashSet<_> =
            existing_cols_info.iter().map(|(_, _, name)| name).collect();

        if let Some(missing_colname) = cols_to_change
            .iter()
            .find(|col| !existing_cols_set.contains(*col))
        {
            let existing_cols = existing_cols_info
                .iter()
                .map(|(_, _, name)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ");

            return Err(PlanError::AnalysisError(format!(
                "Cannot resolve column name \"{}\" among ({})",
                missing_colname, existing_cols
            )));
        }

        let cols_to_change_set: HashSet<_> = cols_to_change.iter().collect();

        let replace_exprs = existing_cols_info
            .into_iter()
            .map(|(column_expr, column_type, column_name)| {
                let expr = if cols_to_change.is_empty() || cols_to_change_set.contains(&column_name)
                {
                    let when_then_expr = replacements
                        .iter()
                        .filter(|(old, _new)| {
                            old.get_type(schema).is_ok_and(|old_type| {
                                old_type.is_null()
                                    || (old_type.is_numeric() && column_type.is_numeric())
                                    || (old_type == *column_type)
                            })
                        })
                        .map(|(old, new)| {
                            let old = cast(old.clone(), column_type.clone());
                            let new = cast(new.clone(), column_type.clone());
                            (Box::new(column_expr.clone().eq(old)), Box::new(new))
                        })
                        .collect::<Vec<_>>();

                    if when_then_expr.is_empty() {
                        column_expr
                    } else {
                        Expr::Case(datafusion_expr::Case {
                            expr: None,
                            when_then_expr,
                            else_expr: Some(Box::new(column_expr)),
                        })
                    }
                } else {
                    column_expr
                };
                Ok(NamedExpr::new(vec![column_name], expr))
            })
            .collect::<PlanResult<Vec<_>>>()?;

        Ok(LogicalPlan::Projection(Projection::try_new(
            self.rewrite_named_expressions(replace_exprs, state)?,
            Arc::new(input),
        )?))
    }
}

/// Returns whether a column of the type `from` can be assigned to a field of the type `to`, as in
/// Spark's `Cast.canANSIStoreAssign`, which `DataFrame.to()` uses. Numeric types are converted to
/// each other, and every other atomic type to a string, but a string is not converted to
/// any other type. The fields of structs are compared by position.
fn can_store_assign(from: &DataType, to: &DataType) -> bool {
    use DataType::*;

    fn is_string(data_type: &DataType) -> bool {
        matches!(data_type, Utf8 | LargeUtf8 | Utf8View)
    }
    fn is_numeric(data_type: &DataType) -> bool {
        data_type.is_numeric()
    }
    fn is_datetime(data_type: &DataType) -> bool {
        matches!(data_type, Date32 | Date64 | Timestamp(_, _))
    }
    fn is_nested(data_type: &DataType) -> bool {
        matches!(
            data_type,
            List(_)
                | LargeList(_)
                | FixedSizeList(_, _)
                | ListView(_)
                | LargeListView(_)
                | Struct(_)
                | Map(_, _)
                | Union(_, _)
                | Dictionary(_, _)
                | RunEndEncoded(_, _)
        )
    }
    fn resolvable_nullability(from: bool, to: bool) -> bool {
        !from || to
    }
    fn can_store_assign_fields(from: &Fields, to: &Fields) -> bool {
        from.len() == to.len()
            && from.iter().zip(to.iter()).all(|(from, to)| {
                resolvable_nullability(from.is_nullable(), to.is_nullable())
                    && can_store_assign(from.data_type(), to.data_type())
            })
    }

    if from == to || matches!(from, Null) {
        return true;
    }
    if is_numeric(from) && is_numeric(to) {
        return true;
    }
    if is_string(to) && !is_nested(from) {
        return true;
    }
    if is_datetime(from) && is_datetime(to) {
        return true;
    }
    match (from, to) {
        (List(from) | LargeList(from), List(to) | LargeList(to)) => {
            resolvable_nullability(from.is_nullable(), to.is_nullable())
                && can_store_assign(from.data_type(), to.data_type())
        }
        (Map(from, _), Map(to, _)) => match (from.data_type(), to.data_type()) {
            (Struct(from), Struct(to)) => can_store_assign_fields(from, to),
            _ => false,
        },
        (Struct(from), Struct(to)) => can_store_assign_fields(from, to),
        _ => false,
    }
}

/// Returns the name of the type in Spark SQL, as used in Spark error messages.
fn spark_sql_type_name(data_type: &DataType) -> String {
    use DataType::*;

    match data_type {
        Null => "VOID".to_string(),
        Boolean => "BOOLEAN".to_string(),
        Int8 => "TINYINT".to_string(),
        Int16 => "SMALLINT".to_string(),
        Int32 => "INT".to_string(),
        Int64 => "BIGINT".to_string(),
        Float32 => "FLOAT".to_string(),
        Float64 => "DOUBLE".to_string(),
        Decimal128(precision, scale) | Decimal256(precision, scale) => {
            format!("DECIMAL({precision},{scale})")
        }
        Utf8 | LargeUtf8 | Utf8View => "STRING".to_string(),
        Binary | LargeBinary | BinaryView | FixedSizeBinary(_) => "BINARY".to_string(),
        Date32 | Date64 => "DATE".to_string(),
        Timestamp(_, Some(_)) => "TIMESTAMP".to_string(),
        Timestamp(_, None) => "TIMESTAMP_NTZ".to_string(),
        List(field) | LargeList(field) | FixedSizeList(field, _) => {
            format!("ARRAY<{}>", spark_sql_type_name(field.data_type()))
        }
        Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| {
                    format!(
                        "{}: {}",
                        field.name(),
                        spark_sql_type_name(field.data_type())
                    )
                })
                .collect::<Vec<_>>();
            format!("STRUCT<{}>", fields.join(", "))
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};

    use super::{can_store_assign, spark_sql_type_name};

    #[test]
    fn test_atomic_assignments() {
        assert!(can_store_assign(&DataType::Int32, &DataType::Int64));
        assert!(can_store_assign(&DataType::Int64, &DataType::Int32));
        assert!(can_store_assign(&DataType::Float64, &DataType::Int64));
        assert!(can_store_assign(
            &DataType::Int64,
            &DataType::Decimal128(10, 2)
        ));
        assert!(can_store_assign(&DataType::Int32, &DataType::Utf8));
        assert!(can_store_assign(&DataType::Boolean, &DataType::Utf8));
        assert!(can_store_assign(&DataType::Date32, &DataType::Utf8));
        assert!(can_store_assign(
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
        assert!(!can_store_assign(&DataType::Utf8, &DataType::Int64));
        assert!(!can_store_assign(&DataType::Utf8, &DataType::Date32));
        assert!(!can_store_assign(&DataType::Boolean, &DataType::Int32));
        assert!(!can_store_assign(&DataType::Int32, &DataType::Boolean));
    }

    #[test]
    fn test_nested_assignments() {
        let list = |data_type: DataType, nullable: bool| {
            DataType::List(Arc::new(Field::new("item", data_type, nullable)))
        };
        assert!(can_store_assign(
            &list(DataType::Int32, false),
            &list(DataType::Int64, true)
        ));
        assert!(!can_store_assign(
            &list(DataType::Int32, true),
            &list(DataType::Int64, false)
        ));
        assert!(!can_store_assign(
            &list(DataType::Int32, true),
            &DataType::Utf8
        ));
        let record = |data_type: DataType| {
            DataType::Struct(Fields::from(vec![Field::new("a", data_type, true)]))
        };
        assert!(can_store_assign(
            &record(DataType::Int32),
            &record(DataType::Int64)
        ));
        assert!(!can_store_assign(
            &record(DataType::Utf8),
            &record(DataType::Int64)
        ));
    }

    #[test]
    fn test_spark_sql_type_names() {
        assert_eq!(spark_sql_type_name(&DataType::Int64), "BIGINT");
        assert_eq!(spark_sql_type_name(&DataType::Utf8), "STRING");
        assert_eq!(
            spark_sql_type_name(&DataType::Decimal128(10, 2)),
            "DECIMAL(10,2)"
        );
    }
}
