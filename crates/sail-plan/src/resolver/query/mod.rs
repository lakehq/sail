use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion_common::DFSchema;
use datafusion_expr::{
    Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, ScalarUDF, cast,
};
use sail_cache::remote_checkpoint::RemoteCheckpointRegistry;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_function::scalar::datetime::spark_timezone_cast::SparkTimezoneCast;
use sail_logical_plan::remote_checkpoint::RemoteCheckpointRelationNode;

use crate::config::{PlanConfig, StoreAssignmentPolicy};
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::command::store_assignment_compatible;
use crate::resolver::state::PlanResolverState;

mod aggregate;
mod alias;
mod column_op;
mod cte;
mod dedup;
mod filter;
mod join;
mod lateral;
mod lateral_join;
mod limit;
mod misc;
mod na;
mod pivoting;
mod project;
mod read;
mod recursion;
mod repartition;
mod sample;
mod set_op;
mod sort;
mod stat;
mod time_travel;
mod udf;
mod udtf;
mod values;
mod window;
mod with_relations;

fn contains_ltz(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(_, Some(_)) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_ltz(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_ltz(field.data_type())),
        _ => false,
    }
}

fn canonicalize_ltz_type(data_type: &DataType) -> DataType {
    let field = |field: &Arc<datafusion::arrow::datatypes::Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(canonicalize_ltz_type(field.data_type())),
        )
    };
    match data_type {
        DataType::Timestamp(_, Some(_)) => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        }
        DataType::List(value) => DataType::List(field(value)),
        DataType::LargeList(value) => DataType::LargeList(field(value)),
        DataType::ListView(value) => DataType::ListView(field(value)),
        DataType::LargeListView(value) => DataType::LargeListView(field(value)),
        DataType::FixedSizeList(value, size) => DataType::FixedSizeList(field(value), *size),
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(field).collect::<Vec<_>>().into())
        }
        DataType::Map(entries, sorted) => DataType::Map(field(entries), *sorted),
        _ => data_type.clone(),
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_temporal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

fn spark_force_nullable(source_type: &DataType, target_type: &DataType) -> bool {
    if source_type == target_type
        || matches!(source_type, DataType::Null)
        || matches!(
            (source_type, target_type),
            (
                DataType::Timestamp(_, Some(_)),
                DataType::Timestamp(_, Some(_))
            ) | (DataType::Timestamp(_, None), DataType::Timestamp(_, None))
        )
    {
        return false;
    }
    match (source_type, target_type) {
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) => false,
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, _) => true,
        (_, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => false,
        (DataType::Timestamp(_, Some(_)), DataType::Date32 | DataType::Date64) => false,
        (_, DataType::Date32 | DataType::Date64) => true,
        (DataType::Date32 | DataType::Date64, DataType::Timestamp(_, Some(_))) => false,
        (DataType::Date32 | DataType::Date64, _) => true,
        _ => false,
    }
}

/// Finds the Spark common type for the subset of coercions involving LTZ.
/// The boolean records LTZ involvement even when legacy coercion widens it to string.
fn widen_ltz_types(
    left: &DataType,
    right: &DataType,
    ansi_mode: bool,
    promote_strings: bool,
    case_sensitive: bool,
) -> Option<(DataType, bool)> {
    if left == right {
        return Some((canonicalize_ltz_type(left), contains_ltz(left)));
    }
    if left.is_null() {
        return Some((canonicalize_ltz_type(right), contains_ltz(right)));
    }
    if right.is_null() {
        return Some((canonicalize_ltz_type(left), contains_ltz(left)));
    }

    if is_string(left) && is_string(right) {
        return Some((DataType::Utf8, false));
    }
    if promote_strings
        && ((is_string(left) && is_temporal(right)) || (is_temporal(left) && is_string(right)))
    {
        let (string, temporal) = if is_string(left) {
            (left, right)
        } else {
            (right, left)
        };
        return if ansi_mode {
            Some((canonicalize_ltz_type(temporal), contains_ltz(temporal)))
        } else {
            Some((string.clone(), contains_ltz(temporal)))
        };
    }
    if is_temporal(left) && is_temporal(right) {
        let has_ltz = contains_ltz(left) || contains_ltz(right);
        let data_type = if has_ltz {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        } else if matches!(left, DataType::Timestamp(_, None))
            || matches!(right, DataType::Timestamp(_, None))
        {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        } else {
            DataType::Date32
        };
        return Some((data_type, has_ltz));
    }

    let merge_field = |left: &Arc<datafusion::arrow::datatypes::Field>,
                       right: &Arc<datafusion::arrow::datatypes::Field>| {
        let (data_type, has_ltz) = widen_ltz_types(
            left.data_type(),
            right.data_type(),
            ansi_mode,
            promote_strings,
            case_sensitive,
        )?;
        let nullable = left.is_nullable()
            || right.is_nullable()
            || spark_force_nullable(left.data_type(), &data_type)
            || spark_force_nullable(right.data_type(), &data_type);
        Some((
            Arc::new(
                left.as_ref()
                    .clone()
                    .with_data_type(data_type)
                    .with_nullable(nullable),
            ),
            has_ltz,
        ))
    };

    match (left, right) {
        (DataType::List(left), DataType::List(right)) => {
            let (field, has_ltz) = merge_field(left, right)?;
            Some((DataType::List(field), has_ltz))
        }
        (DataType::LargeList(left), DataType::LargeList(right)) => {
            let (field, has_ltz) = merge_field(left, right)?;
            Some((DataType::LargeList(field), has_ltz))
        }
        (DataType::ListView(left), DataType::ListView(right)) => {
            let (field, has_ltz) = merge_field(left, right)?;
            Some((DataType::ListView(field), has_ltz))
        }
        (DataType::LargeListView(left), DataType::LargeListView(right)) => {
            let (field, has_ltz) = merge_field(left, right)?;
            Some((DataType::LargeListView(field), has_ltz))
        }
        (DataType::FixedSizeList(left, left_size), DataType::FixedSizeList(right, right_size))
            if left_size == right_size =>
        {
            let (field, has_ltz) = merge_field(left, right)?;
            Some((DataType::FixedSizeList(field, *left_size), has_ltz))
        }
        (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len() => {
            let mut has_ltz = false;
            let fields = left
                .iter()
                .zip(right)
                .map(|(left, right)| {
                    let names_match = if case_sensitive {
                        left.name() == right.name()
                    } else {
                        left.name().eq_ignore_ascii_case(right.name())
                    };
                    if !names_match {
                        return None;
                    }
                    let (field, field_has_ltz) = merge_field(left, right)?;
                    has_ltz |= field_has_ltz;
                    Some(field)
                })
                .collect::<Option<Vec<_>>>()?;
            Some((DataType::Struct(fields.into()), has_ltz))
        }
        (DataType::Map(left, left_sorted), DataType::Map(right, right_sorted))
            if left_sorted == right_sorted =>
        {
            let (DataType::Struct(left_fields), DataType::Struct(right_fields)) =
                (left.data_type(), right.data_type())
            else {
                return None;
            };
            let ([left_key, left_value], [right_key, right_value]) =
                (left_fields.as_ref(), right_fields.as_ref())
            else {
                return None;
            };
            let (key_type, key_has_ltz) = widen_ltz_types(
                left_key.data_type(),
                right_key.data_type(),
                ansi_mode,
                promote_strings,
                case_sensitive,
            )?;
            if spark_force_nullable(left_key.data_type(), &key_type)
                || spark_force_nullable(right_key.data_type(), &key_type)
            {
                return None;
            }
            let (value_type, value_has_ltz) = widen_ltz_types(
                left_value.data_type(),
                right_value.data_type(),
                ansi_mode,
                promote_strings,
                case_sensitive,
            )?;
            let value_nullable = left_value.is_nullable()
                || right_value.is_nullable()
                || spark_force_nullable(left_value.data_type(), &value_type)
                || spark_force_nullable(right_value.data_type(), &value_type);
            let fields = vec![
                Arc::new(left_key.as_ref().clone().with_data_type(key_type)),
                Arc::new(
                    left_value
                        .as_ref()
                        .clone()
                        .with_data_type(value_type)
                        .with_nullable(value_nullable),
                ),
            ];
            let field = Arc::new(
                left.as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(fields.into()))
                    .with_nullable(left.is_nullable() || right.is_nullable()),
            );
            Some((
                DataType::Map(field, *left_sorted),
                key_has_ltz || value_has_ltz,
            ))
        }
        _ => None,
    }
}

fn common_ltz_type(
    left: &DataType,
    right: &DataType,
    ansi_mode: bool,
    promote_strings: bool,
    case_sensitive: bool,
) -> Option<DataType> {
    let (data_type, has_ltz) =
        widen_ltz_types(left, right, ansi_mode, promote_strings, case_sensitive)?;
    has_ltz.then_some(data_type)
}

pub(crate) fn align_expr_to_ltz_type(
    expression: Expr,
    source_type: &DataType,
    target_type: &DataType,
    config: &PlanConfig,
    safe: bool,
) -> PlanResult<Expr> {
    if source_type == target_type {
        return Ok(expression);
    }
    if source_type.is_null() {
        return Ok(cast(expression, target_type.clone()));
    }
    Ok(ScalarUDF::from(SparkTimezoneCast::new(
        target_type.clone(),
        Arc::clone(&config.session_timezone),
        safe,
    ))
    .call(vec![expression]))
}

fn assignment_needs_timezone_cast(
    source_type: &DataType,
    target_type: &DataType,
    case_sensitive: bool,
    struct_by_name: bool,
) -> Option<bool> {
    match (source_type, target_type) {
        (DataType::List(source), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(target))
        | (DataType::ListView(source), DataType::ListView(target))
        | (DataType::LargeListView(source), DataType::LargeListView(target)) => {
            assignment_needs_timezone_cast(
                source.data_type(),
                target.data_type(),
                case_sensitive,
                struct_by_name,
            )
        }
        (
            DataType::FixedSizeList(source, source_size),
            DataType::FixedSizeList(target, target_size),
        ) if source_size == target_size => assignment_needs_timezone_cast(
            source.data_type(),
            target.data_type(),
            case_sensitive,
            struct_by_name,
        ),
        (DataType::Map(source, source_sorted), DataType::Map(target, target_sorted))
            if source_sorted == target_sorted =>
        {
            let (DataType::Struct(source_fields), DataType::Struct(target_fields)) =
                (source.data_type(), target.data_type())
            else {
                return None;
            };
            let ([source_key, source_value], [target_key, target_value]) =
                (source_fields.as_ref(), target_fields.as_ref())
            else {
                return None;
            };
            let key_needs = assignment_needs_timezone_cast(
                source_key.data_type(),
                target_key.data_type(),
                case_sensitive,
                struct_by_name,
            )?;
            let value_needs = assignment_needs_timezone_cast(
                source_value.data_type(),
                target_value.data_type(),
                case_sensitive,
                struct_by_name,
            )?;
            Some(key_needs || value_needs)
        }
        (DataType::Struct(source), DataType::Struct(target)) if source.len() == target.len() => {
            if !struct_by_name {
                return source
                    .iter()
                    .zip(target)
                    .try_fold(false, |needs, (source, target)| {
                        Some(
                            needs
                                || assignment_needs_timezone_cast(
                                    source.data_type(),
                                    target.data_type(),
                                    case_sensitive,
                                    false,
                                )?,
                        )
                    });
            }
            let mut matched = vec![false; source.len()];
            let mut needs_timezone_cast = false;
            for target_field in target {
                let mut matches = source.iter().enumerate().filter(|(_, source_field)| {
                    if case_sensitive {
                        source_field.name() == target_field.name()
                    } else {
                        source_field
                            .name()
                            .eq_ignore_ascii_case(target_field.name())
                    }
                });
                let (source_index, source_field) = matches.next()?;
                if matches.next().is_some() || matched[source_index] {
                    return None;
                }
                matched[source_index] = true;
                needs_timezone_cast |= assignment_needs_timezone_cast(
                    source_field.data_type(),
                    target_field.data_type(),
                    case_sensitive,
                    true,
                )?;
            }
            Some(needs_timezone_cast)
        }
        _ => Some(crate::resolver::expression::needs_spark_timezone_cast(
            source_type,
            target_type,
        )),
    }
}

fn assignment_source_type_in_target_order(
    source_type: &DataType,
    target_type: &DataType,
    case_sensitive: bool,
    struct_by_name: bool,
) -> Option<DataType> {
    let align_field = |source: &Arc<datafusion::arrow::datatypes::Field>,
                       target: &Arc<datafusion::arrow::datatypes::Field>,
                       struct_by_name| {
        Some(Arc::new(
            source
                .as_ref()
                .clone()
                .with_name(target.name())
                .with_data_type(assignment_source_type_in_target_order(
                    source.data_type(),
                    target.data_type(),
                    case_sensitive,
                    struct_by_name,
                )?),
        ))
    };

    Some(match (source_type, target_type) {
        (DataType::List(source), DataType::List(target)) => {
            DataType::List(align_field(source, target, struct_by_name)?)
        }
        (DataType::LargeList(source), DataType::LargeList(target)) => {
            DataType::LargeList(align_field(source, target, struct_by_name)?)
        }
        (DataType::ListView(source), DataType::ListView(target)) => {
            DataType::ListView(align_field(source, target, struct_by_name)?)
        }
        (DataType::LargeListView(source), DataType::LargeListView(target)) => {
            DataType::LargeListView(align_field(source, target, struct_by_name)?)
        }
        (
            DataType::FixedSizeList(source, source_size),
            DataType::FixedSizeList(target, target_size),
        ) if source_size == target_size => {
            DataType::FixedSizeList(align_field(source, target, struct_by_name)?, *source_size)
        }
        (DataType::Map(source, source_sorted), DataType::Map(target, _)) => {
            let (DataType::Struct(source_fields), DataType::Struct(target_fields)) =
                (source.data_type(), target.data_type())
            else {
                return None;
            };
            let ([source_key, source_value], [target_key, target_value]) =
                (source_fields.as_ref(), target_fields.as_ref())
            else {
                return None;
            };
            let fields = vec![
                align_field(source_key, target_key, struct_by_name)?,
                align_field(source_value, target_value, struct_by_name)?,
            ];
            DataType::Map(
                Arc::new(
                    source
                        .as_ref()
                        .clone()
                        .with_name(target.name())
                        .with_data_type(DataType::Struct(fields.into())),
                ),
                *source_sorted,
            )
        }
        (DataType::Struct(source), DataType::Struct(target)) if source.len() == target.len() => {
            let mut matched = vec![false; source.len()];
            let mut fields = Vec::with_capacity(target.len());
            for (target_index, target_field) in target.iter().enumerate() {
                let (source_index, source_field) = if struct_by_name {
                    let mut matches = source.iter().enumerate().filter(|(_, source_field)| {
                        if case_sensitive {
                            source_field.name() == target_field.name()
                        } else {
                            source_field
                                .name()
                                .eq_ignore_ascii_case(target_field.name())
                        }
                    });
                    let matched = matches.next()?;
                    if matches.next().is_some() {
                        return None;
                    }
                    matched
                } else {
                    (target_index, source.get(target_index)?)
                };
                if matched[source_index] {
                    return None;
                }
                matched[source_index] = true;
                fields.push(align_field(source_field, target_field, struct_by_name)?);
            }
            DataType::Struct(fields.into())
        }
        _ => source_type.clone(),
    })
}

impl PlanResolver<'_> {
    fn cast_assignment_with_safety(
        &self,
        expression: Expr,
        target_type: &DataType,
        schema: &DFSchema,
        safe: bool,
        struct_by_name: bool,
    ) -> PlanResult<Expr> {
        let source_type = expression.get_type(schema)?;
        if assignment_needs_timezone_cast(
            &source_type,
            target_type,
            self.config.case_sensitive,
            struct_by_name,
        ) != Some(true)
        {
            return Ok(expression.cast_to(target_type, schema)?);
        }
        let cast = if struct_by_name {
            SparkTimezoneCast::new_by_name(
                target_type.clone(),
                Arc::clone(&self.config.session_timezone),
                safe,
                self.config.case_sensitive,
            )
        } else {
            SparkTimezoneCast::new(
                target_type.clone(),
                Arc::clone(&self.config.session_timezone),
                safe,
            )
        };
        Ok(ScalarUDF::from(cast).call(vec![expression]))
    }

    pub(crate) fn cast_assignment(
        &self,
        expression: Expr,
        target_type: &DataType,
        schema: &DFSchema,
    ) -> PlanResult<Expr> {
        self.cast_assignment_with_safety(expression, target_type, schema, false, true)
    }

    pub(crate) fn cast_store_assignment(
        &self,
        expression: Expr,
        target_type: &DataType,
        schema: &DFSchema,
        struct_by_name: bool,
    ) -> PlanResult<Expr> {
        let source_type = expression.get_type(schema)?;
        let needs_timezone_cast = assignment_needs_timezone_cast(
            &source_type,
            target_type,
            self.config.case_sensitive,
            struct_by_name,
        ) == Some(true);
        if needs_timezone_cast
            && self.config.store_assignment_policy != StoreAssignmentPolicy::Legacy
        {
            let aligned_source = assignment_source_type_in_target_order(
                &source_type,
                target_type,
                self.config.case_sensitive,
                struct_by_name,
            );
            if aligned_source.as_ref().is_none_or(|source_type| {
                !store_assignment_compatible(
                    source_type,
                    target_type,
                    self.config.store_assignment_policy,
                    self.config.case_sensitive,
                )
            }) {
                return Err(PlanError::AnalysisError(format!(
                    "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot safely cast \"{source_type}\" to \"{target_type}\" for table assignment."
                )));
            }
        }
        self.cast_assignment_with_safety(
            expression,
            target_type,
            schema,
            self.config.store_assignment_policy == StoreAssignmentPolicy::Legacy,
            struct_by_name,
        )
    }

    /// Resolve query plan.
    /// No hidden fields are kept in the resolved plan.
    #[async_recursion]
    pub(super) async fn resolve_query_plan(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let plan = self
            .resolve_query_plan_with_hidden_fields(plan, state)
            .await?;
        self.remove_hidden_fields(plan, state)
    }

    /// Resolve query plan.
    /// The resolved plan may contain hidden fields.
    /// If the hidden fields cannot be handled,
    /// [`Self::resolve_query_plan`] should be used instead,
    #[async_recursion]
    async fn resolve_query_plan_with_hidden_fields(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::QueryNode;

        let plan_id = plan.plan_id;
        let plan = match plan.node {
            QueryNode::Read {
                read_type,
                is_streaming: _,
            } => match read_type {
                spec::ReadType::NamedTable(table) => {
                    self.resolve_query_read_named_table(*table, state).await?
                }
                spec::ReadType::Udtf(udtf) => self.resolve_query_read_udtf(*udtf, state).await?,
                spec::ReadType::DataSource(source) => {
                    self.resolve_query_read_data_source(*source, state).await?
                }
                spec::ReadType::DynamicTable(table) => {
                    self.resolve_query_read_dynamic_table(*table, state).await?
                }
            },
            QueryNode::Project { input, expressions } => {
                self.resolve_query_project(input.map(|x| *x), expressions, state)
                    .await?
            }
            QueryNode::Filter { input, condition } => {
                self.resolve_query_filter(*input, condition, state).await?
            }
            QueryNode::Join(join) => self.resolve_query_join(join, state).await?,
            QueryNode::SetOperation(op) => self.resolve_query_set_operation(op, state).await?,
            QueryNode::Sort {
                input,
                order,
                is_global,
            } => {
                self.resolve_query_sort(*input, order, is_global, state)
                    .await?
            }
            QueryNode::Limit { input, skip, limit } => {
                self.resolve_query_limit(*input, skip, limit, state).await?
            }
            QueryNode::Aggregate(aggregate) => {
                self.resolve_query_aggregate(aggregate, state).await?
            }
            QueryNode::WithParameters {
                input,
                positional_arguments,
                named_arguments,
            } => {
                self.resolve_query_with_parameters(
                    *input,
                    positional_arguments,
                    named_arguments,
                    state,
                )
                .await?
            }
            QueryNode::LocalRelation { data, schema } => {
                self.resolve_query_local_relation(data, schema, state)
                    .await?
            }
            QueryNode::Sample(sample) => self.resolve_query_sample(sample, state).await?,
            QueryNode::TableSample { input, sample } => {
                let plan = self.resolve_query_plan(*input, state).await?;
                self.apply_table_sample(plan, sample, state).await?
            }
            QueryNode::Deduplicate(deduplicate) => {
                self.resolve_query_deduplicate(deduplicate, state).await?
            }
            QueryNode::Range(range) => self.resolve_query_range(range, state).await?,
            QueryNode::SubqueryAlias {
                input,
                alias,
                qualifier,
            } => {
                self.resolve_query_subquery_alias(*input, alias, qualifier, state)
                    .await?
            }
            QueryNode::Repartition {
                input,
                num_partitions,
                shuffle,
            } => {
                self.resolve_query_repartition(*input, num_partitions, shuffle, state)
                    .await?
            }
            QueryNode::ToDf {
                input,
                column_names,
            } => {
                self.resolve_query_to_df(*input, column_names, state)
                    .await?
            }
            QueryNode::WithColumnsRenamed {
                input,
                rename_columns_map,
            } => {
                self.resolve_query_with_columns_renamed(*input, rename_columns_map, state)
                    .await?
            }
            QueryNode::Drop {
                input,
                columns,
                column_names,
            } => {
                self.resolve_query_drop(*input, columns, column_names, state)
                    .await?
            }
            QueryNode::Tail { input, limit } => {
                self.resolve_query_tail(*input, limit, state).await?
            }
            QueryNode::WithColumns { input, aliases } => {
                self.resolve_query_with_columns(*input, aliases, state)
                    .await?
            }
            QueryNode::Hint {
                input,
                name,
                parameters,
            } => {
                self.resolve_query_hint(*input, name, parameters, state)
                    .await?
            }
            QueryNode::Pivot(pivot) => self.resolve_query_pivot(pivot, state).await?,
            QueryNode::Unpivot(unpivot) => self.resolve_query_unpivot(unpivot, state).await?,
            QueryNode::ToSchema { input, schema } => {
                self.resolve_query_to_schema(*input, schema, state).await?
            }
            QueryNode::RepartitionByExpression {
                input,
                partition_expressions,
                num_partitions,
            } => {
                self.resolve_query_repartition_by_expression(
                    *input,
                    partition_expressions,
                    num_partitions,
                    state,
                )
                .await?
            }
            QueryNode::MapPartitions {
                input,
                function,
                is_barrier,
            } => {
                self.resolve_query_map_partitions(*input, function, is_barrier, state)
                    .await?
            }
            QueryNode::CollectMetrics {
                input,
                name,
                metrics,
            } => {
                self.resolve_query_collect_metrics(*input, name, metrics, state)
                    .await?
            }
            QueryNode::Parse(parse) => self.resolve_query_parse(parse, state).await?,
            QueryNode::GroupMap(map) => self.resolve_query_group_map(map, state).await?,
            QueryNode::CoGroupMap(map) => self.resolve_query_co_group_map(map, state).await?,
            QueryNode::WithWatermark(watermark) => {
                self.resolve_query_with_watermark(watermark, state).await?
            }
            QueryNode::ApplyInPandasWithState(apply) => {
                self.resolve_query_apply_in_pandas_with_state(apply, state)
                    .await?
            }
            QueryNode::CachedLocalRelation { .. } => {
                return Err(PlanError::todo("cached local relation"));
            }
            QueryNode::CachedRemoteRelation { relation_id } => {
                let registry = self.ctx.extension::<RemoteCheckpointRegistry>()?;
                let descriptor = registry.get(&relation_id)?.ok_or_else(|| {
                    PlanError::analysis(format!(
                        "remote checkpoint relation does not exist: {relation_id}"
                    ))
                })?;
                let names = state.register_fields(descriptor.logical_schema.fields());
                let fields = descriptor
                    .logical_schema
                    .fields()
                    .iter()
                    .zip(names)
                    .map(|(field, name)| Arc::new(field.as_ref().clone().with_name(name)))
                    .collect::<Vec<_>>();
                let schema = Arc::new(Schema::new_with_metadata(
                    fields,
                    descriptor.logical_schema.metadata().clone(),
                ));
                LogicalPlan::Extension(Extension {
                    node: Arc::new(RemoteCheckpointRelationNode::try_new(relation_id, schema)?),
                })
            }
            QueryNode::CommonInlineUserDefinedTableFunction(udtf) => {
                self.resolve_query_common_inline_udtf(udtf, state).await?
            }
            QueryNode::FillNa {
                input,
                columns,
                values,
            } => {
                self.resolve_query_fill_na(*input, columns, values, state)
                    .await?
            }
            QueryNode::DropNa {
                input,
                columns,
                min_non_nulls,
            } => {
                self.resolve_query_drop_na(*input, columns, min_non_nulls, state)
                    .await?
            }
            QueryNode::Replace {
                input,
                columns,
                replacements,
            } => {
                self.resolve_query_replace(*input, columns, replacements, state)
                    .await?
            }
            QueryNode::StatSummary { input, statistics } => {
                self.resolve_query_stat_summary(*input, vec![], statistics, state)
                    .await?
            }
            QueryNode::StatCrosstab {
                input,
                left_column,
                right_column,
            } => {
                self.resolve_query_stat_cross_tab(*input, left_column, right_column, state)
                    .await?
            }
            QueryNode::StatDescribe { input, columns } => {
                self.resolve_query_stat_describe(*input, columns, state)
                    .await?
            }
            QueryNode::StatCov {
                input,
                left_column,
                right_column,
            } => {
                self.resolve_query_stat_cov(*input, left_column, right_column, state)
                    .await?
            }
            QueryNode::StatCorr {
                input,
                left_column,
                right_column,
                method,
            } => {
                self.resolve_query_stat_corr(*input, left_column, right_column, method, state)
                    .await?
            }
            QueryNode::StatApproxQuantile { .. } => {
                return Err(PlanError::todo("approx quantile"));
            }
            QueryNode::StatFreqItems { .. } => {
                return Err(PlanError::todo("freq items"));
            }
            QueryNode::StatSampleBy {
                input,
                column,
                fractions,
                seed,
            } => {
                self.resolve_query_stat_sample_by(*input, column, fractions, seed, state)
                    .await?
            }
            QueryNode::Empty { produce_one_row } => self.resolve_query_empty(produce_one_row)?,
            QueryNode::Values(values) => self.resolve_query_values(values, state).await?,
            QueryNode::TableAlias {
                input,
                name,
                columns,
            } => {
                self.resolve_query_table_alias(*input, name, columns, state)
                    .await?
            }
            QueryNode::WithCtes {
                input,
                recursive,
                ctes,
            } => {
                self.resolve_query_with_ctes(*input, recursive, ctes, state)
                    .await?
            }
            QueryNode::WithRelations { root, references } => {
                self.resolve_query_with_relations(*root, references, state)
                    .await?
            }
            QueryNode::LateralView {
                input,
                function,
                arguments,
                named_arguments,
                table_alias,
                column_aliases,
                outer,
            } => {
                self.resolve_query_lateral_view(
                    input.map(|x| *x),
                    function,
                    arguments,
                    named_arguments,
                    table_alias,
                    column_aliases,
                    outer,
                    state,
                )
                .await?
            }
            QueryNode::LateralJoin {
                left,
                right,
                join_condition,
                join_type,
            } => {
                self.resolve_query_lateral_join(*left, *right, join_condition, join_type, state)
                    .await?
            }
            QueryNode::NamedWindows { input, windows } => {
                self.resolve_named_windows(*input, windows, state).await?
            }
        };
        self.verify_query_plan(&plan, state)?;
        self.register_schema_with_plan_id(&plan, plan_id, state)?;
        Ok(plan)
    }

    fn remove_hidden_fields(
        &self,
        plan: LogicalPlan,
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut columns = vec![];
        let mut has_hidden_columns = false;
        for column in plan.schema().columns() {
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() {
                has_hidden_columns = true;
            } else {
                columns.push(column);
            }
        }
        if has_hidden_columns {
            let plan = LogicalPlanBuilder::new(plan)
                .project(columns.into_iter().map(Expr::Column))?
                .build()?;
            Ok(plan)
        } else {
            Ok(plan)
        }
    }

    /// All resolved plans must have "resolved columns".
    /// If you define new fields in the plan, register the field in the state and use the "resolved field name" to alias the newly created field.
    /// If you fetch an existing field in the plan, you likely have the "unresolved" field name from the spec.
    /// Convert the unresolved field name to the "resolved field name" using the state.
    fn verify_query_plan(&self, plan: &LogicalPlan, state: &PlanResolverState) -> PlanResult<()> {
        let invalid = plan
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if state.get_field_info(f.name()).is_ok() {
                    None
                } else {
                    Some(f.name().to_string())
                }
            })
            .collect::<Vec<_>>();
        if invalid.is_empty() {
            Ok(())
        } else {
            Err(PlanError::internal(format!(
                "a plan resolver bug has produced invalid fields: {invalid:?}",
            )))
        }
    }

    fn register_schema_with_plan_id(
        &self,
        plan: &LogicalPlan,
        plan_id: Option<i64>,
        state: &mut PlanResolverState,
    ) -> PlanResult<()> {
        if let Some(plan_id) = plan_id {
            for field in plan.schema().fields() {
                state.register_plan_id_for_field(field.name(), plan_id)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn widens_nested_ltz_types_using_spark_ansi_rules() {
        let string = DataType::Utf8;
        let ltz = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
        assert_eq!(
            common_ltz_type(&string, &ltz, false, true, false),
            Some(string.clone())
        );
        assert_eq!(
            common_ltz_type(&string, &ltz, true, true, false),
            Some(ltz.clone())
        );
        assert_eq!(common_ltz_type(&string, &ltz, true, false, false), None);

        let list = |data_type| DataType::List(Arc::new(Field::new("element", data_type, true)));
        assert_eq!(
            common_ltz_type(
                &list(string.clone()),
                &list(ltz.clone()),
                false,
                true,
                false,
            ),
            Some(list(string))
        );
        assert_eq!(
            common_ltz_type(&list(DataType::Utf8), &list(ltz.clone()), true, true, false,),
            Some(list(ltz))
        );

        let struct_type = |name: &str, data_type| {
            DataType::Struct(vec![Arc::new(Field::new(name, data_type, true))].into())
        };
        let upper = struct_type("A", DataType::Timestamp(TimeUnit::Microsecond, None));
        let lower = struct_type(
            "a",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        );
        assert_eq!(
            common_ltz_type(&upper, &lower, true, true, false),
            Some(struct_type(
                "A",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            ))
        );
        assert_eq!(common_ltz_type(&upper, &lower, true, true, true), None);

        let map = |entries: &str, key: &str, value: &str, key_type, value_type| {
            DataType::Map(
                Arc::new(Field::new(
                    entries,
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new(key, key_type, false)),
                            Arc::new(Field::new(value, value_type, false)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        let string_key = map(
            "entries_left",
            "keys",
            "values",
            DataType::Utf8,
            DataType::Int32,
        );
        let timestamp_key = map(
            "entries_right",
            "key",
            "value",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            DataType::Int32,
        );
        assert_eq!(
            common_ltz_type(&string_key, &timestamp_key, true, true, false),
            None
        );

        let ntz_value = map(
            "entries_left",
            "keys",
            "values",
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let ltz_value = map(
            "entries_right",
            "key",
            "value",
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        );
        assert_eq!(
            common_ltz_type(&ntz_value, &ltz_value, true, true, false),
            Some(map(
                "entries_left",
                "keys",
                "values",
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            ))
        );
    }
}
