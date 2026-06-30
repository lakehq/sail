use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::MemTable;
use datafusion_common::{DFSchema, DFSchemaRef, ParamValues, ScalarValue};
use datafusion_expr::{EmptyRelation, Expr, Extension, LogicalPlan, UNNAMED_TABLE};
use log::warn;
use sail_common::spec;
use sail_common_datafusion::array::record_batch::{
    cast_record_batch_positionally, read_record_batches,
};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_logical_plan::range::RangeNode;
use sail_logical_plan::repartition::{ExplicitRepartitionKind, ExplicitRepartitionNode};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves a query plan that produces an empty relation.
    /// When `produce_one_row` is true, it can be used for literal projection with no input.
    pub(super) fn resolve_query_empty(&self, produce_one_row: bool) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    pub(super) async fn resolve_query_range(
        &self,
        range: spec::Range,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Range {
            start,
            end,
            step,
            num_partitions,
        } = range;
        let start = start.unwrap_or(0);
        // TODO: use parallelism in Spark configuration as the default
        let num_partitions = num_partitions.unwrap_or(1);
        if num_partitions < 1 {
            return Err(PlanError::invalid(format!(
                "invalid number of partitions: {num_partitions}"
            )));
        }
        let alias = state.register_field_name("id");
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(RangeNode::try_new(alias, start, end, step, num_partitions)?),
        }))
    }

    pub(super) async fn resolve_query_with_parameters(
        &self,
        input: spec::QueryPlan,
        positional: Vec<spec::Expr>,
        named: Vec<(String, spec::Expr)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let evaluator = LiteralEvaluator::new();
        let schema = Arc::new(DFSchema::empty());
        // Evaluate named arguments eagerly so that IDENTIFIER(:col) expressions
        // inside the query body can substitute their placeholder values at plan-resolution
        // time (before `with_param_values` is applied to the resolved plan).
        let named_params = {
            let mut params = HashMap::new();
            for (name, arg) in named {
                let expr = self.resolve_expression(arg, &schema, state).await?;
                let param = evaluator
                    .evaluate(&expr)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                params.insert(name, param);
            }
            params
        };
        // Evaluate positional arguments eagerly for the same reason.
        let positional_params = {
            let mut params = vec![];
            for arg in positional {
                let expr = self.resolve_expression(arg, &schema, state).await?;
                let param = evaluator
                    .evaluate(&expr)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                params.push(param);
            }
            params
        };
        // Enter a scope that makes both named and positional parameter values
        // available for IDENTIFIER clause evaluation inside the query body.
        let mut scope =
            state.enter_param_values_scope(named_params.clone(), positional_params.clone());
        let state = scope.state();
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let input = if !positional_params.is_empty() {
            input.with_param_values(ParamValues::from(positional_params))?
        } else {
            input
        };
        if !named_params.is_empty() {
            Ok(input.with_param_values(ParamValues::from(named_params))?)
        } else {
            Ok(input)
        }
    }

    pub(super) async fn resolve_query_local_relation(
        &self,
        data: Option<Vec<u8>>,
        schema: Option<spec::Schema>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let batches = if let Some(data) = data {
            read_record_batches(&data)?
        } else {
            vec![]
        };
        self.resolve_local_relation_batches(batches, schema, state)
    }

    pub(super) async fn resolve_query_cached_local_relation(
        &self,
        hash: String,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let relation = self
            .config
            .local_relation_cache
            .read_cached_local_relation(&hash)?;
        self.resolve_query_local_relation(relation.data, relation.schema, state)
            .await
    }

    pub(super) async fn resolve_query_chunked_cached_local_relation(
        &self,
        data_hashes: Vec<String>,
        schema_hash: Option<String>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        if data_hashes.is_empty() {
            return Err(PlanError::invalid(
                "chunked cached local relation must contain data",
            ));
        }
        let mut batches = vec![];
        for hash in data_hashes {
            let data = self
                .config
                .local_relation_cache
                .read_chunked_cached_local_relation_data(&hash)?;
            batches.extend(read_record_batches(&data)?);
        }
        let schema = schema_hash
            .map(|hash| {
                self.config
                    .local_relation_cache
                    .read_chunked_cached_local_relation_schema(&hash)
            })
            .transpose()?;
        self.resolve_local_relation_batches(batches, schema, state)
    }

    fn resolve_local_relation_batches(
        &self,
        batches: Vec<datafusion::arrow::array::RecordBatch>,
        schema: Option<spec::Schema>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let (schema, batches) = if let Some(schema) = schema {
            let schema = Arc::new(self.resolve_schema(schema, state)?);
            let batches = batches
                .into_iter()
                .map(|b| Ok(cast_record_batch_positionally(b, schema.clone())?))
                .collect::<PlanResult<_>>()?;
            (schema, batches)
        } else if let [batch, ..] = batches.as_slice() {
            (batch.schema(), batches)
        } else {
            return Err(PlanError::invalid("missing schema for local relation"));
        };
        let table_provider = Arc::new(MemTable::try_new(schema, vec![batches])?);
        self.resolve_table_provider_with_rename(
            table_provider,
            UNNAMED_TABLE,
            None,
            vec![],
            None,
            state,
        )
    }

    pub(super) async fn resolve_query_hint(
        &self,
        input: spec::QueryPlan,
        name: String,
        parameters: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;

        if name.eq_ignore_ascii_case("COALESCE") {
            let num_partitions = self
                .resolve_hint_partition_count(&name, &parameters, input.schema(), state)
                .await?;
            return Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ExplicitRepartitionNode::new(
                    Arc::new(input),
                    Some(num_partitions),
                    ExplicitRepartitionKind::Coalesce,
                    vec![],
                )),
            }));
        }

        warn!("Hint operation is not yet supported and is a no-op");
        Ok(input)
    }

    async fn resolve_hint_partition_count(
        &self,
        hint_name: &str,
        parameters: &[spec::Expr],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<usize> {
        let hint_name = hint_name.to_uppercase();
        let [parameter] = parameters else {
            return Err(PlanError::invalid(format!(
                "{hint_name} hint requires exactly one partition count"
            )));
        };

        let expr = self
            .resolve_expression(parameter.clone(), schema, state)
            .await?;
        let value = literal_partition_count(&hint_name, &expr)?;
        if value < 1 {
            return Err(PlanError::invalid(format!(
                "{hint_name} hint requires at least one partition"
            )));
        }
        Ok(value)
    }

    pub(super) async fn resolve_query_collect_metrics(
        &self,
        _input: spec::QueryPlan,
        _name: String,
        _metrics: Vec<spec::Expr>,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("collect metrics"))
    }

    pub(super) async fn resolve_query_parse(
        &self,
        _parse: spec::Parse,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("parse"))
    }

    pub(super) async fn resolve_query_with_watermark(
        &self,
        _watermark: spec::WithWatermark,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("with watermark"))
    }
}

fn literal_partition_count(hint_name: &str, expr: &Expr) -> PlanResult<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), _metadata) => {
            usize::try_from(i64::from(*value)).map_err(|_| {
                PlanError::invalid(format!("{hint_name} hint requires at least one partition"))
            })
        }
        Expr::Literal(ScalarValue::Int16(Some(value)), _metadata) => {
            usize::try_from(i64::from(*value)).map_err(|_| {
                PlanError::invalid(format!("{hint_name} hint requires at least one partition"))
            })
        }
        Expr::Literal(ScalarValue::Int32(Some(value)), _metadata) => {
            usize::try_from(i64::from(*value)).map_err(|_| {
                PlanError::invalid(format!("{hint_name} hint requires at least one partition"))
            })
        }
        Expr::Literal(ScalarValue::Int64(Some(value)), _metadata) => usize::try_from(*value)
            .map_err(|_| {
                PlanError::invalid(format!("{hint_name} hint requires at least one partition"))
            }),
        Expr::Literal(ScalarValue::UInt8(Some(value)), _metadata) => Ok(*value as usize),
        Expr::Literal(ScalarValue::UInt16(Some(value)), _metadata) => Ok(*value as usize),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _metadata) => Ok(*value as usize),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _metadata) => usize::try_from(*value)
            .map_err(|_| {
                PlanError::invalid(format!("{hint_name} hint partition count is too large"))
            }),
        _ => Err(PlanError::invalid(format!(
            "{hint_name} hint partition count must be an integer"
        ))),
    }
}
