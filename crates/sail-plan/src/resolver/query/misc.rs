use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::MemTable;
use datafusion_common::{DFSchema, DFSchemaRef, ParamValues};
use datafusion_expr::{EmptyRelation, Extension, LogicalPlan, UNNAMED_TABLE};
use log::warn;
use sail_common::spec;
use sail_common_datafusion::array::record_batch::{cast_record_batch, read_record_batches};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_logical_plan::range::RangeNode;

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
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let input = if !positional.is_empty() {
            let params = {
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
            input.with_param_values(ParamValues::from(params))?
        } else {
            input
        };
        if !named.is_empty() {
            let params = {
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
            Ok(input.with_param_values(ParamValues::from(params))?)
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
        let (schema, batches) = if let Some(schema) = schema {
            let schema = Arc::new(self.resolve_schema(schema, state)?);
            let batches = batches
                .into_iter()
                .map(|b| Ok(cast_record_batch(b, schema.clone())?))
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
        _name: String,
        _parameters: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // TODO: Implement
        warn!("Hint operation is not yet supported and is a no-op");
        self.resolve_query_plan_with_hidden_fields(input, state)
            .await
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
