use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion_common::{DFSchema, DFSchemaRef, ParamValues};
use datafusion_expr::{EmptyRelation, Extension, LogicalPlan, UNNAMED_TABLE};
use log::warn;
use sail_common::spec;
use sail_common_datafusion::array::record_batch::{
    cast_record_batch_positionally, read_record_batches,
};
use sail_common_datafusion::literal::{LiteralEvaluator, LiteralValue};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_logical_plan::range::RangeNode;
use serde_json::Value;

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
        let (named_params, named_param_display_names) = {
            let mut params = HashMap::new();
            let mut display_names = HashMap::new();
            for (name, arg) in named {
                let named_expr = self.resolve_named_expression(arg, &schema, state).await?;
                let display_name = named_expr.name.clone().one()?;
                let param = evaluator
                    .evaluate(&named_expr.expr)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                display_names.insert(name.clone(), display_name);
                params.insert(name, param);
            }
            (params, display_names)
        };
        // Evaluate positional arguments eagerly for the same reason.
        let (positional_params, positional_param_display_names) = {
            let mut params = vec![];
            let mut display_names = vec![];
            for arg in positional {
                let named_expr = self.resolve_named_expression(arg, &schema, state).await?;
                display_names.push(named_expr.name.clone().one()?);
                let param = evaluator
                    .evaluate(&named_expr.expr)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                params.push(param);
            }
            (params, display_names)
        };
        // Enter a scope that makes both named and positional parameter values
        // available for IDENTIFIER clause evaluation inside the query body.
        let positional_param_indices = Self::collect_positional_param_indices(&input)?;
        let mut scope = state.enter_param_values_scope(
            named_params.clone(),
            named_param_display_names,
            positional_params.clone(),
            positional_param_display_names,
            positional_param_indices,
        );
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

    fn collect_positional_param_indices(
        plan: &spec::QueryPlan,
    ) -> PlanResult<HashMap<String, usize>> {
        fn visit(value: &Value, placeholders: &mut Vec<String>) {
            match value {
                Value::Object(map) => {
                    if let Some(Value::String(placeholder)) = map.get("placeholder") {
                        if placeholder.starts_with('?') && placeholder.len() > 1 {
                            placeholders.push(placeholder.clone());
                        }
                    }
                    for value in map.values() {
                        visit(value, placeholders);
                    }
                }
                Value::Array(values) => {
                    for value in values {
                        visit(value, placeholders);
                    }
                }
                _ => {}
            }
        }

        let plan = serde_json::to_value(plan).map_err(|e| PlanError::internal(e.to_string()))?;
        let mut placeholders = vec![];
        visit(&plan, &mut placeholders);
        placeholders
            .sort_by_key(|placeholder| placeholder[1..].parse::<usize>().unwrap_or(usize::MAX));
        placeholders.dedup();
        Ok(placeholders
            .into_iter()
            .enumerate()
            .map(|(index, placeholder)| (placeholder, index))
            .collect())
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
                .map(|b| Ok(cast_record_batch_positionally(b, schema.clone())?))
                .collect::<PlanResult<_>>()?;
            (schema, batches)
        } else if let [batch, ..] = batches.as_slice() {
            (batch.schema(), batches)
        } else {
            return Err(PlanError::invalid("missing schema for local relation"));
        };
        let target_partitions = self.ctx.copied_config().target_partitions().max(1);
        let partitions = Self::split_record_batches(batches, target_partitions);
        let table_provider = Arc::new(MemTable::try_new(schema, partitions)?);
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
        if name.eq_ignore_ascii_case("COALESCE") {
            let num_partitions = self
                .resolve_query_coalesce_hint_partition_count(parameters, state)
                .await?;
            return self
                .resolve_query_repartition(input, num_partitions, false, state)
                .await;
        }

        if name.eq_ignore_ascii_case("REPARTITION") {
            let (num_partitions, parameters) =
                Self::resolve_repartition_hint_parameters(parameters)?;
            return if parameters.is_empty() {
                self.resolve_query_repartition(
                    input,
                    num_partitions.ok_or_else(|| {
                        PlanError::AnalysisError(
                            "REPARTITION hint requires partition count or columns".to_string(),
                        )
                    })?,
                    true,
                    state,
                )
                .await
            } else {
                self.resolve_query_repartition_by_expression(
                    input,
                    parameters,
                    num_partitions,
                    state,
                )
                .await
            };
        }

        warn!("Hint operation '{name}' is not yet supported and is a no-op");
        self.resolve_query_plan_with_hidden_fields(input, state)
            .await
    }

    fn resolve_repartition_hint_parameters(
        parameters: Vec<spec::Expr>,
    ) -> PlanResult<(Option<usize>, Vec<spec::Expr>)> {
        let mut parameters = parameters.into_iter();
        let first = parameters.next();
        let mut partition_expressions = vec![];
        let num_partitions = match first {
            Some(parameter) => match Self::literal_to_partition_count(&parameter)? {
                Some(num_partitions) => Some(num_partitions),
                None => {
                    partition_expressions.push(Self::convert_repartition_hint_parameter(parameter));
                    None
                }
            },
            None => None,
        };

        for parameter in parameters {
            if Self::literal_to_partition_count(&parameter)?.is_some() {
                return Err(PlanError::AnalysisError(
                    "REPARTITION hint partition count must be the first parameter".to_string(),
                ));
            }
            partition_expressions.push(Self::convert_repartition_hint_parameter(parameter));
        }

        Ok((num_partitions, partition_expressions))
    }

    fn literal_to_partition_count(parameter: &spec::Expr) -> PlanResult<Option<usize>> {
        macro_rules! convert_signed {
            ($value:expr) => {
                match $value {
                    Some(value) if *value > 0 => Ok(Some(*value as usize)),
                    Some(value) => Err(PlanError::AnalysisError(format!(
                        "REPARTITION hint requires a positive partition count, got {value}"
                    ))),
                    None => Ok(None),
                }
            };
        }
        macro_rules! convert_unsigned {
            ($value:expr) => {
                match $value {
                    Some(value) if *value > 0 => Ok(Some(*value as usize)),
                    Some(value) => Err(PlanError::AnalysisError(format!(
                        "REPARTITION hint requires a positive partition count, got {value}"
                    ))),
                    None => Ok(None),
                }
            };
        }

        match parameter {
            spec::Expr::Literal(spec::Literal::Int8 { value }) => convert_signed!(value),
            spec::Expr::Literal(spec::Literal::Int16 { value }) => convert_signed!(value),
            spec::Expr::Literal(spec::Literal::Int32 { value }) => convert_signed!(value),
            spec::Expr::Literal(spec::Literal::Int64 { value }) => convert_signed!(value),
            spec::Expr::Literal(spec::Literal::UInt8 { value }) => convert_unsigned!(value),
            spec::Expr::Literal(spec::Literal::UInt16 { value }) => convert_unsigned!(value),
            spec::Expr::Literal(spec::Literal::UInt32 { value }) => convert_unsigned!(value),
            spec::Expr::Literal(spec::Literal::UInt64 { value }) => convert_unsigned!(value),
            _ => Ok(None),
        }
    }

    fn split_record_batches(
        batches: Vec<RecordBatch>,
        target_partitions: usize,
    ) -> Vec<Vec<RecordBatch>> {
        let total_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        if target_partitions <= 1 || total_rows <= 1 {
            return vec![batches];
        }

        let partition_count = target_partitions.min(total_rows);
        let mut partitions = Vec::with_capacity(partition_count);
        for partition in 0..partition_count {
            let start = partition * total_rows / partition_count;
            let end = (partition + 1) * total_rows / partition_count;
            let mut offset = 0;
            let mut partition_batches = vec![];
            for batch in &batches {
                let batch_start = offset;
                let batch_end = offset + batch.num_rows();
                let slice_start = start.max(batch_start);
                let slice_end = end.min(batch_end);
                if slice_start < slice_end {
                    partition_batches
                        .push(batch.slice(slice_start - batch_start, slice_end - slice_start));
                }
                offset = batch_end;
            }
            partitions.push(partition_batches);
        }
        partitions
    }

    fn convert_repartition_hint_parameter(parameter: spec::Expr) -> spec::Expr {
        match parameter {
            spec::Expr::Literal(spec::Literal::Utf8 { value: Some(name) })
            | spec::Expr::Literal(spec::Literal::LargeUtf8 { value: Some(name) })
            | spec::Expr::Literal(spec::Literal::Utf8View { value: Some(name) }) => {
                spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::bare(name),
                    plan_id: None,
                    is_metadata_column: false,
                }
            }
            parameter => parameter,
        }
    }

    async fn resolve_query_coalesce_hint_partition_count(
        &self,
        parameters: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<usize> {
        if parameters.len() != 1 {
            return Err(PlanError::invalid(format!(
                "COALESCE hint requires exactly one partition count, got {}",
                parameters.len()
            )));
        }

        let schema = Arc::new(DFSchema::empty());
        let parameter = parameters.into_iter().next().ok_or_else(|| {
            PlanError::invalid("COALESCE hint requires exactly one partition count")
        })?;
        let expr = self.resolve_expression(parameter, &schema, state).await?;
        let scalar = LiteralEvaluator::new().evaluate(&expr).map_err(|e| {
            PlanError::invalid(format!(
                "COALESCE hint requires an integer literal partition count: {e}"
            ))
        })?;
        let num_partitions = LiteralValue(&scalar).try_to_usize().map_err(|e| {
            PlanError::invalid(format!(
                "COALESCE hint requires an integer literal partition count: {e}"
            ))
        })?;

        if num_partitions == 0 {
            return Err(PlanError::invalid(
                "COALESCE hint requires at least one partition",
            ));
        }

        Ok(num_partitions)
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
