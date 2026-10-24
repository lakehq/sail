use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use sail_common::spec;

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::analysis::scope::ResolutionScope;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

fn compatible(from: &DataType, to: &DataType) -> bool {
    from == to
        || from.is_null()
        || to.is_null()
        || (from.is_numeric() && to.is_numeric())
        || (matches!(from, DataType::Utf8 | DataType::LargeUtf8)
            && matches!(to, DataType::Utf8 | DataType::LargeUtf8))
}

impl Resolver<'_, '_> {
    fn selected(&self, attribute: &Attribute, columns: &[spec::Identifier]) -> bool {
        columns.is_empty()
            || columns
                .iter()
                .any(|name| self.name_key(name.as_ref()) == self.name_key(&attribute.name))
    }

    fn missing(&mut self, attribute: &Attribute) -> PlanResult<ExprNode> {
        let reference = self.reference(attribute)?;
        let null = ExprNode {
            id: self.context.ids.expression(),
            kind: Deferred::Resolved(ExprKind::IsNull(Box::new(reference))),
            result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
        };
        let data_type = &attribute
            .result_type
            .resolved("missing value input")?
            .data_type;
        if matches!(
            data_type,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        ) {
            let reference = self.reference(attribute)?;
            let reference = if *data_type == DataType::Float16 {
                typing::coerce(reference, &DataType::Float32, &mut self.context.ids)?
            } else {
                reference
            };
            let nan = self.scalar("isnan", vec![reference])?;
            self.binary(null, Operator::Or, nan)
        } else {
            Ok(null)
        }
    }

    #[async_recursion::async_recursion]
    pub(super) async fn fill_na(
        &mut self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        values: Vec<spec::Expr>,
    ) -> PlanResult<QueryNode> {
        let input = self.query(input).await?;
        let input = self.visible_output(input)?;
        if values.is_empty()
            || (columns.is_empty() && values.len() != 1)
            || (!columns.is_empty() && values.len() != 1 && values.len() != columns.len())
        {
            return Err(PlanError::invalid(
                "fill na values must contain one value or one value per column",
            ));
        }
        let schema = input.schema.resolved("fill na input")?;
        let mut expressions = vec![];
        for attribute in &schema.attributes {
            let mut expression = self.reference(attribute)?;
            if self.selected(attribute, &columns) {
                let index = if values.len() == 1 {
                    0
                } else {
                    columns
                        .iter()
                        .position(|name| {
                            self.name_key(name.as_ref()) == self.name_key(&attribute.name)
                        })
                        .ok_or_else(|| PlanError::internal("fill na column not found"))?
                };
                let value = self
                    .expression(values[index].clone(), schema)
                    .await?
                    .expression;
                let target = attribute.result_type.resolved("fill na target")?;
                if compatible(
                    &value.result_type.resolved("fill na value")?.data_type,
                    &target.data_type,
                ) {
                    let mut target = target.clone();
                    target.nullable = true;
                    let value = ExprNode {
                        id: self.context.ids.expression(),
                        kind: Deferred::Resolved(ExprKind::Cast {
                            expression: Box::new(value),
                            target: target.clone(),
                            safe: true,
                        }),
                        result_type: Deferred::Resolved(target),
                    };
                    let condition = self.missing(attribute)?;
                    expression = typing::resolve_case(
                        self.context.ids.expression(),
                        None,
                        vec![(condition, value)],
                        Some(expression),
                        &mut self.context.ids,
                    )?;
                }
            }
            expressions.push(NamedExpression {
                name: attribute.name.clone(),
                expression,
            });
        }
        self.project(input, expressions)
    }

    #[async_recursion::async_recursion]
    pub(super) async fn drop_na(
        &mut self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        min_non_nulls: Option<usize>,
    ) -> PlanResult<QueryNode> {
        let input = self.query(input).await?;
        let input = self.visible_output(input)?;
        let mut predicates = vec![];
        for attribute in &input.schema.resolved("drop na input")?.attributes {
            if !self.selected(attribute, &columns) {
                continue;
            }
            let missing = self.missing(attribute)?;
            predicates.push(ExprNode {
                id: self.context.ids.expression(),
                kind: Deferred::Resolved(ExprKind::TruthTest {
                    expression: Box::new(missing),
                    value: Some(false),
                    negated: false,
                }),
                result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
            });
        }
        let condition = if let Some(count) = min_non_nulls.filter(|n| *n > 0) {
            let mut sum = self.literal(ScalarValue::UInt64(Some(0)));
            for condition in predicates {
                let one = self.literal(ScalarValue::UInt64(Some(1)));
                let zero = self.literal(ScalarValue::UInt64(Some(0)));
                let value = typing::resolve_case(
                    self.context.ids.expression(),
                    None,
                    vec![(condition, one)],
                    Some(zero),
                    &mut self.context.ids,
                )?;
                sum = self.binary(sum, Operator::Plus, value)?;
            }
            let count = self.literal(ScalarValue::UInt64(Some(count as u64)));
            self.binary(sum, Operator::GtEq, count)?
        } else {
            let mut predicates = predicates.into_iter();
            let first = predicates
                .next()
                .ok_or_else(|| PlanError::invalid("No columns specified for drop na."))?;
            predicates.try_fold(first, |left, right| self.binary(left, Operator::And, right))?
        };
        Self::filter(input, condition)
    }

    #[async_recursion::async_recursion]
    pub(super) async fn replace(
        &mut self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        replacements: Vec<spec::Replacement>,
    ) -> PlanResult<QueryNode> {
        let input = self.query(input).await?;
        let input = self.visible_output(input)?;
        let schema = input.schema.resolved("replace input")?;
        for column in &columns {
            if !schema
                .attributes
                .iter()
                .any(|a| self.name_key(&a.name) == self.name_key(column.as_ref()))
            {
                return Err(PlanError::analysis(format!(
                    "unresolved column: {}",
                    column.as_ref()
                )));
            }
        }
        let mut expressions = vec![];
        for attribute in &schema.attributes {
            let mut expression = self.reference(attribute)?;
            let target = &attribute.result_type.resolved("replace column")?.data_type;
            if self.selected(attribute, &columns) {
                let mut branches = vec![];
                for replacement in &replacements {
                    let old = self
                        .types()
                        .resolve_literal(replacement.old_value.clone())?;
                    if !compatible(&old.data_type(), target) {
                        continue;
                    }
                    let old = self.literal(old);
                    let old = typing::coerce(old, target, &mut self.context.ids)?;
                    let new = self
                        .types()
                        .resolve_literal(replacement.new_value.clone())?;
                    let new = self.literal(new);
                    let new = typing::coerce(new, target, &mut self.context.ids)?;
                    let reference = self.reference(attribute)?;
                    let condition = self.binary(reference, Operator::Eq, old)?;
                    branches.push((condition, new));
                }
                if !branches.is_empty() {
                    expression = typing::resolve_case(
                        self.context.ids.expression(),
                        None,
                        branches,
                        Some(expression),
                        &mut self.context.ids,
                    )?;
                }
            }
            expressions.push(NamedExpression {
                name: attribute.name.clone(),
                expression,
            });
        }
        self.project(input, expressions)
    }

    #[async_recursion::async_recursion]
    pub(super) async fn deduplicate(&mut self, dedup: spec::Deduplicate) -> PlanResult<QueryNode> {
        let input = self.query(*dedup.input).await?;
        if dedup.within_watermark {
            return Err(PlanError::todo("deduplicate within watermark"));
        }
        let schema = input.schema.resolved("deduplicate input")?;
        let node = if dedup.all_columns_as_keys && dedup.column_names.is_empty() {
            QueryKind::Distinct {
                input: Box::new(input.clone()),
            }
        } else if !dedup.all_columns_as_keys && !dedup.column_names.is_empty() {
            let mut keys = vec![];
            for name in dedup.column_names {
                let bound = ResolutionScope::new(schema, [])
                    .with_case_sensitive(self.context.config.case_sensitive)
                    .resolve(&[name.into()], None)?;
                keys.push(self.reference(schema.attribute(bound.id)?)?);
            }
            QueryKind::DistinctOn {
                input: Box::new(input.clone()),
                keys,
            }
        } else {
            return Err(PlanError::invalid(
                "must specify deduplicate column names or use all columns as keys",
            ));
        };
        Ok(QueryNode {
            schema: input.schema,
            kind: Deferred::Resolved(node),
        })
    }

    #[async_recursion::async_recursion]
    pub(super) async fn hint(
        &mut self,
        input: spec::QueryPlan,
        name: String,
        mut parameters: Vec<spec::Expr>,
    ) -> PlanResult<QueryNode> {
        let mut input = self.query(input).await?;
        let name = name.to_ascii_uppercase();
        if !matches!(name.as_str(), "REPARTITION" | "COALESCE") {
            log::warn!("Hint operation {name} is not yet supported and is a no-op");
            return Ok(input);
        }
        let schema = input.schema.resolved("hint input")?.clone();
        let mut count = self.context.config.shuffle_partitions;
        if let Some(first) = parameters.first() {
            let is_column = matches!(
                first,
                spec::Expr::UnresolvedAttribute { .. }
                    | spec::Expr::Literal(spec::Literal::Utf8 { .. })
            );
            if !is_column {
                if !partition_literal(first) {
                    return Err(PlanError::analysis(format!(
                        "{name} hint requires an integer literal partition count"
                    )));
                }
                let first = self
                    .expression(parameters.remove(0), &schema)
                    .await?
                    .expression;
                let value = crate::analysis::lowering::evaluate_constant(&first)?;
                if !matches!(
                    value,
                    ScalarValue::Int8(_)
                        | ScalarValue::Int16(_)
                        | ScalarValue::Int32(_)
                        | ScalarValue::Int64(_)
                ) {
                    return Err(PlanError::analysis("partition count must be an integer"));
                }
                let ScalarValue::Int64(Some(value)) = value.cast_to(&DataType::Int64)? else {
                    return Err(PlanError::analysis("partition count must be non-null"));
                };
                count = usize::try_from(value)
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| {
                        PlanError::invalid(format!("{name} hint requires at least one partition"))
                    })?;
            } else if name == "COALESCE" {
                return Err(PlanError::analysis(
                    "COALESCE requires an integer partition count",
                ));
            }
        } else if name == "COALESCE" {
            return Err(PlanError::analysis("COALESCE requires a partition count"));
        }
        let partitioning = if name == "COALESCE" {
            if !parameters.is_empty() {
                return Err(PlanError::analysis("COALESCE requires one partition count"));
            }
            Partitioning::Coalesce(count)
        } else if parameters.is_empty() {
            Partitioning::RoundRobin(count)
        } else {
            let mut expressions = vec![];
            for parameter in parameters {
                let parameter = match parameter {
                    spec::Expr::UnresolvedAttribute { .. } => parameter,
                    spec::Expr::Literal(spec::Literal::Utf8 { value: Some(name) }) => {
                        spec::Expr::UnresolvedAttribute {
                            name: sail_sql_analyzer::expression::from_ast_object_name(
                                sail_sql_analyzer::parser::parse_object_name(&name)
                                    .map_err(|e| PlanError::analysis(e.to_string()))?,
                            )?,
                            plan_id: None,
                            is_metadata_column: false,
                        }
                    }
                    _ => {
                        return Err(PlanError::analysis(
                            "REPARTITION requires column names after its partition count",
                        ));
                    }
                };
                let (next, value) = self.recover_expression(input, parameter, false).await?;
                input = next;
                expressions.push(value.expression);
            }
            Partitioning::Hash(expressions, count)
        };
        let partitioned = QueryNode {
            schema: input.schema.clone(),
            kind: Deferred::Resolved(QueryKind::Repartition {
                input: Box::new(input),
                partitioning,
            }),
        };
        self.restore_output(partitioned, schema)
    }
}

fn partition_literal(value: &spec::Expr) -> bool {
    match value {
        spec::Expr::Literal(_) => true,
        spec::Expr::UnresolvedFunction(function) => {
            matches!(function.function_name.parts(), [name] if matches!(name.as_ref(), "-" | "negative" | "+" | "positive"))
                && matches!(function.arguments.as_slice(), [spec::Expr::Literal(_)])
        }
        _ => false,
    }
}
