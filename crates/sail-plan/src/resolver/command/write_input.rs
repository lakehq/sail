use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF, WindowFrame, WindowFunctionDefinition};
use sail_common::spec;
use sail_common_datafusion::catalog::{CatalogTableColumnIdentity, TableColumnStatus};
use sail_common_datafusion::column_features::{
    ColumnFeaturesBuilder, SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY,
};
use sail_function::scalar::math::spark_try_add::SparkTryAdd;
use sail_function::scalar::math::spark_try_mult::SparkTryMult;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{TableInfo, WriteColumnMatch};
use crate::resolver::{NamedExpr, PlanResolver};

impl PlanResolver<'_> {
    pub(in crate::resolver) async fn rewrite_write_input(
        &mut self,
        input: Query,
        matching: WriteColumnMatch,
        info: &TableInfo,
    ) -> PlanResult<Query> {
        let mut provided = classify(&input, &matching, info)?;
        let input = if Self::plan_has_default_column_value(&input)? {
            let mut defaults = vec![];
            for column in &info.columns {
                defaults.push(self.column_default(column, info).await?);
            }
            let bindings = provided
                .iter()
                .enumerate()
                .filter_map(|(index, value)| value.as_ref().map(|attribute| (attribute.id, index)))
                .collect();
            let input = self.replace_write_defaults(input, &bindings, &defaults)?;
            if Self::plan_has_default_column_value(&input)? {
                return Err(PlanError::invalid(
                    "DEFAULT must be a standalone INSERT value",
                ));
            }
            for attribute in provided.iter_mut().flatten() {
                *attribute = input
                    .schema
                    .resolved("default output")?
                    .attribute(attribute.id)?
                    .clone();
            }
            input
        } else {
            input
        };
        let mut intermediate = vec![];
        let mut slots = vec![None; info.columns.len()];
        for (index, (column, value)) in info.columns.iter().zip(&provided).enumerate() {
            if value.is_none()
                && (column.generated_always_as.is_some() || column.identity.is_some())
            {
                continue;
            }
            if value.is_some()
                && column
                    .identity
                    .as_ref()
                    .is_some_and(|identity| !identity.allow_explicit_insert)
            {
                return Err(PlanError::invalid(format!(
                    "Providing values for GENERATED ALWAYS AS IDENTITY column `{}` is not supported",
                    column.name
                )));
            }
            let value = match value {
                Some(attribute) => self.reference(attribute)?,
                None => self.column_default(column, info).await?,
            };
            let expression = self.align_store_value(value, &column.data_type, false)?;
            slots[index] = Some(intermediate.len());
            intermediate.push(NamedExpr {
                name: column.name.clone(),
                expression,
            });
        }
        let mut input = self.project(input, intermediate)?;
        // User-provided generated values are hidden from generation expressions.
        let mut schema = input.schema.clone().into_resolved("write intermediate")?;
        for (index, column) in info.columns.iter().enumerate() {
            if column.generated_always_as.is_some()
                && let Some(slot) = slots[index]
            {
                schema.attributes[slot].hidden = true;
            }
        }
        input.schema = Deferred::Resolved(schema);
        let row_number = if info
            .columns
            .iter()
            .zip(&provided)
            .any(|(column, value)| column.identity.is_some() && value.is_none())
        {
            let function = WindowFunctionDefinition::WindowUDF(
                datafusion::functions_window::row_number::row_number_udwf(),
            );
            let resolved = crate::analyzer::typing::aggregate::resolve_window(&function, &[])?;
            let expression = Expr {
                id: self.context.ids.expression(),
                result_type: Deferred::Resolved(ExprType::from_field(&resolved.result)),
                kind: Deferred::Resolved(ExprKind::WindowFunction {
                    function,
                    arguments: vec![],
                    partition_by: vec![],
                    order_by: vec![],
                    frame: WindowFrame::new(None),
                    filter: None,
                    distinct: false,
                    null_treatment: Some(datafusion_expr::expr::NullTreatment::RespectNulls),
                }),
            };
            let mut schema = input.schema.resolved("identity input")?.clone();
            let mut attribute = self.context.function_context().attribute(
                "identity_row_number".into(),
                expression
                    .result_type
                    .resolved("identity row number")?
                    .clone(),
            );
            attribute.hidden = true;
            schema.attributes.push(attribute.clone());
            input = Query {
                kind: Deferred::Resolved(QueryKind::Window {
                    input: Box::new(input),
                    expressions: vec![Projection {
                        expression,
                        attribute: attribute.clone(),
                    }],
                }),
                schema: Deferred::Resolved(schema),
            };
            Some(attribute)
        } else {
            None
        };
        let schema = input.schema.resolved("generation scope")?.clone();
        let mut final_values = vec![];
        for (index, column) in info.columns.iter().enumerate() {
            let value = if let Some(source) = &column.generated_always_as {
                let request = parse_expression(source)?;
                let generated = self
                    .resolve_named_expression(request, &schema)
                    .await?
                    .expression;
                if let Some(slot) = slots[index] {
                    let user = self.reference(&schema.attributes[slot])?;
                    let null = self.context.function_context().is_null(user);
                    let user = self.reference(&schema.attributes[slot])?;
                    let copy = self
                        .context
                        .function_context()
                        .copy_expression(&generated)?;
                    let cast = self
                        .context
                        .function_context()
                        .cast(copy, column.data_type.clone())?;
                    let equal = self.context.function_context().binary(
                        user,
                        Operator::IsNotDistinctFrom,
                        cast,
                    )?;
                    let valid =
                        self.context
                            .function_context()
                            .binary(null, Operator::Or, equal)?;
                    let message = format!(
                        "[DELTA_GENERATED_COLUMNS_VALUE_MISMATCH] CHECK constraint for generated column `{}` (expression: {source}) violated: user-provided value does not match.",
                        column.name
                    );
                    let error = self.write_error(message)?;
                    self.context
                        .function_context()
                        .case(vec![(valid, generated)], Some(error))?
                } else {
                    generated
                }
            } else if let Some(slot) = slots[index] {
                self.reference(&schema.attributes[slot])?
            } else if let Some(identity) = &column.identity {
                let row = row_number
                    .as_ref()
                    .ok_or_else(|| PlanError::internal("identity row number missing"))?;
                self.identity_value(&column.name, identity, row)?
            } else {
                return Err(PlanError::internal("write column has no value"));
            };
            let value = self.align_store_value(value, &column.data_type, false)?;
            let value = crate::function::scalar::datetime::declare_nullable_result(
                &mut self.context.function_context(),
                value,
                column.nullable,
            )?;
            let mut result_type = value.result_type.resolved("write field")?.clone();
            let mut metadata = ColumnFeaturesBuilder::new();
            if let Some(generation) = &column.generated_always_as {
                metadata = metadata.with_generation_expression(generation);
            }
            if let Some(default) = &column.default {
                metadata = metadata.with_current_default(default);
            }
            if let Some(identity) = &column.identity {
                metadata = metadata.with_identity(identity);
            }
            if info.format.eq_ignore_ascii_case("delta") && !column.nullable {
                metadata = metadata.with_not_null_constraint();
            }
            result_type.metadata.extend(metadata.build());
            result_type.metadata.insert(
                SAIL_WRITE_TARGET_NULLABLE_METADATA_KEY.into(),
                column.nullable.to_string(),
            );
            let expression = Expr {
                id: self.context.ids.expression(),
                kind: Deferred::Resolved(ExprKind::WithMetadata(Box::new(value))),
                result_type: Deferred::Resolved(result_type),
            };
            final_values.push(NamedExpr {
                name: column.name.clone(),
                expression,
            });
        }
        self.project(input, final_values)
    }

    pub(in crate::resolver) async fn column_default(
        &mut self,
        column: &TableColumnStatus,
        info: &TableInfo,
    ) -> PlanResult<Expr> {
        if let Some(value) = info.default_values.get(&column.name) {
            return Ok(self.context.function_context().literal(value.clone()));
        }
        if let Some(source) = &column.default {
            return self.resolve_column_default_expression(source).await;
        }
        Ok(self
            .context
            .function_context()
            .literal(ScalarValue::try_from(&column.data_type)?))
    }

    pub(in crate::resolver) async fn resolve_column_default_expression(
        &mut self,
        source: &str,
    ) -> PlanResult<Expr> {
        let request = parse_expression(source)?;
        let request = if matches!(request, spec::Expr::UnresolvedAttribute { .. }) {
            spec::Expr::Literal(spec::Literal::Utf8 {
                value: Some(source.into()),
            })
        } else {
            request
        };
        Ok(self
            .resolve_named_expression(request, &QuerySchema::default())
            .await?
            .expression)
    }

    pub(in crate::resolver) fn write_error(&mut self, message: String) -> PlanResult<Expr> {
        let message = self
            .context
            .function_context()
            .literal(ScalarValue::Utf8(Some(message)));
        self.context
            .function_context()
            .scalar_udf(Arc::new(ScalarUDF::from(RaiseError::new())), vec![message])
    }

    fn identity_value(
        &mut self,
        name: &str,
        identity: &CatalogTableColumnIdentity,
        row: &Attribute,
    ) -> PlanResult<Expr> {
        let base = identity.high_water_mark.map(|n| n.checked_add(identity.step)).unwrap_or(Some(identity.start))
            .ok_or_else(|| PlanError::invalid(format!("identity column `{name}` cannot generate a next value without overflowing BIGINT")))?;
        let row = self.reference(row)?;
        let row = self.context.function_context().cast(row, DataType::Int64)?;
        let one = self
            .context
            .function_context()
            .literal(ScalarValue::Int64(Some(1)));
        let offset = self
            .context
            .function_context()
            .binary(row, Operator::Minus, one)?;
        let step = self
            .context
            .function_context()
            .literal(ScalarValue::Int64(Some(identity.step)));
        let product = self.context.function_context().scalar_udf(
            Arc::new(ScalarUDF::from(SparkTryMult::new())),
            vec![offset, step],
        )?;
        let copy = self.context.function_context().copy_expression(&product)?;
        let base = self
            .context
            .function_context()
            .literal(ScalarValue::Int64(Some(base)));
        let value = self.context.function_context().scalar_udf(
            Arc::new(ScalarUDF::from(SparkTryAdd::new())),
            vec![base, copy],
        )?;
        let product_null = self.context.function_context().is_null(product);
        let copy = self.context.function_context().copy_expression(&value)?;
        let value_null = self.context.function_context().is_null(copy);
        let invalid =
            self.context
                .function_context()
                .binary(product_null, Operator::Or, value_null)?;
        let error = self.write_error(format!("[DELTA_IDENTITY_COLUMN_VALUE_OVERFLOW] identity column `{name}` overflowed BIGINT while generating values"))?;
        self.context
            .function_context()
            .case(vec![(invalid, error)], Some(value))
    }
}

pub(in crate::resolver) fn parse_expression(source: &str) -> PlanResult<spec::Expr> {
    Ok(sail_sql_analyzer::expression::from_ast_expression(
        sail_sql_analyzer::parser::parse_expression(source)?,
    )?)
}

fn classify(
    input: &Query,
    matching: &WriteColumnMatch,
    info: &TableInfo,
) -> PlanResult<Vec<Option<Attribute>>> {
    let fields = &input.schema.resolved("write input fields")?.attributes;
    let mut output = vec![None; info.columns.len()];
    let required = info
        .columns
        .iter()
        .filter(|column| !info.column_is_omittable(column, matching))
        .count();
    match matching {
        WriteColumnMatch::ByPosition => {
            if fields.len() == info.columns.len() {
                for (target, field) in output.iter_mut().zip(fields) {
                    *target = Some(field.clone());
                }
            } else if fields.len() == required {
                let mut values = fields.iter();
                for (target, column) in output.iter_mut().zip(&info.columns) {
                    if !info.column_is_omittable(column, matching) {
                        *target = values.next().cloned();
                    }
                }
            } else {
                return Err(PlanError::invalid(format!(
                    "input schema for INSERT has {} fields, but table schema has {} fields ({required} required)",
                    fields.len(),
                    info.columns.len()
                )));
            }
        }
        WriteColumnMatch::ByName => {
            if required == info.columns.len() && fields.len() != required {
                return Err(PlanError::invalid(
                    "input schema for INSERT by name does not match table width",
                ));
            }
            for (target, column) in output.iter_mut().zip(&info.columns) {
                let mut matches = fields
                    .iter()
                    .filter(|field| field.name.eq_ignore_ascii_case(&column.name));
                *target = matches.next().cloned();
                if matches.next().is_some() {
                    return Err(PlanError::invalid(format!(
                        "ambiguous column for INSERT by name: {}",
                        column.name
                    )));
                }
            }
        }
        WriteColumnMatch::ByColumns { columns } => {
            if columns.len() != fields.len() {
                return Err(PlanError::invalid(
                    "INSERT column list does not match input width",
                ));
            }
            let targets = info
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name.to_ascii_lowercase(), i))
                .collect::<HashMap<_, _>>();
            for (column, field) in columns.iter().zip(fields) {
                let index = targets
                    .get(&column.as_ref().to_ascii_lowercase())
                    .copied()
                    .ok_or_else(|| {
                        PlanError::invalid(format!("column not found in target table: {column:?}"))
                    })?;
                if output[index].is_some() {
                    return Err(PlanError::invalid(format!(
                        "column specified more than once in INSERT: {column:?}"
                    )));
                }
                output[index] = Some(field.clone());
            }
        }
    }
    for (provided, column) in output.iter().zip(&info.columns) {
        if provided.is_none() && !info.column_is_omittable(column, matching) {
            return Err(PlanError::invalid(format!(
                "INSERT is missing value for non-generated column `{}`",
                column.name
            )));
        }
    }
    Ok(output)
}
