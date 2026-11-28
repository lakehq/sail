// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    Column as LogicalColumn, DataFusionError, Result, ScalarValue, ToDFSchema,
};
use datafusion::functions::core::expr_fn::nullif;
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::functions::string::expr_fn::concat;
use datafusion::logical_expr::expr::{Between, BinaryExpr, Case, Cast, InList, TryCast};
use datafusion::logical_expr::utils::{conjunction, disjunction, split_conjunction};
use datafusion::logical_expr::{Expr, Operator, ScalarUDF, lit};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use sail_common_datafusion::session::ansi_mode::spark_session_ansi_mode;
use sail_function::scalar::datetime::partition_timestamp::DeltaPartitionTimestamp;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::datasource::{PATH_COLUMN, predicate_uses_struct_value, simplify_expr};
use crate::physical_plan::DeltaMetadataStatsExec;
use crate::schema::{logical_to_physical_arrow_paths, make_physical_arrow_schema};
use crate::spec::fields::{
    FIELD_NAME_STATS_PARSED, STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES,
    STATS_FIELD_NULL_COUNT, STATS_FIELD_NUM_RECORDS,
};
use crate::spec::{ColumnMappingMode, DataSkippingNumIndexedCols, StructType, stats_schema};
use crate::table::DeltaSnapshot;

pub(crate) fn predicate_requires_stats(expr: &Expr, partition_columns: &[String]) -> bool {
    let partition_columns: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
    expr.column_refs()
        .iter()
        .any(|col| !partition_columns.contains(col.name.as_str()))
}

pub(crate) fn build_metadata_filter(
    session: &dyn Session,
    input: Arc<dyn ExecutionPlan>,
    snapshot: &DeltaSnapshot,
    logical_schema: &ArrowSchema,
    predicate: Expr,
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_columns = snapshot.metadata().partition_columns().clone();
    let predicate = if snapshot.effective_column_mapping_mode() == ColumnMappingMode::None {
        predicate
    } else {
        // File statistics of column-mapped tables use physical struct field names, so a
        // conjunct using a whole struct value cannot be evaluated against them. Dropping a
        // top-level conjunct only keeps more files, and the rows are filtered after the scan.
        let df_schema = logical_schema.clone().to_dfschema()?;
        conjunction(
            split_conjunction(&predicate)
                .into_iter()
                .filter(|conjunct| !predicate_uses_struct_value(conjunct, &df_schema))
                .cloned(),
        )
        .unwrap_or_else(|| lit(true))
    };
    let needs_stats = predicate_requires_stats(&predicate, &partition_columns);
    let stats_paths = logical_to_physical_arrow_paths(
        snapshot.schema(),
        snapshot.effective_column_mapping_mode(),
    );
    let (input, predicate) = if needs_stats {
        let stats_schema = build_metadata_stats_schema(snapshot, &predicate)?;
        (
            Arc::new(DeltaMetadataStatsExec::new(input, stats_schema)) as Arc<dyn ExecutionPlan>,
            rewrite_predicate_for_metadata(
                predicate,
                &partition_columns,
                &stats_paths,
                logical_schema,
            ),
        )
    } else {
        (input, predicate)
    };
    let predicate = type_partition_columns(
        predicate,
        &partition_columns,
        logical_schema,
        snapshot.partition_time_zone(),
        spark_session_ansi_mode(session.config().options()),
    )?;
    let df_schema = input.schema().to_dfschema()?;
    let physical_expr = simplify_expr(session, &df_schema, predicate)?;
    Ok(Arc::new(FilterExec::try_new(physical_expr, input)?))
}

/// Fails for a file of the log replay `input` whose partition value does not parse. Such a value
/// reads as NULL, so rewriting the file would write NULL in its place. Delta-Spark rewrites it as
/// NULL; Sail refuses to, in both ANSI modes, so that the value is not lost.
pub(crate) fn build_rewrite_guard(
    session: &dyn Session,
    input: Arc<dyn ExecutionPlan>,
    snapshot: &DeltaSnapshot,
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_columns = snapshot.metadata().partition_columns();
    let logical_schema = snapshot.schema();
    let mut when_then = Vec::new();
    for name in partition_columns {
        let data_type = logical_schema.field_with_name(name)?.data_type();
        if matches!(
            data_type,
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View
        ) {
            continue;
        }
        let raw = Expr::Column(LogicalColumn::new_unqualified(name));
        let value = type_partition_columns(
            raw.clone(),
            partition_columns,
            logical_schema,
            snapshot.partition_time_zone(),
            false,
        )?;
        let does_not_parse = nullif(raw.clone(), lit(""))
            .is_not_null()
            .and(value.is_null());
        let message = concat(vec![
            lit("Cannot rewrite the Delta file '"),
            Expr::Column(LogicalColumn::new_unqualified(PATH_COLUMN)),
            lit("' because the value '"),
            raw,
            lit(format!(
                "' of its partition column `{name}` cannot be read as {data_type}, so the rewritten file would have NULL instead."
            )),
        ]);
        let raise = ScalarUDF::from(RaiseError::new()).call(vec![message]);
        when_then.push((Box::new(does_not_parse), Box::new(raise)));
    }
    if when_then.is_empty() {
        return Ok(input);
    }
    let predicate = Expr::Case(Case::new(None, when_then, Some(Box::new(lit(true)))));
    let physical_expr = simplify_expr(session, &input.schema().to_dfschema()?, predicate)?;
    Ok(Arc::new(FilterExec::try_new(physical_expr, input)?))
}

fn type_partition_columns(
    predicate: Expr,
    partition_columns: &[String],
    logical_schema: &ArrowSchema,
    time_zone: &str,
    ansi_mode: bool,
) -> Result<Expr> {
    // Replay carries raw strings for Add decoding; only predicate inputs use logical types.
    predicate
        .transform_up(|expr| {
            let Expr::Column(column) = &expr else {
                return Ok(Transformed::no(expr));
            };
            if !partition_columns.contains(&column.name) {
                return Ok(Transformed::no(expr));
            }
            let data_type = logical_schema
                .field_with_name(&column.name)?
                .data_type()
                .clone();
            let mut value = nullif(expr, lit(""));
            // Parse a value as `PartitionValueParser` does: a TIMESTAMP value in the session time
            // zone, and a value that does not parse as NULL, or as an error in ANSI mode, as
            // Delta-Spark casts partition values in its filters.
            if let ArrowDataType::Timestamp(_, Some(_)) = data_type {
                let parse = DeltaPartitionTimestamp::new(Arc::from(time_zone), ansi_mode);
                value = ScalarUDF::from(parse).call(vec![value]);
            }
            let value = Box::new(value);
            Ok(Transformed::yes(if ansi_mode {
                Expr::Cast(Cast::new(value, data_type))
            } else {
                Expr::TryCast(TryCast::new(value, data_type))
            }))
        })
        .map(|transformed| transformed.data)
}

pub(crate) fn build_metadata_stats_schema(
    snapshot: &DeltaSnapshot,
    predicate: &Expr,
) -> Result<SchemaRef> {
    let partition_columns = snapshot.metadata().partition_columns();
    let mode = snapshot.effective_column_mapping_mode();
    let referenced_columns = predicate
        .column_refs()
        .iter()
        .filter_map(|column| {
            if snapshot.schema().field_with_name(&column.name).is_ok() {
                Some(column.name.clone())
            } else {
                let root = column.name.split('.').next()?;
                snapshot
                    .schema()
                    .field_with_name(root)
                    .is_ok()
                    .then(|| root.to_string())
            }
        })
        .collect::<HashSet<_>>();
    let non_partition_fields = snapshot
        .schema()
        .fields()
        .iter()
        .filter(|field| !partition_columns.contains(field.name()))
        .filter(|field| referenced_columns.contains(field.name()))
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let logical_non_partition = ArrowSchema::new(non_partition_fields);
    let physical_arrow = make_physical_arrow_schema(&logical_non_partition, mode);
    let physical_kernel = StructType::try_from(&physical_arrow)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    // The parsing schema describes predicate-referenced columns, including columns for which an
    // individual Add has no statistics. Missing JSON fields then materialize as NULL and keep the
    // file conservatively. Applying the table's logical stats-column configuration after mapping
    // this schema to physical names can instead omit the field and make get_field fail planning.
    let mut parsing_properties = snapshot.table_properties().clone();
    parsing_properties.data_skipping_stats_columns = None;
    parsing_properties.data_skipping_num_indexed_cols =
        Some(DataSkippingNumIndexedCols::AllColumns);
    let stats_schema = stats_schema(&physical_kernel, &parsing_properties)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(Arc::new(
        ArrowSchema::try_from(&stats_schema).map_err(|e| DataFusionError::External(Box::new(e)))?,
    ))
}

fn rewrite_predicate_for_metadata(
    expr: Expr,
    partition_columns: &[String],
    stats_paths: &HashMap<Vec<String>, Vec<String>>,
    logical_schema: &ArrowSchema,
) -> Expr {
    let partition_columns = partition_columns.iter().cloned().collect::<HashSet<_>>();
    MetadataPredicateRewriter {
        partition_columns,
        stats_paths,
        logical_schema,
    }
    .rewrite(expr)
}

struct MetadataPredicateRewriter<'a> {
    partition_columns: HashSet<String>,
    stats_paths: &'a HashMap<Vec<String>, Vec<String>>,
    logical_schema: &'a ArrowSchema,
}

#[derive(Clone)]
struct ExprTemplate {
    logical_path: Vec<String>,
    cast_type: Option<ArrowDataType>,
    /// Whether the cast only changes the time zone label of a TIMESTAMP column, which keeps the
    /// order of the values.
    relabels_timestamp: bool,
}

impl MetadataPredicateRewriter<'_> {
    fn rewrite(&self, expr: Expr) -> Expr {
        if expr
            .column_refs()
            .iter()
            .all(|column| self.partition_columns.contains(&column.name))
        {
            return expr;
        }
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And | Operator::Or => Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(self.rewrite(*left)),
                    op,
                    Box::new(self.rewrite(*right)),
                )),
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => self
                    .rewrite_comparison(*left, op, *right)
                    .unwrap_or_else(literal_true),
                _ => literal_true(),
            },
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => self
                .rewrite_between(*expr, negated, *low, *high)
                .unwrap_or_else(literal_true),
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => self
                .rewrite_in_list(*expr, list, negated)
                .unwrap_or_else(literal_true),
            Expr::IsNull(expr) => self
                .rewrite_null_check(*expr, false)
                .unwrap_or_else(literal_true),
            Expr::IsNotNull(expr) => self
                .rewrite_null_check(*expr, true)
                .unwrap_or_else(literal_true),
            Expr::Not(expr) => match *expr {
                Expr::IsNull(expr) => self
                    .rewrite_null_check(*expr, true)
                    .unwrap_or_else(literal_true),
                Expr::IsNotNull(expr) => self
                    .rewrite_null_check(*expr, false)
                    .unwrap_or_else(literal_true),
                // Any other NOT is not safe to negate over rewritten stats bounds –
                // fall back to keeping all files (literal_true = no pruning).
                _ => literal_true(),
            },
            Expr::Alias(alias) => self.rewrite(*alias.expr),
            Expr::Literal(..) => expr,
            _ => literal_true(),
        }
    }

    fn rewrite_comparison(&self, left: Expr, op: Operator, right: Expr) -> Option<Expr> {
        if left.column_refs().is_empty() && right.column_refs().is_empty() {
            return Some(binary(left, op, right));
        }

        if let Some(template) = self.extract_template(&left) {
            if !right.column_refs().is_empty() {
                return None;
            }
            return Some(self.rewrite_template_comparison(template, op, right));
        }

        if let Some(template) = self.extract_template(&right) {
            if !left.column_refs().is_empty() {
                return None;
            }
            return Some(self.rewrite_template_comparison(template, reverse_comparison(op)?, left));
        }

        None
    }

    fn rewrite_between(&self, expr: Expr, negated: bool, low: Expr, high: Expr) -> Option<Expr> {
        if !low.column_refs().is_empty() || !high.column_refs().is_empty() {
            return None;
        }
        let template = self.extract_template(&expr)?;
        if self.is_partition_column(&template) {
            return Some(Expr::Between(Between::new(
                Box::new(expr),
                negated,
                Box::new(low),
                Box::new(high),
            )));
        }
        if template.may_reorder() {
            return None;
        }

        let min_expr = template.apply(self.stats_bound_expr(&template.logical_path, true));
        let max_expr = template.apply(self.stats_bound_expr(&template.logical_path, false));
        let missing = any_null([min_expr.clone(), max_expr.clone()]);
        let actual = if negated {
            disjunction(vec![
                binary(min_expr, Operator::Lt, low),
                binary(max_expr, Operator::Gt, high),
            ])
            .unwrap_or_else(literal_true)
        } else {
            conjunction(vec![
                binary(max_expr, Operator::GtEq, low),
                binary(min_expr, Operator::LtEq, high),
            ])
            .unwrap_or_else(literal_true)
        };
        Some(or(missing, actual))
    }

    fn rewrite_in_list(&self, expr: Expr, list: Vec<Expr>, negated: bool) -> Option<Expr> {
        let template = self.extract_template(&expr)?;
        if list.iter().any(|expr| !expr.column_refs().is_empty()) {
            return None;
        }
        if self.is_partition_column(&template) {
            return Some(Expr::InList(InList::new(Box::new(expr), list, negated)));
        }
        if template.may_reorder() {
            return None;
        }
        let mut rewritten = Vec::with_capacity(list.len());
        for value in list {
            let op = if negated {
                Operator::NotEq
            } else {
                Operator::Eq
            };
            rewritten.push(self.rewrite_template_comparison(template.clone(), op, value));
        }
        if negated {
            Some(conjunction(rewritten).unwrap_or_else(literal_true))
        } else {
            Some(disjunction(rewritten).unwrap_or_else(literal_true))
        }
    }

    fn rewrite_null_check(&self, expr: Expr, is_not_null: bool) -> Option<Expr> {
        let template = self.extract_template(&expr)?;
        if self.is_partition_column(&template) {
            return Some(if is_not_null {
                Expr::IsNotNull(Box::new(expr))
            } else {
                Expr::IsNull(Box::new(expr))
            });
        }
        if template.may_reorder() {
            return None;
        }

        let null_count = template.apply(self.stats_null_count_expr(&template.logical_path));
        if is_not_null {
            let num_records = self.stats_num_records_expr();
            Some(
                disjunction(vec![
                    null_count.clone().is_null(),
                    num_records.clone().is_null(),
                    binary(null_count, Operator::Lt, num_records),
                ])
                .unwrap_or_else(literal_true),
            )
        } else {
            Some(
                disjunction(vec![
                    null_count.clone().is_null(),
                    binary(null_count, Operator::Gt, literal_i64(0)),
                ])
                .unwrap_or_else(literal_true),
            )
        }
    }

    fn rewrite_template_comparison(
        &self,
        template: ExprTemplate,
        op: Operator,
        value: Expr,
    ) -> Expr {
        if self.is_partition_column(&template) {
            return binary(template.apply(column_expr(template.root_name())), op, value);
        }
        // File bounds can only be pushed through a proven order-preserving cast. Keep the file
        // when cast monotonicity is unknown rather than risking false pruning.
        if template.may_reorder() {
            return literal_true();
        }

        let min_expr = template.apply(self.stats_bound_expr(&template.logical_path, true));
        let max_expr = template.apply(self.stats_bound_expr(&template.logical_path, false));
        match op {
            Operator::Eq => or(
                any_null([min_expr.clone(), max_expr.clone()]),
                and(
                    binary(min_expr, Operator::LtEq, value.clone()),
                    binary(max_expr, Operator::GtEq, value),
                ),
            ),
            Operator::NotEq => or(
                any_null([min_expr.clone(), max_expr.clone()]),
                disjunction(vec![
                    binary(min_expr, Operator::Lt, value.clone()),
                    binary(max_expr, Operator::Gt, value),
                ])
                .unwrap_or_else(literal_true),
            ),
            Operator::Lt => or(
                min_expr.clone().is_null(),
                binary(min_expr, Operator::Lt, value),
            ),
            Operator::LtEq => or(
                min_expr.clone().is_null(),
                binary(min_expr, Operator::LtEq, value),
            ),
            Operator::Gt => or(
                max_expr.clone().is_null(),
                binary(max_expr, Operator::Gt, value),
            ),
            Operator::GtEq => or(
                max_expr.clone().is_null(),
                binary(max_expr, Operator::GtEq, value),
            ),
            _ => literal_true(),
        }
    }

    fn is_partition_column(&self, template: &ExprTemplate) -> bool {
        template.logical_path.len() == 1 && self.partition_columns.contains(template.root_name())
    }

    fn stats_num_records_expr(&self) -> Expr {
        get_field(
            column_expr(FIELD_NAME_STATS_PARSED),
            STATS_FIELD_NUM_RECORDS,
        )
    }

    fn stats_null_count_expr(&self, logical_path: &[String]) -> Expr {
        self.stats_nested_expr(STATS_FIELD_NULL_COUNT, logical_path)
    }

    fn stats_bound_expr(&self, logical_path: &[String], is_min: bool) -> Expr {
        self.stats_nested_expr(
            if is_min {
                STATS_FIELD_MIN_VALUES
            } else {
                STATS_FIELD_MAX_VALUES
            },
            logical_path,
        )
    }

    fn stats_nested_expr(&self, root: &str, logical_path: &[String]) -> Expr {
        let mut expr = get_field(column_expr(FIELD_NAME_STATS_PARSED), root);
        let path = self
            .stats_paths
            .get(logical_path)
            .cloned()
            .unwrap_or_else(|| logical_path.to_vec());
        for segment in path {
            expr = get_field(expr, &segment);
        }
        expr
    }

    fn extract_template(&self, expr: &Expr) -> Option<ExprTemplate> {
        match expr {
            Expr::Cast(Cast { expr, field }) => {
                let logical_path = extract_column_path(expr)?;
                let relabels_timestamp = match (
                    field.data_type(),
                    self.logical_schema
                        .field_with_name(&logical_path[0])
                        .map(|field| field.data_type()),
                ) {
                    (
                        ArrowDataType::Timestamp(unit, Some(_)),
                        Ok(ArrowDataType::Timestamp(column_unit, Some(_))),
                    ) => logical_path.len() == 1 && unit == column_unit,
                    _ => false,
                };
                Some(ExprTemplate {
                    logical_path,
                    cast_type: Some(field.data_type().clone()),
                    relabels_timestamp,
                })
            }
            _ => Some(ExprTemplate {
                logical_path: extract_column_path(expr)?,
                cast_type: None,
                relabels_timestamp: false,
            }),
        }
    }
}

impl ExprTemplate {
    /// Whether file bounds cannot be pushed through the cast, because it may change the order of
    /// the values.
    fn may_reorder(&self) -> bool {
        self.cast_type.is_some() && !self.relabels_timestamp
    }

    fn root_name(&self) -> &str {
        self.logical_path[0].as_str()
    }

    fn apply(&self, expr: Expr) -> Expr {
        if let Some(data_type) = &self.cast_type {
            Expr::Cast(Cast::new(Box::new(expr), data_type.clone()))
        } else {
            expr
        }
    }
}

fn extract_column_path(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::Column(column) => Some(vec![column.name.clone()]),
        Expr::ScalarFunction(function) if function.func.inner().is::<GetFieldFunc>() => {
            let [base, field] = function.args.as_slice() else {
                return None;
            };
            let field = match field {
                Expr::Literal(ScalarValue::Utf8(Some(field)), _)
                | Expr::Literal(ScalarValue::LargeUtf8(Some(field)), _)
                | Expr::Literal(ScalarValue::Utf8View(Some(field)), _) => field,
                _ => return None,
            };
            let mut path = extract_column_path(base)?;
            path.push(field.clone());
            Some(path)
        }
        _ => None,
    }
}

fn reverse_comparison(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Eq => Operator::Eq,
        Operator::NotEq => Operator::NotEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

fn column_expr(name: &str) -> Expr {
    Expr::Column(LogicalColumn::new_unqualified(name))
}

fn get_field(struct_expr: Expr, field_name: &str) -> Expr {
    Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        datafusion::functions::core::get_field(),
        vec![
            struct_expr,
            Expr::Literal(ScalarValue::Utf8(Some(field_name.to_string())), None),
        ],
    ))
}

fn literal_true() -> Expr {
    Expr::Literal(ScalarValue::Boolean(Some(true)), None)
}

fn literal_i64(value: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(Some(value)), None)
}

fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

fn and(left: Expr, right: Expr) -> Expr {
    binary(left, Operator::And, right)
}

fn or(left: Expr, right: Expr) -> Expr {
    binary(left, Operator::Or, right)
}

fn any_null(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    let checks = exprs
        .into_iter()
        .map(|expr| expr.is_null())
        .collect::<Vec<_>>();
    disjunction(checks).unwrap_or_else(literal_true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray, StructArray};
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::ToDFSchema;
    use datafusion::logical_expr::Expr;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn metadata_batch() -> Result<(SchemaRef, RecordBatch)> {
        let stats = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    STATS_FIELD_NUM_RECORDS,
                    ArrowDataType::Int64,
                    true,
                )),
                Arc::new(Int64Array::from(vec![Some(3), Some(1), Some(1)])) as Arc<_>,
            ),
            (
                Arc::new(Field::new(
                    STATS_FIELD_MIN_VALUES,
                    ArrowDataType::Struct(
                        vec![Arc::new(Field::new("value", ArrowDataType::Int32, true))].into(),
                    ),
                    true,
                )),
                Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new("value", ArrowDataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![Some(1), Some(4), None])) as Arc<_>,
                )])) as Arc<_>,
            ),
            (
                Arc::new(Field::new(
                    STATS_FIELD_MAX_VALUES,
                    ArrowDataType::Struct(
                        vec![Arc::new(Field::new("value", ArrowDataType::Int32, true))].into(),
                    ),
                    true,
                )),
                Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new("value", ArrowDataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![Some(3), Some(4), None])) as Arc<_>,
                )])) as Arc<_>,
            ),
            (
                Arc::new(Field::new(
                    STATS_FIELD_NULL_COUNT,
                    ArrowDataType::Struct(
                        vec![Arc::new(Field::new("value", ArrowDataType::Int64, true))].into(),
                    ),
                    true,
                )),
                Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new("value", ArrowDataType::Int64, true)),
                    Arc::new(Int64Array::from(vec![Some(0), Some(0), Some(1)])) as Arc<_>,
                )])) as Arc<_>,
            ),
        ]);
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("p", ArrowDataType::Utf8, true),
            Field::new(FIELD_NAME_STATS_PARSED, stats.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
                Arc::new(stats),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok((schema, batch))
    }

    #[test]
    fn rewrites_partition_predicates_to_metadata_columns() {
        let expr = binary(
            Expr::Column(LogicalColumn::new_unqualified("p")),
            Operator::Eq,
            Expr::Literal(ScalarValue::Utf8(Some("b".to_string())), None),
        );
        let rewritten = rewrite_predicate_for_metadata(
            expr.clone(),
            &["p".to_string()],
            &HashMap::new(),
            &ArrowSchema::empty(),
        );
        assert_eq!(rewritten, expr);
    }

    #[test]
    fn replay_partition_comparisons_use_logical_types_and_nulls() -> Result<()> {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "p",
                Arc::new(StringArray::from(vec![
                    Some("10"),
                    Some("2"),
                    Some(""),
                    None,
                ])) as Arc<_>,
            ),
            (
                "q",
                Arc::new(StringArray::from(vec!["2", "10", "2", "2"])) as Arc<_>,
            ),
        ])?;
        let ctx = SessionContext::new();
        let partition_columns = vec!["p".to_string(), "q".to_string()];
        for (data_type, expected) in [
            (
                ArrowDataType::Int32,
                vec![Some(true), Some(false), None, None],
            ),
            (
                ArrowDataType::Decimal128(10, 2),
                vec![Some(true), Some(false), None, None],
            ),
            (
                ArrowDataType::Utf8,
                vec![Some(false), Some(true), None, None],
            ),
        ] {
            let logical_schema = ArrowSchema::new(vec![
                Field::new("p", data_type.clone(), true),
                Field::new("q", data_type, true),
            ]);
            let predicate = Expr::Column(LogicalColumn::new_unqualified("p"))
                .gt(Expr::Column(LogicalColumn::new_unqualified("q")));
            let predicate = type_partition_columns(
                predicate,
                &partition_columns,
                &logical_schema,
                "UTC",
                true,
            )?;
            let physical = simplify_expr(&ctx.state(), &batch.schema().to_dfschema()?, predicate)?;
            let actual = physical.evaluate(&batch)?.into_array(batch.num_rows())?;
            assert_eq!(
                actual.as_ref(),
                &datafusion::arrow::array::BooleanArray::from(expected)
            );
        }
        Ok(())
    }

    /// TIMESTAMP partition values parse as the scans parse them: without an offset, as a wall
    /// clock in the session time zone, the first instant when it occurs twice, a spelling that
    /// only Arrow's cast accepts as Arrow reads it, and as NULL when they do not parse.
    #[test]
    fn replay_timestamp_partition_values_use_the_session_time_zone() -> Result<()> {
        let batch = RecordBatch::try_from_iter(vec![(
            "ts",
            Arc::new(StringArray::from(vec![
                "2024-01-01 01:00:00.000000",
                "2024-01-01T09:00:00.000000Z",
                "2024-01-01 09:00:00.000000",
                "2024-11-03 01:30:00",
                "",
                "2024-01-01t09:00:00z",
                "not a timestamp",
            ])) as Arc<_>,
        )])?;
        let logical_schema = ArrowSchema::new(vec![Field::new(
            "ts",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]);
        let instant = |micros: i64| {
            Expr::Literal(
                ScalarValue::TimestampMicrosecond(Some(micros), Some("UTC".into())),
                None,
            )
        };
        let ts = Expr::Column(LogicalColumn::new_unqualified("ts"));
        // 2024-01-01T09:00Z and 2024-11-03T08:30Z (01:30 PDT).
        let predicate = ts
            .clone()
            .eq(instant(1_704_099_600_000_000))
            .or(ts.eq(instant(1_730_622_600_000_000)));
        let predicate = type_partition_columns(
            predicate,
            &["ts".to_string()],
            &logical_schema,
            "America/Los_Angeles",
            false,
        )?;
        let ctx = SessionContext::new();
        let physical = simplify_expr(&ctx.state(), &batch.schema().to_dfschema()?, predicate)?;
        let actual = physical.evaluate(&batch)?.into_array(batch.num_rows())?;
        assert_eq!(
            actual.as_ref(),
            &datafusion::arrow::array::BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
                Some(true),
                None,
            ])
        );
        Ok(())
    }

    /// Partition values that do not parse are NULL, as the scans read them, and an error in ANSI
    /// mode, for every row and whatever the rest of the predicate selects.
    #[test]
    fn replay_partition_values_that_do_not_parse_follow_ansi_mode() -> Result<()> {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "n",
                Arc::new(StringArray::from(vec!["1", "abc", ""])) as Arc<_>,
            ),
            (
                "d",
                Arc::new(StringArray::from(vec!["2024-02-28", "2024-02-30", ""])) as Arc<_>,
            ),
            (
                "ts",
                Arc::new(StringArray::from(vec![
                    "2024-01-01 09:00:00",
                    "2024-01-01T09:00:00 Q",
                    "",
                ])) as Arc<_>,
            ),
        ])?;
        let logical_schema = ArrowSchema::new(vec![
            Field::new("n", ArrowDataType::Int32, true),
            Field::new("d", ArrowDataType::Date32, true),
            Field::new(
                "ts",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let partition_columns = ["n".to_string(), "d".to_string(), "ts".to_string()];
        let ctx = SessionContext::new();
        let evaluate = |predicate: Expr, ansi_mode: bool| -> Result<Arc<dyn Array>> {
            let predicate = type_partition_columns(
                predicate,
                &partition_columns,
                &logical_schema,
                "UTC",
                ansi_mode,
            )?;
            let physical = simplify_expr(&ctx.state(), &batch.schema().to_dfschema()?, predicate)?;
            physical.evaluate(&batch)?.into_array(batch.num_rows())
        };
        for column in ["n", "d", "ts"] {
            let column = Expr::Column(LogicalColumn::new_unqualified(column));
            assert_eq!(
                evaluate(column.clone().is_null(), false)?.as_ref(),
                &datafusion::arrow::array::BooleanArray::from(vec![false, true, true]),
                "{column}"
            );
            assert!(
                evaluate(column.clone().is_null(), true).is_err(),
                "{column}"
            );
        }
        Ok(())
    }

    #[test]
    fn mixed_metadata_predicates_preserve_exact_partition_subexpressions() {
        let p = Expr::Column(LogicalColumn::new_unqualified("p"));
        let q = Expr::Column(LogicalColumn::new_unqualified("q"));
        let value = Expr::Column(LogicalColumn::new_unqualified("value"));
        let exact = p.gt(q);
        // A data-column comparison has no usable bounds; its OR must retain every file.
        let unsupported = value.clone().gt(value);
        for op in [Operator::And, Operator::Or] {
            let actual = rewrite_predicate_for_metadata(
                binary(exact.clone(), op, unsupported.clone()),
                &["p".to_string(), "q".to_string()],
                &HashMap::new(),
                &ArrowSchema::empty(),
            );
            assert_eq!(actual, binary(exact.clone(), op, literal_true()));
        }
    }

    /// A cast that only relabels a TIMESTAMP column keeps the order of the values, so file bounds
    /// are compared through it. Any other cast keeps every file.
    #[test]
    fn stats_comparisons_go_through_timestamp_relabels_only() {
        let timestamp =
            |zone: &str| ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(zone.into()));
        let schema = ArrowSchema::new(vec![
            Field::new("ts", timestamp("UTC"), true),
            Field::new("s", ArrowDataType::Utf8, true),
        ]);
        let literal = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(0), Some("America/Los_Angeles".into())),
            None,
        );
        for (column, pruned) in [("ts", true), ("s", false)] {
            let expr = Expr::Cast(Cast::new(
                Box::new(Expr::Column(LogicalColumn::new_unqualified(column))),
                timestamp("America/Los_Angeles"),
            ))
            .gt(literal.clone());
            let rewritten = rewrite_predicate_for_metadata(expr, &[], &HashMap::new(), &schema);
            assert_eq!(rewritten != literal_true(), pruned, "{column}");
        }
    }

    #[test]
    fn rewritten_stats_predicate_filters_against_bounds() -> Result<()> {
        let (schema, batch) = metadata_batch()?;
        let expr = binary(
            Expr::Column(LogicalColumn::new_unqualified("value")),
            Operator::Gt,
            literal_i64(3),
        );
        let rewritten = rewrite_predicate_for_metadata(
            expr,
            &["p".to_string()],
            &HashMap::new(),
            &ArrowSchema::empty(),
        );
        let ctx = SessionContext::new();
        let physical = simplify_expr(&ctx.state(), &schema.to_dfschema()?, rewritten)?;
        let values = physical.evaluate(&batch)?.into_array(batch.num_rows())?;
        let values = values
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected boolean predicate output".to_string())
            })?;

        assert!(!values.value(0));
        assert!(values.value(1));
        assert!(values.value(2));
        Ok(())
    }
}
