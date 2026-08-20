use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{
    Column as LogicalColumn, DataFusionError, Result, ScalarValue, ToDFSchema,
};
use datafusion::functions::core::getfield::GetFieldFunc;
use datafusion::logical_expr::expr::{Between, BinaryExpr, Cast, InList};
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;

use crate::datasource::simplify_expr;
use crate::physical_plan::DeltaMetadataStatsExec;
use crate::schema::{logical_to_physical_arrow_paths, make_physical_arrow_schema};
use crate::spec::fields::{
    FIELD_NAME_STATS_PARSED, STATS_FIELD_MAX_VALUES, STATS_FIELD_MIN_VALUES,
    STATS_FIELD_NULL_COUNT, STATS_FIELD_NUM_RECORDS,
};
use crate::spec::{DataSkippingNumIndexedCols, StructType, stats_schema};
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
    predicate: Expr,
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_columns = snapshot.metadata().partition_columns().clone();
    let needs_stats = predicate_requires_stats(&predicate, &partition_columns);
    let stats_paths = logical_to_physical_arrow_paths(
        snapshot.schema(),
        snapshot.effective_column_mapping_mode(),
    );
    let stats_schema = needs_stats
        .then(|| build_metadata_stats_schema(snapshot, &predicate))
        .transpose()?;
    let rewritten = rewrite_predicate_for_metadata(predicate, &partition_columns, &stats_paths);
    if !needs_stats {
        let df_schema = input.schema().to_dfschema()?;
        let physical_expr = simplify_expr(session, &df_schema, rewritten)?;
        return Ok(Arc::new(FilterExec::try_new(physical_expr, input)?));
    }

    let input: Arc<dyn ExecutionPlan> = Arc::new(DeltaMetadataStatsExec::new(
        input,
        stats_schema.ok_or_else(|| {
            DataFusionError::Internal("metadata stats schema was not built".to_string())
        })?,
    ));
    let df_schema = input.schema().to_dfschema()?;
    let physical_expr = simplify_expr(session, &df_schema, rewritten)?;
    Ok(Arc::new(FilterExec::try_new(physical_expr, input)?))
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
) -> Expr {
    let partition_columns = partition_columns.iter().cloned().collect::<HashSet<_>>();
    MetadataPredicateRewriter {
        partition_columns,
        stats_paths,
    }
    .rewrite(expr)
}

struct MetadataPredicateRewriter<'a> {
    partition_columns: HashSet<String>,
    stats_paths: &'a HashMap<Vec<String>, Vec<String>>,
}

#[derive(Clone)]
struct ExprTemplate {
    logical_path: Vec<String>,
    cast_type: Option<ArrowDataType>,
}

impl MetadataPredicateRewriter<'_> {
    fn rewrite(&self, expr: Expr) -> Expr {
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

        if let Some(template) = Self::extract_template(&left) {
            if !right.column_refs().is_empty() {
                return None;
            }
            return Some(self.rewrite_template_comparison(template, op, right));
        }

        if let Some(template) = Self::extract_template(&right) {
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
        let template = Self::extract_template(&expr)?;
        if self.is_partition_column(&template) {
            return Some(Expr::Between(Between::new(
                Box::new(expr),
                negated,
                Box::new(low),
                Box::new(high),
            )));
        }
        if template.cast_type.is_some() {
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
        let template = Self::extract_template(&expr)?;
        if list.iter().any(|expr| !expr.column_refs().is_empty()) {
            return None;
        }
        if self.is_partition_column(&template) {
            return Some(Expr::InList(InList::new(Box::new(expr), list, negated)));
        }
        if template.cast_type.is_some() {
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
        let template = Self::extract_template(&expr)?;
        if self.is_partition_column(&template) {
            return Some(if is_not_null {
                Expr::IsNotNull(Box::new(expr))
            } else {
                Expr::IsNull(Box::new(expr))
            });
        }
        if template.cast_type.is_some() {
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
        if template.cast_type.is_some() {
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

    fn extract_template(expr: &Expr) -> Option<ExprTemplate> {
        match expr {
            Expr::Cast(Cast { expr, field }) => Some(ExprTemplate {
                logical_path: extract_column_path(expr)?,
                cast_type: Some(field.data_type().clone()),
            }),
            _ => Some(ExprTemplate {
                logical_path: extract_column_path(expr)?,
                cast_type: None,
            }),
        }
    }
}

impl ExprTemplate {
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
    use datafusion::arrow::datatypes::Field;
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
        let rewritten =
            rewrite_predicate_for_metadata(expr.clone(), &["p".to_string()], &HashMap::new());
        assert_eq!(rewritten, expr);
    }

    #[test]
    fn rewritten_stats_predicate_filters_against_bounds() -> Result<()> {
        let (schema, batch) = metadata_batch()?;
        let expr = binary(
            Expr::Column(LogicalColumn::new_unqualified("value")),
            Operator::Gt,
            literal_i64(3),
        );
        let rewritten = rewrite_predicate_for_metadata(expr, &["p".to_string()], &HashMap::new());
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
