use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::Session;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{BinaryExpr, Expr, Operator, lit};

use crate::datasource::pruning::prune_files;
use crate::spec::transform::Transform;
use crate::spec::types::{PrimitiveType, Type};
use crate::spec::{DataFile, Datum, Literal, PartitionSpec, Schema};

pub(crate) struct CopyOnWriteFileSelection {
    pub candidates: Vec<(DataFile, i64)>,
    pub all_rows_match: bool,
}

/// Select whole files. Row predicates must remain above the scan so that
/// survivors and original positions are available to the rewrite.
pub(crate) fn select_copy_on_write_files(
    session: &dyn Session,
    predicate: &Expr,
    arrow_schema: Arc<ArrowSchema>,
    schema: &Schema,
    specs: &[PartitionSpec],
    files: Vec<(DataFile, i64)>,
) -> Result<CopyOnWriteFileSelection> {
    let statistics = files
        .iter()
        .map(|(file, _)| file_statistics(file, schema, specs))
        .collect::<Vec<_>>();
    let filters = split_conjunction(predicate)
        .into_iter()
        .filter(|expr| can_prune(expr))
        .cloned()
        .collect::<Vec<_>>();
    let (candidates, _) = prune_files(
        session,
        &filters,
        None,
        Arc::clone(&arrow_schema),
        statistics,
        schema,
    )?;
    let (possible_survivors, _) = prune_files(
        session,
        &[non_matching_rows(predicate)],
        None,
        arrow_schema,
        candidates.clone(),
        schema,
    )?;
    let paths = candidates
        .iter()
        .map(|file| file.file_path.as_str())
        .collect::<HashSet<_>>();
    Ok(CopyOnWriteFileSelection {
        candidates: files
            .into_iter()
            .filter(|(file, _)| paths.contains(file.file_path.as_str()))
            .collect(),
        all_rows_match: possible_survivors.is_empty(),
    })
}

fn file_statistics(file: &DataFile, schema: &Schema, specs: &[PartitionSpec]) -> DataFile {
    let mut statistics = file.clone();
    // Bounds omit NaNs, so they cannot bound a floating column unless the
    // file explicitly establishes that no NaNs occur.
    for field in schema.fields() {
        if matches!(
            field.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Float | PrimitiveType::Double)
        ) && file.nan_value_counts.get(&field.id) != Some(&0)
        {
            statistics.lower_bounds.remove(&field.id);
            statistics.upper_bounds.remove(&field.id);
        }
    }
    if let Some(spec) = specs
        .iter()
        .find(|spec| spec.spec_id() == file.partition_spec_id)
    {
        for (partition, value) in spec.fields().iter().zip(&file.partition) {
            if partition.transform != Transform::Identity {
                continue;
            }
            let Some(field) = schema.field_by_id(partition.source_id) else {
                continue;
            };
            let Type::Primitive(primitive) = field.field_type.as_ref() else {
                continue;
            };
            if matches!(primitive, PrimitiveType::Float | PrimitiveType::Double) {
                continue;
            }
            match value {
                Some(Literal::Primitive(value)) => {
                    let datum = Datum {
                        r#type: primitive.clone(),
                        literal: value.clone(),
                    };
                    statistics.lower_bounds.insert(field.id, datum.clone());
                    statistics.upper_bounds.insert(field.id, datum);
                    statistics.null_value_counts.insert(field.id, 0);
                }
                None => {
                    statistics.lower_bounds.remove(&field.id);
                    statistics.upper_bounds.remove(&field.id);
                    statistics
                        .null_value_counts
                        .insert(field.id, file.record_count);
                }
                _ => {}
            }
        }
    }
    statistics
}

fn can_prune(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_, _) => true,
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And | Operator::Or,
            right,
        }) => can_prune(left) && can_prune(right),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if op.negate().is_some() => {
            matches!(
                (left.as_ref(), right.as_ref()),
                (Expr::Column(_), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(_))
            )
        }
        Expr::IsNull(value) | Expr::IsNotNull(value) => matches!(value.as_ref(), Expr::Column(_)),
        Expr::InList(list) => {
            matches!(list.expr.as_ref(), Expr::Column(_))
                && list
                    .list
                    .iter()
                    .all(|value| matches!(value, Expr::Literal(_, _)))
        }
        _ => false,
    }
}

/// An inclusive predicate for rows that are FALSE or UNKNOWN. Pruning every
/// such row proves a metadata-only delete; missing evidence must keep files.
fn non_matching_rows(expr: &Expr) -> Expr {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(value)), _) => lit(!value),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => non_matching_rows(left).or(non_matching_rows(right)),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => non_matching_rows(left).and(non_matching_rows(right)),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if can_prune(expr) => {
            let (column, literal) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(value, _)) => (left, value),
                (Expr::Literal(value, _), Expr::Column(_)) => (right, value),
                _ => return lit(true),
            };
            if literal.is_null() {
                return lit(true);
            }
            if let Some(op) = op.negate() {
                Expr::BinaryExpr(BinaryExpr::new(left.clone(), op, right.clone()))
                    .or(column.as_ref().clone().is_null())
            } else {
                lit(true)
            }
        }
        Expr::IsNull(value) if can_prune(expr) => value.as_ref().clone().is_not_null(),
        Expr::IsNotNull(value) if can_prune(expr) => value.as_ref().clone().is_null(),
        Expr::InList(list) if can_prune(expr) => {
            let comparisons = list.list.iter().map(|value| {
                let comparison = if list.negated {
                    list.expr.as_ref().clone().not_eq(value.clone())
                } else {
                    list.expr.as_ref().clone().eq(value.clone())
                };
                non_matching_rows(&comparison)
            });
            if list.negated {
                comparisons.fold(lit(false), Expr::or)
            } else {
                comparisons.fold(lit(true), Expr::and)
            }
        }
        _ => lit(true),
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Int32Array, as_boolean_array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ToDFSchema;
    use datafusion_expr::col;

    use super::*;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::spec::{DataContentType, DataFileFormat, NestedField, PrimitiveLiteral};

    fn data_file(path: &str, lower: i32, upper: i32) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition: vec![],
            record_count: 2,
            file_size_in_bytes: 100,
            column_sizes: HashMap::new(),
            value_counts: HashMap::from([(1, 2)]),
            null_value_counts: HashMap::from([(1, 0)]),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::from([(
                1,
                Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(lower)),
            )]),
            upper_bounds: HashMap::from([(
                1,
                Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(upper)),
            )]),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: vec![],
            equality_ids: vec![],
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: 0,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn select(
        predicate: Expr,
        schema: &Schema,
        specs: &[PartitionSpec],
        files: Vec<DataFile>,
    ) -> Result<CopyOnWriteFileSelection> {
        select_copy_on_write_files(
            &SessionContext::new().state(),
            &predicate,
            Arc::new(iceberg_schema_to_arrow(schema)?),
            schema,
            specs,
            files.into_iter().map(|file| (file, 7)).collect(),
        )
    }

    fn integer_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("schema")
    }

    #[test]
    fn candidates_retain_complete_files_and_metadata_requires_every_row() -> Result<()> {
        for (predicate, expected_paths, all_rows_match) in [
            (col("id").eq(lit(1i32)), vec!["a"], false),
            (col("id").lt(lit(3i32)), vec!["a"], true),
            (col("id").eq(lit(99i32)), vec![], true),
            (
                col("id").lt(lit(3i32)).or(col("id").gt(lit(8i32))),
                vec!["a", "b"],
                true,
            ),
            (
                col("id").in_list(vec![lit(1i32), lit(2i32)], false),
                vec!["a"],
                false,
            ),
        ] {
            let selected = select(
                predicate.clone(),
                &integer_schema(),
                &[],
                vec![data_file("a", 1, 2), data_file("b", 9, 10)],
            )?;
            assert_eq!(
                selected
                    .candidates
                    .iter()
                    .map(|(file, sequence)| {
                        assert_eq!(*sequence, 7);
                        assert_eq!(file.record_count, 2);
                        file.file_path.as_str()
                    })
                    .collect::<Vec<_>>(),
                expected_paths
            );
            assert_eq!(selected.all_rows_match, all_rows_match, "{predicate:?}");
        }
        Ok(())
    }

    #[test]
    fn null_and_missing_statistics_cannot_prove_a_complete_match() -> Result<()> {
        for null_count in [None, Some(1)] {
            let mut file = data_file("a", 1, 2);
            file.null_value_counts = null_count
                .map(|count| HashMap::from([(1, count)]))
                .unwrap_or_default();
            let selected = select(col("id").lt(lit(3i32)), &integer_schema(), &[], vec![file])?;
            assert_eq!(selected.candidates.len(), 1);
            assert!(!selected.all_rows_match);
        }
        let mut file = data_file("a", 1, 2);
        file.lower_bounds.clear();
        file.upper_bounds.clear();
        let selected = select(col("id").lt(lit(3i32)), &integer_schema(), &[], vec![file])?;
        assert_eq!(selected.candidates.len(), 1);
        assert!(!selected.all_rows_match);
        Ok(())
    }

    #[test]
    fn file_selection_is_sound_against_row_evaluation_with_nulls() -> Result<()> {
        let schema = integer_schema();
        let arrow_schema = Arc::new(iceberg_schema_to_arrow(&schema)?);
        let df_schema = Arc::clone(&arrow_schema).to_dfschema()?;
        let session = SessionContext::new().state();
        let null = lit(ScalarValue::Int32(None));
        let predicates = [
            col("id").eq(lit(1i32)),
            col("id").not_eq(lit(1i32)),
            col("id").lt_eq(lit(1i32)),
            lit(1i32).lt(col("id")),
            col("id").eq(null.clone()),
            col("id").is_null(),
            col("id").is_not_null(),
            col("id").lt(lit(2i32)).and(col("id").gt(lit(0i32))),
            col("id").lt(lit(2i32)).or(col("id").is_null()),
            col("id").in_list(vec![lit(1i32), lit(2i32)], false),
            col("id").in_list(vec![lit(1i32), lit(2i32)], true),
            col("id").in_list(vec![lit(1i32), null.clone()], false),
            col("id").in_list(vec![lit(1i32), null], true),
        ];
        for left in [None, Some(0), Some(1), Some(2)] {
            for right in [None, Some(0), Some(1), Some(2)] {
                let values = [left, right];
                let batch = RecordBatch::try_new(
                    Arc::clone(&arrow_schema),
                    vec![Arc::new(Int32Array::from(values.to_vec()))],
                )?;
                let mut file = data_file("a", 0, 2);
                file.lower_bounds.clear();
                file.upper_bounds.clear();
                if let Some(min) = values.into_iter().flatten().min() {
                    file.lower_bounds.insert(
                        1,
                        Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(min)),
                    );
                }
                if let Some(max) = values.into_iter().flatten().max() {
                    file.upper_bounds.insert(
                        1,
                        Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(max)),
                    );
                }
                file.null_value_counts.insert(
                    1,
                    values.iter().filter(|value| value.is_none()).count() as u64,
                );
                for predicate in &predicates {
                    let evaluated = session
                        .create_physical_expr(predicate.clone(), &df_schema)?
                        .evaluate(&batch)?
                        .into_array(batch.num_rows())?;
                    let rows = as_boolean_array(evaluated.as_ref());
                    let selected = select_copy_on_write_files(
                        &session,
                        predicate,
                        Arc::clone(&arrow_schema),
                        &schema,
                        &[],
                        vec![(file.clone(), 7)],
                    )?;
                    if selected.candidates.is_empty() {
                        assert!(
                            rows.iter().all(|value| value != Some(true)),
                            "pruned matching rows: {predicate:?}, {values:?}"
                        );
                    } else if selected.all_rows_match {
                        assert!(
                            rows.iter().all(|value| value == Some(true)),
                            "removed surviving rows: {predicate:?}, {values:?}"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn null_predicates_and_unknown_expressions_are_conservative() -> Result<()> {
        let mut file = data_file("a", 1, 2);
        file.null_value_counts.insert(1, 2);
        file.lower_bounds.clear();
        file.upper_bounds.clear();
        assert!(
            select(
                col("id").is_null(),
                &integer_schema(),
                &[],
                vec![file.clone()]
            )?
            .all_rows_match
        );
        assert!(
            select(
                col("id").is_not_null(),
                &integer_schema(),
                &[],
                vec![file.clone()]
            )?
            .candidates
            .is_empty()
        );
        assert!(
            !select(
                (col("id") + lit(1i32)).lt(lit(3i32)),
                &integer_schema(),
                &[],
                vec![file]
            )?
            .all_rows_match
        );
        Ok(())
    }

    #[test]
    fn nan_counts_are_required_before_using_floating_bounds() -> Result<()> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Double)).into(),
            ])
            .build()
            .expect("schema");
        for count in [None, Some(1), Some(0)] {
            let mut file = data_file("a", 1, 2);
            file.lower_bounds.insert(
                1,
                Datum::new(PrimitiveType::Double, PrimitiveLiteral::Double(1.0.into())),
            );
            file.upper_bounds.insert(
                1,
                Datum::new(PrimitiveType::Double, PrimitiveLiteral::Double(2.0.into())),
            );
            file.nan_value_counts = count
                .map(|count| HashMap::from([(1, count)]))
                .unwrap_or_default();
            let selected = select(col("id").lt(lit(3.0f64)), &schema, &[], vec![file])?;
            assert_eq!(selected.candidates.len(), 1);
            assert_eq!(selected.all_rows_match, count == Some(0));
        }
        Ok(())
    }

    #[test]
    fn identity_partitions_supply_exact_statistics_for_each_files_spec() -> Result<()> {
        let specs = [PartitionSpec::builder()
            .with_spec_id(7)
            .add_field_with_id(1, 1001, "original_id", Transform::Identity)
            .build()];
        let mut file = data_file("a", 1, 2);
        file.partition_spec_id = 7;
        file.partition = vec![Some(Literal::Primitive(PrimitiveLiteral::Int(10)))];
        file.lower_bounds.clear();
        file.upper_bounds.clear();
        file.null_value_counts.clear();
        let selected = select(
            col("id").eq(lit(10i32)),
            &integer_schema(),
            &specs,
            vec![file.clone()],
        )?;
        assert!(selected.all_rows_match);
        assert_eq!(selected.candidates, vec![(file.clone(), 7)]);
        assert!(
            select(
                col("id").eq(lit(20i32)),
                &integer_schema(),
                &specs,
                vec![file.clone()]
            )?
            .candidates
            .is_empty()
        );
        file.partition = vec![None];
        assert!(
            select(
                col("id").is_null(),
                &integer_schema(),
                &specs,
                vec![file.clone()]
            )?
            .all_rows_match
        );
        file.partition_spec_id = 8;
        assert!(
            !select(col("id").is_null(), &integer_schema(), &specs, vec![file])?.all_rows_match
        );
        Ok(())
    }
}
