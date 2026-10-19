use datafusion_expr::Expr;

use crate::datasource::predicate::Predicate;
use crate::spec::{DataFile, PartitionSpec, Schema};

pub(crate) struct CopyOnWriteFileSelection {
    pub candidates: Vec<(DataFile, i64)>,
    pub all_rows_match: bool,
}

/// Rewrites need every surviving row of a candidate file. Metadata removal
/// requires proof that neither FALSE nor NULL is possible in any candidate.
pub(crate) fn select_copy_on_write_files(
    predicate: &Expr,
    schema: &Schema,
    specs: &[PartitionSpec],
    files: Vec<(DataFile, i64)>,
) -> CopyOnWriteFileSelection {
    let predicate = Predicate::new(schema, predicate);
    let mut all_rows_match = true;
    let candidates = files
        .into_iter()
        .filter(|(file, _)| {
            let spec = specs
                .iter()
                .find(|spec| spec.spec_id() == file.partition_spec_id);
            let truth = predicate.file(file, spec);
            if !truth.may_match() {
                return false;
            }
            all_rows_match &= truth.all_match();
            true
        })
        .collect();
    CopyOnWriteFileSelection {
        candidates,
        all_rows_match,
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, as_boolean_array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{Result, ScalarValue, ToDFSchema};
    use datafusion_expr::{col, lit};

    use super::*;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::spec::{
        DataContentType, DataFileFormat, Datum, Literal, NestedField, PrimitiveLiteral,
        PrimitiveType, Transform, Type,
    };

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
        Ok(select_copy_on_write_files(
            &predicate,
            schema,
            specs,
            files.into_iter().map(|file| (file, 7)).collect(),
        ))
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
                        predicate,
                        &schema,
                        &[],
                        vec![(file.clone(), 7)],
                    );
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
