use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterTracking;
use datafusion::physical_plan::metrics::Count;
use datafusion_common::{Result, internal_datafusion_err};
use serde::{Deserialize, Serialize};

use crate::datasource::predicate::Predicate;
use crate::spec::{DataFile, PartitionSpec, Schema};

/// Source facts for pruning, kept separate from optimizer statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FilePruning {
    pub schema: Schema,
    pub spec: Option<PartitionSpec>,
    #[serde(with = "file_serde")]
    pub file: DataFile,
}

impl FilePruning {
    pub fn new(schema: &Schema, specs: &[PartitionSpec], file: &DataFile) -> Self {
        Self {
            schema: schema.clone(),
            spec: specs
                .iter()
                .find(|spec| spec.spec_id() == file.partition_spec_id)
                .cloned(),
            file: file.clone(),
        }
    }
}

mod file_serde {
    use std::collections::HashMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use crate::spec::{DataFile, Datum, Literal, PrimitiveLiteral, PrimitiveType};

    #[derive(Serialize, Deserialize)]
    struct Value(#[serde(with = "crate::utils::literal_serde")] PrimitiveLiteral);

    #[derive(Serialize, Deserialize)]
    struct Bound {
        r#type: PrimitiveType,
        value: Value,
    }

    #[derive(Serialize, Deserialize)]
    struct EncodedFile {
        data_file: DataFile,
        partition: Vec<Option<Value>>,
        lower_bounds: HashMap<i32, Bound>,
        upper_bounds: HashMap<i32, Bound>,
    }

    pub fn serialize<S: Serializer>(file: &DataFile, serializer: S) -> Result<S::Ok, S::Error> {
        let mut data_file = file.clone();
        // DataFile's untagged literals lose NaNs and numeric types in JSON.
        let partition = std::mem::take(&mut data_file.partition)
            .into_iter()
            .map(|value| match value {
                None | Some(Literal::Null) => Ok(None),
                Some(Literal::Primitive(value)) => Ok(Some(Value(value))),
                Some(_) => Err(serde::ser::Error::custom("non-primitive partition value")),
            })
            .collect::<Result<_, S::Error>>()?;
        let bounds = |values: HashMap<i32, Datum>| {
            values
                .into_iter()
                .map(|(id, datum)| {
                    (
                        id,
                        Bound {
                            r#type: datum.r#type,
                            value: Value(datum.literal),
                        },
                    )
                })
                .collect()
        };
        let lower_bounds = bounds(std::mem::take(&mut data_file.lower_bounds));
        let upper_bounds = bounds(std::mem::take(&mut data_file.upper_bounds));
        EncodedFile {
            data_file,
            partition,
            lower_bounds,
            upper_bounds,
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<DataFile, D::Error> {
        let EncodedFile {
            mut data_file,
            partition,
            lower_bounds,
            upper_bounds,
        } = EncodedFile::deserialize(deserializer)?;
        data_file.partition = partition
            .into_iter()
            .map(|value| value.map(|value| Literal::Primitive(value.0)))
            .collect();
        let bounds = |values: HashMap<i32, Bound>| {
            values
                .into_iter()
                .map(|(id, bound)| (id, Datum::new(bound.r#type, bound.value.0)))
                .collect()
        };
        data_file.lower_bounds = bounds(lower_bounds);
        data_file.upper_bounds = bounds(upper_bounds);
        Ok(data_file)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IcebergFilePruner(Arc<Mutex<PruningState>>);

#[derive(Debug)]
struct PruningState {
    predicate: Arc<dyn PhysicalExpr>,
    arrow_schema: SchemaRef,
    facts: FilePruning,
    tracking: DynamicFilterTracking,
    pruned: Option<bool>,
    metric: Count,
}

impl IcebergFilePruner {
    pub fn new(
        predicate: Arc<dyn PhysicalExpr>,
        arrow_schema: SchemaRef,
        facts: FilePruning,
        metric: Count,
    ) -> Self {
        let tracking = DynamicFilterTracking::classify(&predicate);
        Self(Arc::new(Mutex::new(PruningState {
            predicate,
            arrow_schema,
            facts,
            tracking,
            pruned: None,
            metric,
        })))
    }

    pub fn should_prune(&self) -> Result<bool> {
        let mut state = self
            .0
            .lock()
            .map_err(|error| internal_datafusion_err!("Iceberg pruning state: {error}"))?;
        if state.pruned == Some(true) {
            return Ok(true);
        }
        if state.pruned.is_none()
            || state
                .tracking
                .watcher()
                .is_some_and(|watcher| watcher.changed())
        {
            let predicate =
                Predicate::physical(&state.facts.schema, &state.arrow_schema, &state.predicate);
            let prune = !predicate
                .file(&state.facts.file, state.facts.spec.as_ref())
                .may_match();
            if prune {
                state.metric.add(1);
            }
            state.pruned = Some(prune);
        }
        Ok(state.pruned.unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, binary, lit};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
    use datafusion_common::{ScalarValue, plan_datafusion_err};

    use super::*;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::spec::{
        Datum, Literal, NestedField, PrimitiveLiteral, PrimitiveType, Transform, Type,
    };
    use crate::utils::transform::apply_transform;

    fn missing_data_file() -> Result<DataFile> {
        serde_json::from_value(serde_json::json!({
            "content": "DATA",
            "file_path": "missing.parquet",
            "file_format": "PARQUET",
            "partition": [],
            "record_count": 2,
            "file_size_in_bytes": 100,
            "partition_spec_id": 0
        }))
        .map_err(|error| plan_datafusion_err!("{error}"))
    }

    #[test]
    fn file_facts_preserve_literal_types_and_non_finite_values() -> Result<()> {
        use PrimitiveLiteral as L;
        use PrimitiveType as T;

        let values = [
            (T::Boolean, L::Boolean(true)),
            (T::Int, L::Int(1)),
            (T::Long, L::Long(1)),
            (T::Float, L::Float(f32::from_bits(0x7fc0_0001).into())),
            (T::Float, L::Float(f32::INFINITY.into())),
            (T::Double, L::Double(f64::NAN.into())),
            (T::Double, L::Double(f64::NEG_INFINITY.into())),
            (T::Double, L::Double(1.0000000000000002.into())),
            (T::Double, L::Double((-0.0).into())),
            (
                T::Decimal {
                    precision: 38,
                    scale: 0,
                },
                L::Int128(10_i128.pow(37) + 1),
            ),
            (T::String, L::String("世界".into())),
            (T::Uuid, L::UInt128(u128::MAX)),
            (T::Binary, L::Binary(vec![0, 128, 255])),
        ];
        let schema = Schema::builder()
            .with_fields(values.iter().enumerate().map(|(index, (primitive, _))| {
                Arc::new(NestedField::optional(
                    index as i32 + 1,
                    format!("v{index}"),
                    Type::Primitive(primitive.clone()),
                ))
            }))
            .build()
            .map_err(|error| plan_datafusion_err!("{error}"))?;
        let mut file = missing_data_file()?;
        for (index, (primitive, value)) in values.iter().enumerate() {
            file.partition.push(Some(Literal::Primitive(value.clone())));
            file.lower_bounds.insert(
                index as i32 + 1,
                Datum::new(primitive.clone(), value.clone()),
            );
        }
        file.partition.push(None);
        file.upper_bounds = file.lower_bounds.clone();
        let facts = FilePruning::new(&schema, &[], &file);
        let encoded =
            serde_json::to_vec(&facts).map_err(|error| plan_datafusion_err!("{error}"))?;
        let decoded: FilePruning =
            serde_json::from_slice(&encoded).map_err(|error| plan_datafusion_err!("{error}"))?;
        assert_eq!(decoded.file, file);
        for (before, after) in file.partition.iter().zip(&decoded.file.partition) {
            match (before, after) {
                (Some(Literal::Primitive(L::Float(a))), Some(Literal::Primitive(L::Float(b)))) => {
                    assert_eq!(a.0.to_bits(), b.0.to_bits())
                }
                (
                    Some(Literal::Primitive(L::Double(a))),
                    Some(Literal::Primitive(L::Double(b))),
                ) => assert_eq!(a.0.to_bits(), b.0.to_bits()),
                _ => {}
            }
        }
        Ok(())
    }

    #[test]
    fn dynamic_partition_projection_observes_updates_and_keeps_collisions() -> Result<()> {
        let schema = Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                7,
                "key",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .map_err(|error| plan_datafusion_err!("{error}"))?;
        let arrow = Arc::new(iceberg_schema_to_arrow(&schema)?);
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        for transform in [
            Transform::Identity,
            Transform::Bucket(16),
            Transform::Truncate(10),
        ] {
            let spec = PartitionSpec::builder()
                .add_field_with_id(7, 1000, "partition", transform)
                .build();
            for (key, selected) in [(-21, -11), (-5, -4), (1, 2), (0, 0)] {
                let partition = |key| {
                    apply_transform(
                        transform,
                        &Type::Primitive(PrimitiveType::Int),
                        Some(Literal::Primitive(PrimitiveLiteral::Int(key))),
                    )
                };
                let mut file = missing_data_file()?;
                file.partition = vec![partition(key)];
                let facts = FilePruning::new(&schema, std::slice::from_ref(&spec), &file);
                // The same facts travel through both streaming batches and the plan codec.
                let facts = serde_json::from_slice(
                    &serde_json::to_vec(&facts).map_err(|error| plan_datafusion_err!("{error}"))?,
                )
                .map_err(|error| plan_datafusion_err!("{error}"))?;
                let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
                    vec![column.clone()],
                    lit(true),
                ));
                let metric =
                    MetricBuilder::new(&ExecutionPlanMetricsSet::new()).counter("pruned", 0);
                let pruner =
                    IcebergFilePruner::new(dynamic.clone(), arrow.clone(), facts, metric.clone());
                assert!(!pruner.should_prune()?);
                dynamic.update(binary(column.clone(), Operator::Eq, lit(selected), &arrow)?)?;
                dynamic.mark_complete();
                let expected = partition(key) != partition(selected);
                assert_eq!(
                    pruner.should_prune()?,
                    expected,
                    "{transform}: {key} = {selected}"
                );
                assert_eq!(pruner.should_prune()?, expected);
                assert_eq!(metric.value(), usize::from(expected));
            }
        }
        // Unsupported arithmetic in an OR must not turn into a false proof.
        let unknown = binary(
            binary(column.clone(), Operator::Plus, lit(1), &arrow)?,
            Operator::Eq,
            lit(100),
            &arrow,
        )?;
        let expr = binary(
            binary(column, Operator::Eq, lit(100), &arrow)?,
            Operator::Or,
            unknown,
            &arrow,
        )?;
        let mut file = missing_data_file()?;
        let bound = Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(99));
        file.lower_bounds.insert(7, bound.clone());
        file.upper_bounds.insert(7, bound);
        file.null_value_counts.insert(7, 0);
        assert!(
            Predicate::physical(&schema, &arrow, &expr)
                .file(&file, None)
                .may_match()
        );
        assert!(
            !Predicate::physical(&schema, &arrow, &lit(ScalarValue::Boolean(None)))
                .file(&file, None)
                .may_match()
        );
        Ok(())
    }
}
