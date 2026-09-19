use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::ScalarValue;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    LexOrdering, PhysicalExpr, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion::physical_plan::Partitioning;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err, plan_datafusion_err};

use crate::datasource::type_converter::iceberg_type_to_arrow;
use crate::physical_plan::partition_transform_expr::IcebergPartitionTransformExpr;
use crate::spec::transform::Transform;
use crate::spec::types::{PrimitiveType, Type};
use crate::spec::{DataFile, PartitionSpec, Schema};
use crate::utils::conversions::to_scalar;

#[derive(Debug, Clone)]
pub(crate) struct IcebergScanRangeLayout {
    range: RangePartitioning,
    file_partitions: HashMap<String, usize>,
}

impl IcebergScanRangeLayout {
    pub(crate) fn try_new(
        schema: &Schema,
        arrow_schema: &ArrowSchema,
        partition_specs: &[PartitionSpec],
        data_files: &[DataFile],
        target_partitions: usize,
    ) -> Result<Option<Self>> {
        let Some(first_file) = data_files.first() else {
            return Ok(None);
        };
        let partition_specs = partition_specs
            .iter()
            .map(|spec| (spec.spec_id(), spec))
            .collect::<HashMap<_, _>>();
        let reference_spec = partition_specs
            .get(&first_file.partition_spec_id)
            .copied()
            .ok_or_else(|| {
                plan_datafusion_err!(
                    "Iceberg data file {} references missing partition spec {}",
                    first_file.file_path,
                    first_file.partition_spec_id
                )
            })?;

        for data_file in data_files {
            let file_spec = partition_specs
                .get(&data_file.partition_spec_id)
                .copied()
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "Iceberg data file {} references missing partition spec {}",
                        data_file.file_path,
                        data_file.partition_spec_id
                    )
                })?;
            if data_file.partition().len() != file_spec.fields().len() {
                return Err(plan_datafusion_err!(
                    "Iceberg data file {} has {} partition values for spec {} with {} fields",
                    data_file.file_path,
                    data_file.partition().len(),
                    file_spec.spec_id(),
                    file_spec.fields().len()
                ));
            }
            if !reference_spec.is_compatible_with(file_spec) {
                return Ok(None);
            }
        }

        let Some(key_fields) = scan_partition_key_fields(schema, arrow_schema, reference_spec)?
        else {
            return Ok(None);
        };
        let ordering = LexOrdering::new(
            key_fields
                .iter()
                .map(|field| field.sort_expr.clone())
                .collect::<Vec<_>>(),
        )
        .ok_or_else(|| internal_datafusion_err!("Iceberg scan range key is empty"))?;
        if ordering.len() != key_fields.len() {
            return Err(internal_datafusion_err!(
                "Iceberg scan range key lost expressions while building its ordering"
            ));
        }

        let mut files_by_partition_values: HashMap<Vec<ScalarValue>, Vec<String>> = HashMap::new();
        for data_file in data_files {
            let mut partition_values = Vec::with_capacity(key_fields.len());
            for key_field in &key_fields {
                let result_arrow_type = iceberg_type_to_arrow(&key_field.result_type)?;
                let partition_value = match data_file
                    .partition()
                    .get(key_field.partition_field_index)
                    .and_then(Option::as_ref)
                {
                    Some(literal) => to_scalar(literal, &key_field.result_type).map_err(|error| {
                        plan_datafusion_err!(
                            "Iceberg data file {} has an invalid partition value for field {}: {error}",
                            data_file.file_path,
                            key_field.partition_field_index
                        )
                    })?,
                    None => ScalarValue::try_new_null(&result_arrow_type)?,
                };
                if partition_value.data_type() != result_arrow_type {
                    return Err(plan_datafusion_err!(
                        "Iceberg data file {} partition field {} has type {}, expected {}",
                        data_file.file_path,
                        key_field.partition_field_index,
                        partition_value.data_type(),
                        result_arrow_type
                    ));
                }
                partition_values.push(partition_value);
            }
            files_by_partition_values
                .entry(partition_values)
                .or_default()
                .push(data_file.file_path.clone());
        }

        let mut distinct_partition_values = files_by_partition_values
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let sort_options = ordering
            .iter()
            .map(|sort_expr| sort_expr.options)
            .collect::<Vec<_>>();
        let mut comparison_error = None;
        distinct_partition_values.sort_by(|left, right| {
            if comparison_error.is_some() {
                return Ordering::Equal;
            }
            match datafusion::common::utils::compare_rows(left, right, &sort_options) {
                Ok(ordering) => ordering,
                Err(error) => {
                    comparison_error = Some(error);
                    Ordering::Equal
                }
            }
        });
        if let Some(error) = comparison_error {
            return Err(error);
        }

        let distinct_count = distinct_partition_values.len();
        if distinct_count == 0 {
            return Ok(None);
        }
        let partition_count = distinct_count.min(target_partitions.max(1));
        let mut partition_by_values = HashMap::with_capacity(distinct_count);
        let mut split_points = Vec::with_capacity(partition_count.saturating_sub(1));
        let mut previous_partition = 0;
        for (value_index, partition_values) in distinct_partition_values.iter().enumerate() {
            let partition = value_index.saturating_mul(partition_count) / distinct_count;
            if partition > previous_partition {
                if partition != previous_partition + 1 {
                    return Err(internal_datafusion_err!(
                        "Iceberg scan range skipped output partition {}",
                        previous_partition + 1
                    ));
                }
                split_points.push(SplitPoint::new(partition_values.clone()));
                previous_partition = partition;
            }
            partition_by_values.insert(partition_values.clone(), partition);
        }
        if split_points.len() + 1 != partition_count {
            return Err(internal_datafusion_err!(
                "Iceberg scan range produced {} split points for {partition_count} partitions",
                split_points.len()
            ));
        }

        let range = RangePartitioning::try_new(ordering, split_points)?;
        let mut file_partitions = HashMap::with_capacity(data_files.len());
        for (partition_values, file_paths) in files_by_partition_values {
            let partition = partition_by_values
                .get(&partition_values)
                .copied()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Iceberg scan range lost a partition key after sorting"
                    )
                })?;
            for file_path in file_paths {
                if let Some(previous) = file_partitions.insert(file_path.clone(), partition)
                    && previous != partition
                {
                    return Err(plan_datafusion_err!(
                        "Iceberg data file {file_path} maps to conflicting scan partitions {previous} and {partition}"
                    ));
                }
            }
        }

        Ok(Some(Self {
            range,
            file_partitions,
        }))
    }

    pub(crate) fn output_partitioning(&self) -> Partitioning {
        Partitioning::Range(self.range.clone())
    }

    pub(crate) fn partition_count(&self) -> usize {
        self.range.partition_count()
    }

    pub(crate) fn partition_index_for_file(&self, file_path: &str) -> Option<usize> {
        self.file_partitions.get(file_path).copied()
    }
}

#[derive(Debug)]
struct IcebergPartitionKeyField {
    partition_field_index: usize,
    result_type: Type,
    sort_expr: PhysicalSortExpr,
}

fn scan_partition_key_fields(
    schema: &Schema,
    arrow_schema: &ArrowSchema,
    partition_spec: &PartitionSpec,
) -> Result<Option<Vec<IcebergPartitionKeyField>>> {
    if partition_spec.is_unpartitioned() {
        return Ok(None);
    }

    let mut key_fields = Vec::with_capacity(partition_spec.fields().len());
    for (partition_field_index, partition_field) in partition_spec.fields().iter().enumerate() {
        if partition_field.transform == Transform::Void {
            continue;
        }
        let Ok(source_id) = partition_field.source_id() else {
            return Ok(None);
        };
        let Some(source_field) = schema.fields().iter().find(|field| field.id == source_id) else {
            return Ok(None);
        };
        let Type::Primitive(source_type) = source_field.field_type.as_ref() else {
            return Ok(None);
        };
        if !supports_scan_partition_transform(partition_field.transform, source_type) {
            return Ok(None);
        }

        let Ok(column_index) = arrow_schema.index_of(source_field.name.as_str()) else {
            return Ok(None);
        };
        let source_arrow_type = iceberg_type_to_arrow(source_field.field_type.as_ref())?;
        if arrow_schema.field(column_index).data_type() != &source_arrow_type {
            return Ok(None);
        }
        let result_type = partition_field
            .transform
            .result_type(source_field.field_type.as_ref())
            .map_err(DataFusionError::Plan)?;
        let column: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new(source_field.name.as_str(), column_index));
        let partition_expr: Arc<dyn PhysicalExpr> = match partition_field.transform {
            Transform::Identity => column,
            transform => Arc::new(IcebergPartitionTransformExpr::new(column, transform)),
        };
        key_fields.push(IcebergPartitionKeyField {
            partition_field_index,
            result_type,
            sort_expr: PhysicalSortExpr::new_default(partition_expr),
        });
    }

    if key_fields.is_empty() {
        Ok(None)
    } else {
        Ok(Some(key_fields))
    }
}

fn supports_scan_partition_transform(transform: Transform, source_type: &PrimitiveType) -> bool {
    match transform {
        Transform::Identity => matches!(
            source_type,
            PrimitiveType::Boolean
                | PrimitiveType::Int
                | PrimitiveType::Long
                | PrimitiveType::Decimal { .. }
                | PrimitiveType::Date
                | PrimitiveType::Time
                | PrimitiveType::Timestamp
                | PrimitiveType::Timestamptz
                | PrimitiveType::TimestampNs
                | PrimitiveType::TimestamptzNs
                | PrimitiveType::String
        ),
        Transform::Bucket(count) if count > 0 && count <= i32::MAX as u32 => matches!(
            source_type,
            PrimitiveType::Int
                | PrimitiveType::Long
                | PrimitiveType::Date
                | PrimitiveType::Time
                | PrimitiveType::Timestamp
                | PrimitiveType::Timestamptz
                | PrimitiveType::String
                | PrimitiveType::Binary
        ),
        Transform::Truncate(width) if width > 0 => match source_type {
            PrimitiveType::Int => width <= i32::MAX as u32,
            PrimitiveType::Long | PrimitiveType::String => true,
            _ => false,
        },
        Transform::Year | Transform::Month | Transform::Day => matches!(
            source_type,
            PrimitiveType::Date
                | PrimitiveType::Timestamp
                | PrimitiveType::Timestamptz
                | PrimitiveType::TimestampNs
                | PrimitiveType::TimestamptzNs
        ),
        Transform::Hour => matches!(
            source_type,
            PrimitiveType::Timestamp
                | PrimitiveType::Timestamptz
                | PrimitiveType::TimestampNs
                | PrimitiveType::TimestamptzNs
        ),
        Transform::Void | Transform::Unknown | Transform::Bucket(_) | Transform::Truncate(_) => {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::collections::HashMap;

    use datafusion::physical_plan::Partitioning;

    use super::*;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::spec::manifest::{DataContentType, DataFileFormat};
    use crate::spec::types::NestedField;
    use crate::spec::types::values::{Literal, PrimitiveLiteral};

    fn schema(field_type: PrimitiveType) -> Schema {
        Schema::builder()
            .with_fields([Arc::new(NestedField::optional(
                1,
                "partition_source",
                Type::Primitive(field_type),
            ))])
            .build()
            .unwrap()
    }

    fn partition_spec(spec_id: i32, transform: Transform) -> PartitionSpec {
        PartitionSpec::builder()
            .with_spec_id(spec_id)
            .add_field(1, "partition_value", transform)
            .build()
    }

    fn data_file(path: &str, spec_id: i32, partition: Vec<Option<Literal>>) -> DataFile {
        DataFile {
            content: DataContentType::Data,
            file_path: path.to_string(),
            file_format: DataFileFormat::Parquet,
            partition,
            record_count: 1,
            file_size_in_bytes: 1,
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            nan_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            block_size_in_bytes: None,
            key_metadata: None,
            split_offsets: Vec::new(),
            equality_ids: Vec::new(),
            sort_order_id: None,
            first_row_id: None,
            partition_spec_id: spec_id,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn int_partition(value: i32) -> Vec<Option<Literal>> {
        vec![Some(Literal::Primitive(PrimitiveLiteral::Int(value)))]
    }

    #[test]
    fn range_layout_assigns_contiguous_partition_keys() {
        let schema = schema(PrimitiveType::Int);
        let arrow_schema = iceberg_schema_to_arrow(&schema).unwrap();
        let spec = partition_spec(3, Transform::Identity);
        let files = (1..=5)
            .map(|value| data_file(&format!("file-{value}.parquet"), 3, int_partition(value)))
            .collect::<Vec<_>>();

        let layout = IcebergScanRangeLayout::try_new(&schema, &arrow_schema, &[spec], &files, 3)
            .unwrap()
            .unwrap();

        assert_eq!(layout.partition_count(), 3);
        assert_eq!(layout.partition_index_for_file("file-1.parquet"), Some(0));
        assert_eq!(layout.partition_index_for_file("file-2.parquet"), Some(0));
        assert_eq!(layout.partition_index_for_file("file-3.parquet"), Some(1));
        assert_eq!(layout.partition_index_for_file("file-4.parquet"), Some(1));
        assert_eq!(layout.partition_index_for_file("file-5.parquet"), Some(2));
        assert!(matches!(
            layout.output_partitioning(),
            Partitioning::Range(_)
        ));
        let range = &layout.range;
        assert_eq!(
            range.split_points()[0].values(),
            &[ScalarValue::Int32(Some(3))]
        );
        assert_eq!(
            range.split_points()[1].values(),
            &[ScalarValue::Int32(Some(5))]
        );
    }

    #[test]
    fn range_layout_supports_hidden_day_transform_before_epoch() {
        let schema = schema(PrimitiveType::Timestamp);
        let arrow_schema = iceberg_schema_to_arrow(&schema).unwrap();
        let spec = partition_spec(7, Transform::Day);
        let files = vec![
            data_file("before.parquet", 7, int_partition(-1)),
            data_file("epoch.parquet", 7, int_partition(0)),
        ];

        let layout = IcebergScanRangeLayout::try_new(&schema, &arrow_schema, &[spec], &files, 2)
            .unwrap()
            .unwrap();

        assert_eq!(layout.partition_index_for_file("before.parquet"), Some(0));
        assert_eq!(layout.partition_index_for_file("epoch.parquet"), Some(1));
        assert!(matches!(
            layout.output_partitioning(),
            Partitioning::Range(_)
        ));
        let range = &layout.range;
        assert!(
            range.ordering()[0]
                .expr
                .downcast_ref::<IcebergPartitionTransformExpr>()
                .is_some()
        );
        assert_eq!(
            range.split_points()[0].values(),
            &[ScalarValue::Int32(Some(0))]
        );
    }

    #[test]
    fn compatible_partition_specs_share_one_layout() {
        let schema = schema(PrimitiveType::Int);
        let arrow_schema = iceberg_schema_to_arrow(&schema).unwrap();
        let files = vec![
            data_file("old.parquet", 1, int_partition(1)),
            data_file("new.parquet", 2, int_partition(2)),
        ];

        let layout = IcebergScanRangeLayout::try_new(
            &schema,
            &arrow_schema,
            &[
                partition_spec(1, Transform::Identity),
                partition_spec(2, Transform::Identity),
            ],
            &files,
            2,
        )
        .unwrap();

        assert!(layout.is_some());
    }

    #[test]
    fn incompatible_partition_specs_disable_layout() {
        let schema = schema(PrimitiveType::Int);
        let arrow_schema = iceberg_schema_to_arrow(&schema).unwrap();
        let files = vec![
            data_file("identity.parquet", 1, int_partition(1)),
            data_file("bucket.parquet", 2, int_partition(1)),
        ];

        let layout = IcebergScanRangeLayout::try_new(
            &schema,
            &arrow_schema,
            &[
                partition_spec(1, Transform::Identity),
                partition_spec(2, Transform::Bucket(8)),
            ],
            &files,
            2,
        )
        .unwrap();

        assert!(layout.is_none());
    }

    #[test]
    fn missing_partition_spec_and_invalid_tuple_are_errors() {
        let schema = schema(PrimitiveType::Int);
        let arrow_schema = iceberg_schema_to_arrow(&schema).unwrap();
        let missing_spec = data_file("missing.parquet", 9, int_partition(1));
        let error = IcebergScanRangeLayout::try_new(
            &schema,
            &arrow_schema,
            &[partition_spec(1, Transform::Identity)],
            &[missing_spec],
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("missing partition spec 9"));

        let invalid_tuple = data_file("invalid.parquet", 1, Vec::new());
        let error = IcebergScanRangeLayout::try_new(
            &schema,
            &arrow_schema,
            &[partition_spec(1, Transform::Identity)],
            &[invalid_tuple],
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("0 partition values"));
    }
}
