use std::sync::Arc;

use datafusion::arrow::array::builder::{
    ArrayBuilder, BooleanBuilder, ListBuilder, StringBuilder, StructBuilder,
};
use datafusion::arrow::array::{Int32Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use datafusion::common::{DataFusionError, Result};

use crate::io::load_manifest_list_with_version;
use crate::spec::types::values::Literal;
use crate::spec::{
    FieldSummary, ManifestContentType, ManifestFile, PartitionSpec, StructType, TableMetadata, Type,
};
use crate::table::Table;

pub(super) fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("content", DataType::Int32, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("length", DataType::Int64, false),
        Field::new("partition_spec_id", DataType::Int32, false),
        Field::new("added_snapshot_id", DataType::Int64, false),
        Field::new("added_data_files_count", DataType::Int32, false),
        Field::new("existing_data_files_count", DataType::Int32, false),
        Field::new("deleted_data_files_count", DataType::Int32, false),
        Field::new("added_delete_files_count", DataType::Int32, false),
        Field::new("existing_delete_files_count", DataType::Int32, false),
        Field::new("deleted_delete_files_count", DataType::Int32, false),
        Field::new(
            "partition_summaries",
            DataType::List(Arc::new(Field::new(
                "element",
                DataType::Struct(partition_summary_fields()),
                false,
            ))),
            false,
        ),
    ]))
}

pub(super) async fn batch(table: &Table) -> Result<RecordBatch> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return batch_from_manifest_files(metadata, &[]);
    };
    if snapshot.manifest_list().is_empty() {
        return Err(DataFusionError::NotImplemented(
            "Iceberg manifests metadata table does not yet support V1 snapshots without a manifest list"
                .to_string(),
        ));
    }
    let manifest_list = load_manifest_list_with_version(
        table.store_context(),
        snapshot.manifest_list(),
        metadata.format_version,
    )
    .await?;
    batch_from_manifest_files(metadata, manifest_list.entries())
}

pub(super) fn batch_from_manifest_files(
    metadata: &TableMetadata,
    manifests: &[ManifestFile],
) -> Result<RecordBatch> {
    let content = manifests
        .iter()
        .map(|manifest| match &manifest.content {
            ManifestContentType::Data => 0,
            ManifestContentType::Deletes => 1,
        })
        .collect::<Vec<_>>();
    let paths = manifests
        .iter()
        .map(|manifest| manifest.manifest_path.as_str())
        .collect::<Vec<_>>();
    let lengths = manifests
        .iter()
        .map(|manifest| manifest.manifest_length)
        .collect::<Vec<_>>();
    let spec_ids = manifests
        .iter()
        .map(|manifest| manifest.partition_spec_id)
        .collect::<Vec<_>>();
    let snapshot_ids = manifests
        .iter()
        .map(|manifest| manifest.added_snapshot_id)
        .collect::<Vec<_>>();

    let data_count = |manifest: &ManifestFile, count: Option<i32>| match &manifest.content {
        ManifestContentType::Data => count.unwrap_or(0),
        ManifestContentType::Deletes => 0,
    };
    let delete_count = |manifest: &ManifestFile, count: Option<i32>| match &manifest.content {
        ManifestContentType::Data => 0,
        ManifestContentType::Deletes => count.unwrap_or(0),
    };
    let added_data = manifests
        .iter()
        .map(|manifest| data_count(manifest, manifest.added_files_count))
        .collect::<Vec<_>>();
    let existing_data = manifests
        .iter()
        .map(|manifest| data_count(manifest, manifest.existing_files_count))
        .collect::<Vec<_>>();
    let deleted_data = manifests
        .iter()
        .map(|manifest| data_count(manifest, manifest.deleted_files_count))
        .collect::<Vec<_>>();
    let added_deletes = manifests
        .iter()
        .map(|manifest| delete_count(manifest, manifest.added_files_count))
        .collect::<Vec<_>>();
    let existing_deletes = manifests
        .iter()
        .map(|manifest| delete_count(manifest, manifest.existing_files_count))
        .collect::<Vec<_>>();
    let deleted_deletes = manifests
        .iter()
        .map(|manifest| delete_count(manifest, manifest.deleted_files_count))
        .collect::<Vec<_>>();

    let summary_fields = partition_summary_fields();
    let mut summaries = ListBuilder::new(StructBuilder::from_fields(
        summary_fields.clone(),
        manifests
            .iter()
            .filter_map(|manifest| manifest.partitions.as_ref())
            .map(Vec::len)
            .sum(),
    ))
    .with_field(Field::new(
        "element",
        DataType::Struct(summary_fields),
        false,
    ));
    for manifest in manifests {
        append_partition_summaries(&mut summaries, metadata, manifest)?;
    }

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(content)),
            Arc::new(datafusion::arrow::array::StringArray::from(paths)),
            Arc::new(Int64Array::from(lengths)),
            Arc::new(Int32Array::from(spec_ids)),
            Arc::new(Int64Array::from(snapshot_ids)),
            Arc::new(Int32Array::from(added_data)),
            Arc::new(Int32Array::from(existing_data)),
            Arc::new(Int32Array::from(deleted_data)),
            Arc::new(Int32Array::from(added_deletes)),
            Arc::new(Int32Array::from(existing_deletes)),
            Arc::new(Int32Array::from(deleted_deletes)),
            Arc::new(summaries.finish()),
        ],
    )
    .map_err(Into::into)
}

fn partition_summary_fields() -> Fields {
    vec![
        Arc::new(Field::new("contains_null", DataType::Boolean, false)),
        Arc::new(Field::new("contains_nan", DataType::Boolean, true)),
        Arc::new(Field::new("lower_bound", DataType::Utf8, true)),
        Arc::new(Field::new("upper_bound", DataType::Utf8, true)),
    ]
    .into()
}

fn append_partition_summaries(
    builder: &mut ListBuilder<StructBuilder>,
    metadata: &TableMetadata,
    manifest: &ManifestFile,
) -> Result<()> {
    let (spec, partition_type) = manifest_partition_type(metadata, manifest.partition_spec_id)?;
    let Some(summaries) = manifest.partitions.as_deref() else {
        if spec.fields().is_empty() {
            builder.append(true);
            return Ok(());
        }
        return Err(DataFusionError::Plan(format!(
            "Iceberg manifest '{}' is missing partition summaries for partition spec {}",
            manifest.manifest_path, manifest.partition_spec_id
        )));
    };
    if summaries.len() != spec.fields().len() {
        return Err(DataFusionError::Plan(format!(
            "Iceberg manifest '{}' has {} partition summaries for a spec with {} fields",
            manifest.manifest_path,
            summaries.len(),
            spec.fields().len()
        )));
    }
    for (index, summary) in summaries.iter().enumerate() {
        let field = partition_type.fields().get(index).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Iceberg partition spec {} is missing field {index}",
                manifest.partition_spec_id
            ))
        })?;
        let transform = spec.fields()[index].transform;
        let lower = human_bound(
            transform,
            field.field_type.as_ref(),
            summary.lower_bound_bytes.as_deref(),
        )?;
        let upper = human_bound(
            transform,
            field.field_type.as_ref(),
            summary.upper_bound_bytes.as_deref(),
        )?;
        append_partition_summary(builder.values(), summary, &lower, &upper)?;
    }
    builder.append(true);
    Ok(())
}

fn manifest_partition_type(
    metadata: &TableMetadata,
    spec_id: i32,
) -> Result<(&PartitionSpec, StructType)> {
    let spec = metadata
        .partition_specs
        .iter()
        .find(|spec| spec.spec_id() == spec_id)
        .ok_or_else(|| {
            DataFusionError::Plan(format!("Unknown Iceberg partition spec {spec_id}"))
        })?;
    let partition_type = metadata
        .current_schema()
        .and_then(|schema| spec.partition_type(schema).ok())
        .or_else(|| {
            metadata
                .schemas
                .iter()
                .find_map(|schema| spec.partition_type(schema).ok())
        })
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Cannot bind Iceberg partition spec {spec_id} to a retained table schema"
            ))
        })?;
    Ok((spec, partition_type))
}

fn human_bound(
    transform: crate::spec::Transform,
    field_type: &Type,
    bytes: Option<&[u8]>,
) -> Result<String> {
    let Type::Primitive(primitive) = field_type else {
        return Err(DataFusionError::Plan(
            "Iceberg manifest partition bound has a non-primitive type".to_string(),
        ));
    };
    let literal = bytes
        .map(|bytes| primitive.literal_from_bytes(bytes).map(Literal::Primitive))
        .transpose()
        .map_err(DataFusionError::Plan)?;
    Ok(transform.to_human_string(field_type, literal.as_ref()))
}

fn append_partition_summary(
    builder: &mut StructBuilder,
    summary: &FieldSummary,
    lower: &str,
    upper: &str,
) -> Result<()> {
    field_builder::<BooleanBuilder>(builder, 0)?.append_value(summary.contains_null);
    field_builder::<BooleanBuilder>(builder, 1)?.append_option(summary.contains_nan);
    field_builder::<StringBuilder>(builder, 2)?.append_value(lower);
    field_builder::<StringBuilder>(builder, 3)?.append_value(upper);
    builder.append(true);
    Ok(())
}

fn field_builder<T: ArrayBuilder>(builder: &mut StructBuilder, index: usize) -> Result<&mut T> {
    builder.field_builder::<T>(index).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Iceberg manifests partition summary builder is missing field {index}"
        ))
    })
}
