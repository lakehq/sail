// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, HashMap, HashSet};

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StructArray};
use datafusion::common::{DataFusionError, Result, plan_datafusion_err, plan_err};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::execute_stream;
use futures::TryStreamExt;
use url::Url;

use super::alpha::AlphaSketch;
use crate::datasource::IcebergScan;
use crate::io::puffin::{PUFFIN_MAGIC, append_puffin_footer};
use crate::spec::{
    BlobMetadata, Datum, Literal, NestedFieldRef, PrimitiveType, Schema, Snapshot, StatisticsFile,
    TableMetadata, Type,
};
use crate::utils::conversions::array_value_to_literal;

struct ColumnSketch {
    path: Vec<NestedFieldRef>,
    field_id: i32,
    primitive: PrimitiveType,
    sketch: AlphaSketch,
}

impl ColumnSketch {
    fn update(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut array: ArrayRef = batch
            .column(batch.schema().index_of(&self.path[0].name)?)
            .clone();
        for field in &self.path[1..] {
            let parent = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| plan_datafusion_err!("Expected struct parent for {}", field.name))?;
            let index = parent
                .fields()
                .find(&field.name)
                .ok_or_else(|| plan_datafusion_err!("Missing struct field {}", field.name))?
                .0;
            // The child's validity must include all enclosing nullable structs.
            array = parent.flatten().1.swap_remove(index);
        }
        let ty = Type::Primitive(self.primitive.clone());
        for row in 0..array.len() {
            if let Some(Literal::Primitive(value)) =
                array_value_to_literal(&array, row, &ty).map_err(DataFusionError::Execution)?
            {
                let bytes = Datum::new(self.primitive.clone(), value)
                    .to_bytes()
                    .map_err(DataFusionError::Execution)?;
                self.sketch.update(&bytes)?;
            }
        }
        Ok(())
    }
}

fn select_columns(schema: &Schema, names: Option<&[String]>) -> Result<Vec<ColumnSketch>> {
    let fields = match names {
        Some(names) => names
            .iter()
            .map(|name| {
                schema
                    .field_by_name(name)
                    .ok_or_else(|| plan_datafusion_err!("Can't find column {name}"))
            })
            .collect::<Result<Vec<_>>>()?,
        None => schema
            .fields()
            .iter()
            .filter(|f| matches!(f.field_type.as_ref(), Type::Primitive(p) if *p != PrimitiveType::Variant))
            .collect(),
    };
    if fields.is_empty() {
        return plan_err!("No columns found to compute stats");
    }
    let mut seen = HashSet::new();
    fields
        .into_iter()
        .filter(|f| seen.insert(f.id))
        .map(|f| {
            let primitive = match f.field_type.as_ref() {
                Type::Primitive(p) if *p != PrimitiveType::Variant => p,
                _ => {
                    return plan_err!(
                        "Can't compute stats on non-primitive type column: {}",
                        f.name
                    );
                }
            };
            Ok(ColumnSketch {
                field_id: f.id,
                primitive: primitive.clone(),
                path: schema.field_path_by_id(f.id).ok_or_else(|| {
                    plan_datafusion_err!("Expected a row-level field: {}", f.name)
                })?,
                sketch: AlphaSketch::default(),
            })
        })
        .collect()
}

// [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/actions/NDVSketchUtil.java
pub(super) async fn compute(
    ctx: &TaskContext,
    url: &Url,
    metadata: &TableMetadata,
    snapshot: &Snapshot,
    names: Option<&[String]>,
    path: String,
) -> Result<(StatisticsFile, Vec<u8>)> {
    let schema = match snapshot.schema_id() {
        Some(id) => metadata
            .schemas
            .iter()
            .find(|schema| schema.schema_id() == id),
        None => metadata.current_schema(),
    }
    .ok_or_else(|| {
        plan_datafusion_err!("Missing schema for snapshot {}", snapshot.snapshot_id())
    })?;
    let mut columns = select_columns(schema, names)?;
    let mut scan = IcebergScan::new(
        url,
        schema.clone(),
        snapshot.clone(),
        metadata.partition_specs.clone(),
        metadata.default_spec_id,
    )?;
    scan.set_table_metadata(metadata)?;
    let projection = columns
        .iter()
        .map(|col| scan.schema().index_of(&col.path[0].name))
        .collect::<std::result::Result<BTreeSet<_>, _>>()?
        .into_iter()
        .collect();
    let session = SessionStateBuilder::new()
        .with_default_features()
        .with_session_id(ctx.session_id())
        .with_config(ctx.session_config().clone())
        .with_runtime_env(ctx.runtime_env())
        .build();
    let plan = scan
        .create_physical_plan(&session, Some(&projection), &[], None, None)
        .await?;
    let mut stream = execute_stream(plan, session.task_ctx())?;
    while let Some(batch) = stream.try_next().await? {
        for col in &mut columns {
            col.update(&batch)?;
        }
    }
    let mut bytes = PUFFIN_MAGIC.to_vec();
    let mut blob_metadata = Vec::with_capacity(columns.len());
    let mut descriptors = Vec::with_capacity(columns.len());
    for col in columns {
        let (compact, estimate) = col.sketch.compact();
        let payload = zstd::bulk::compress(&compact, 0)?;
        let blob = BlobMetadata {
            r#type: "apache-datasketches-theta-v1".into(),
            snapshot_id: snapshot.snapshot_id(),
            sequence_number: snapshot.sequence_number(),
            fields: vec![col.field_id],
            properties: HashMap::from([("ndv".into(), estimate.to_string())]),
        };
        let mut descriptor =
            serde_json::to_value(&blob).map_err(|e| DataFusionError::External(Box::new(e)))?;
        descriptor["compression-codec"] = "zstd".into();
        descriptor["offset"] = bytes.len().into();
        descriptor["length"] = payload.len().into();
        bytes.extend_from_slice(&payload);
        descriptors.push(descriptor);
        blob_metadata.push(blob);
    }
    let footer = append_puffin_footer(&mut bytes, descriptors)?;
    Ok((
        StatisticsFile {
            snapshot_id: snapshot.snapshot_id(),
            statistics_path: path,
            file_size_in_bytes: bytes.len() as i64,
            file_footer_size_in_bytes: footer,
            key_metadata: None,
            blob_metadata,
        },
        bytes,
    ))
}
