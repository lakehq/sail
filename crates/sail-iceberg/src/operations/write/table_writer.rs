// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, LargeListArray, ListArray, MapArray, StructArray, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use sail_common_datafusion::array::record_batch::{
    cast_array_recursively, cast_record_batch_relaxed_tz,
};
use url::Url;

use crate::operations::write::arrow_parquet::ArrowParquetWriter;
use crate::operations::write::base_writer::DataFileWriter;
use crate::operations::write::config::WriterConfig;
use crate::operations::write::file_writer::location_generator::{
    DefaultLocationGenerator, LocationGenerator,
};
use crate::operations::write::partition::split_record_batch_by_partition;
use crate::operations::write::variant_shredding::{
    VariantShreddingPlan, apply_variant_shredding_plan, build_variant_shredding_plan,
    unshred_shredded_variants_for_write,
};
use crate::spec::DataFile;
use crate::spec::schema::Schema as IcebergSchema;
use crate::spec::types::NestedField;
use crate::spec::types::values::Literal;
use crate::utils::conversions::to_scalar;

enum PartitionWriterState {
    Pending {
        batches: Vec<RecordBatch>,
        num_rows: usize,
    },
    Open {
        writer: Box<ArrowParquetWriter>,
        variant_shredding_plan: Option<VariantShreddingPlan>,
    },
}

pub struct IcebergTableWriter {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub config: WriterConfig,
    pub generator: DefaultLocationGenerator,
    pub data_url: Url,
    // partition_dir -> writer
    writers: HashMap<String, PartitionWriterState>,
    // partition_dir -> partition values aligned with spec
    partition_values_map: HashMap<String, Vec<Option<Literal>>>,
    written: Vec<DataFile>,
    pub partition_spec_id: i32,
}

impl IcebergTableWriter {
    pub fn new(
        store: Arc<dyn object_store::ObjectStore>,
        root: ObjectPath,
        config: WriterConfig,
        partition_spec_id: i32,
        data_url: Url,
    ) -> Self {
        Self {
            generator: DefaultLocationGenerator::new(root),
            store,
            config,
            data_url,
            writers: HashMap::new(),
            partition_values_map: HashMap::new(),
            written: Vec::new(),
            partition_spec_id,
        }
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let spec = &self.config.partition_spec;
        let iceberg_schema = &self.config.iceberg_schema;

        if spec.fields.is_empty() {
            // Unpartitioned: write as-is once
            let partition_dir = String::new();
            self.partition_values_map
                .entry(partition_dir.clone())
                .or_default();
            let padded = Self::align_batch_with_table_schema(
                batch,
                &self.config.table_schema,
                self.config.iceberg_schema.as_ref(),
            )
            .map_err(|e| e.to_string())?;
            let normalized =
                unshred_shredded_variants_for_write(&padded, &self.config.table_schema)?;
            let aligned = cast_record_batch_relaxed_tz(&normalized, &self.config.table_schema)
                .map_err(|e| e.to_string())?;
            self.write_aligned_batch(partition_dir, aligned).await?;
            return Ok(());
        }

        let parts = split_record_batch_by_partition(batch, spec, iceberg_schema)?;
        for p in parts.into_iter() {
            let partition_dir = p.partition_dir;
            self.partition_values_map
                .entry(partition_dir.clone())
                .or_insert(p.partition_values);
            let padded = Self::align_batch_with_table_schema(
                &p.record_batch,
                &self.config.table_schema,
                self.config.iceberg_schema.as_ref(),
            )
            .map_err(|e| e.to_string())?;
            let normalized =
                unshred_shredded_variants_for_write(&padded, &self.config.table_schema)?;
            let aligned = cast_record_batch_relaxed_tz(&normalized, &self.config.table_schema)
                .map_err(|e| e.to_string())?;
            self.write_aligned_batch(partition_dir, aligned).await?;
        }

        Ok(())
    }

    async fn write_aligned_batch(
        &mut self,
        partition_dir: String,
        batch: RecordBatch,
    ) -> Result<(), String> {
        let state = self
            .writers
            .remove(&partition_dir)
            .map(Ok)
            .unwrap_or_else(|| self.new_partition_writer_state())?;
        let state = self.write_partition_state(state, batch).await?;
        self.writers.insert(partition_dir, state);
        Ok(())
    }

    fn new_partition_writer_state(&self) -> Result<PartitionWriterState, String> {
        if self.config.variant_shredding.enabled {
            Ok(PartitionWriterState::Pending {
                batches: Vec::new(),
                num_rows: 0,
            })
        } else {
            Ok(PartitionWriterState::Open {
                writer: Box::new(self.new_arrow_writer(self.config.table_schema.clone())?),
                variant_shredding_plan: None,
            })
        }
    }

    async fn write_partition_state(
        &mut self,
        state: PartitionWriterState,
        batch: RecordBatch,
    ) -> Result<PartitionWriterState, String> {
        match state {
            PartitionWriterState::Pending {
                mut batches,
                mut num_rows,
            } => {
                num_rows += batch.num_rows();
                batches.push(batch);
                if num_rows >= self.config.variant_shredding.inference_buffer_size.max(1) {
                    self.open_and_write_pending_batches(batches).await
                } else {
                    Ok(PartitionWriterState::Pending { batches, num_rows })
                }
            }
            PartitionWriterState::Open {
                mut writer,
                variant_shredding_plan,
            } => {
                let batch = if let Some(plan) = variant_shredding_plan.as_ref() {
                    apply_variant_shredding_plan(&batch, plan)?
                } else {
                    batch
                };
                writer.write_batch(&batch).await?;
                Ok(PartitionWriterState::Open {
                    writer,
                    variant_shredding_plan,
                })
            }
        }
    }

    async fn open_and_write_pending_batches(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Result<PartitionWriterState, String> {
        let plan = build_variant_shredding_plan(
            &self.config.table_schema,
            &batches,
            self.config.variant_shredding.inference_buffer_size,
            self.config.variant_shredding.inference_node_budget,
        )?;
        let plan = (!plan.is_noop()).then_some(plan);
        let physical_batches = batches
            .into_iter()
            .map(|batch| {
                if let Some(plan) = plan.as_ref() {
                    apply_variant_shredding_plan(&batch, plan)
                } else {
                    Ok(batch)
                }
            })
            .collect::<std::result::Result<Vec<_>, String>>()?;

        let schema = physical_batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| self.config.table_schema.clone());
        let mut writer = self.new_arrow_writer(schema)?;
        for batch in physical_batches {
            writer.write_batch(&batch).await?;
        }
        Ok(PartitionWriterState::Open {
            writer: Box::new(writer),
            variant_shredding_plan: plan,
        })
    }

    fn new_arrow_writer(&self, schema: SchemaRef) -> Result<ArrowParquetWriter, String> {
        for (i, f) in schema.fields().iter().enumerate() {
            log::trace!(
                "iceberg.table_writer.writer_schema: field[{}]='{}' type={:?} field_id_meta={:?}",
                i,
                f.name(),
                f.data_type(),
                f.metadata().get(PARQUET_FIELD_ID_META_KEY)
            );
        }
        ArrowParquetWriter::try_new(schema.as_ref(), self.config.writer_properties.clone())
    }

    async fn finish_partition_state(
        &mut self,
        state: PartitionWriterState,
    ) -> Result<ArrowParquetWriter, String> {
        match state {
            PartitionWriterState::Pending { batches, .. } => {
                let PartitionWriterState::Open { writer, .. } =
                    self.open_and_write_pending_batches(batches).await?
                else {
                    return Err("failed to open pending Iceberg partition writer".to_string());
                };
                Ok(*writer)
            }
            PartitionWriterState::Open { writer, .. } => Ok(*writer),
        }
    }

    pub async fn flush_partition(
        &mut self,
        partition_dir: &str,
        partition_values: Vec<Option<Literal>>,
    ) -> Result<(), String> {
        if let Some(state) = self.writers.remove(partition_dir) {
            let writer = self.finish_partition_state(state).await?;
            let (bytes, meta) = writer.close().await?;
            let (rel, full) = self.generator.with_partition_dir(Some(partition_dir));
            log::trace!("iceberg.table_writer.flush_partition.writing: {}", full);
            self.store
                .put(&full, object_store::PutPayload::from(bytes))
                .await
                .map_err(|e| e.to_string())?;
            log::trace!(
                "iceberg.table_writer.flush_partition.written: rel={} full={}",
                rel,
                full
            );
            // Prevent a leading partition segment containing ':' from being parsed as a URI scheme.
            let file_path = match self.data_url.join(&format!("./{rel}")) {
                Ok(u) => u.to_string(),
                Err(_) => {
                    format!("{}{}", self.data_url.as_str(), rel)
                }
            };
            let df = DataFileWriter::new(self.partition_spec_id, file_path, partition_values)
                .finish(meta)?
                .data_file;
            self.written.push(df);
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>, String> {
        let keys: Vec<String> = self.writers.keys().cloned().collect();
        for k in keys {
            let vals = self
                .partition_values_map
                .remove(&k)
                .unwrap_or_default()
                .into_iter()
                .collect();
            self.flush_partition(&k, vals).await?;
        }
        Ok(self.written)
    }

    fn align_batch_with_table_schema(
        batch: &RecordBatch,
        table_schema: &SchemaRef,
        iceberg_schema: &IcebergSchema,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(table_schema.fields().len());
        let mut schema_fields: Vec<FieldRef> = Vec::with_capacity(table_schema.fields().len());

        for field in table_schema.fields() {
            match batch.schema().index_of(field.name()) {
                Ok(idx) => {
                    let iceberg_field =
                        iceberg_schema.field_by_name(field.name()).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Column '{}' missing from Iceberg schema during alignment",
                                field.name()
                            ))
                        })?;
                    let array = Self::align_nested_write_defaults(
                        batch.column(idx),
                        field,
                        iceberg_field.as_ref(),
                    )?;
                    let schema_field = if array.data_type() == field.data_type() {
                        field.clone()
                    } else {
                        Arc::new(batch.schema().field(idx).clone())
                    };
                    columns.push(array);
                    schema_fields.push(schema_field);
                }
                Err(_) => {
                    let array =
                        Self::build_missing_column_array(field, iceberg_schema, batch.num_rows())?;
                    columns.push(array);
                    schema_fields.push(field.clone());
                }
            }
        }

        let aligned_schema = Arc::new(Schema::new(schema_fields));
        Ok(RecordBatch::try_new(aligned_schema, columns)?)
    }

    fn align_nested_write_defaults(
        source: &ArrayRef,
        target_field: &FieldRef,
        iceberg_field: &NestedField,
    ) -> Result<ArrayRef, DataFusionError> {
        match (
            source.data_type(),
            target_field.data_type(),
            iceberg_field.field_type.as_ref(),
        ) {
            (
                DataType::Struct(_),
                DataType::Struct(target_fields),
                crate::spec::types::Type::Struct(iceberg_struct),
            ) => {
                let source = source
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Column '{}' is not a struct array",
                            target_field.name()
                        ))
                    })?;
                let mut children = Vec::with_capacity(target_fields.len());
                for child_field in target_fields {
                    let iceberg_child = iceberg_struct
                        .field_by_name(child_field.name())
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Nested column '{}.{}' missing from Iceberg schema during alignment",
                                target_field.name(),
                                child_field.name()
                            ))
                        })?;
                    let child = match source.column_by_name(child_field.name()) {
                        Some(child) => Self::align_nested_write_defaults(
                            child,
                            child_field,
                            iceberg_child.as_ref(),
                        )?,
                        None => Self::build_missing_nested_array(
                            child_field,
                            iceberg_child.as_ref(),
                            source.len(),
                        )?,
                    };
                    children.push(cast_array_recursively(&child, child_field.data_type())?);
                }
                Ok(Arc::new(StructArray::try_new(
                    target_fields.clone(),
                    children,
                    source.nulls().cloned(),
                )?))
            }
            (
                DataType::List(_),
                DataType::List(target_element),
                crate::spec::types::Type::List(iceberg_list),
            ) => {
                let source = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Column '{}' is not a list array",
                        target_field.name()
                    ))
                })?;
                let values = Self::align_nested_write_defaults(
                    source.values(),
                    target_element,
                    iceberg_list.element_field.as_ref(),
                )?;
                let values = cast_array_recursively(&values, target_element.data_type())?;
                Ok(Arc::new(ListArray::try_new(
                    target_element.clone(),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )?))
            }
            (
                DataType::LargeList(_),
                DataType::LargeList(target_element),
                crate::spec::types::Type::List(iceberg_list),
            ) => {
                let source = source
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Column '{}' is not a large-list array",
                            target_field.name()
                        ))
                    })?;
                let values = Self::align_nested_write_defaults(
                    source.values(),
                    target_element,
                    iceberg_list.element_field.as_ref(),
                )?;
                let values = cast_array_recursively(&values, target_element.data_type())?;
                Ok(Arc::new(LargeListArray::try_new(
                    target_element.clone(),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )?))
            }
            (
                DataType::Map(_, _),
                DataType::Map(target_entries, sorted),
                crate::spec::types::Type::Map(iceberg_map),
            ) => {
                let source = source.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Column '{}' is not a map array",
                        target_field.name()
                    ))
                })?;
                let DataType::Struct(target_entry_fields) = target_entries.data_type() else {
                    return Err(DataFusionError::Plan(format!(
                        "Column '{}' has invalid map entry type",
                        target_field.name()
                    )));
                };
                if target_entry_fields.len() != 2 {
                    return Err(DataFusionError::Plan(format!(
                        "Column '{}' map entries must contain key and value fields",
                        target_field.name()
                    )));
                }
                let source_entries = source.entries();
                let key = Self::align_nested_write_defaults(
                    source_entries.column(0),
                    &target_entry_fields[0],
                    iceberg_map.key_field.as_ref(),
                )?;
                let value = Self::align_nested_write_defaults(
                    source_entries.column(1),
                    &target_entry_fields[1],
                    iceberg_map.value_field.as_ref(),
                )?;
                let entries = StructArray::try_new(
                    target_entry_fields.clone(),
                    vec![
                        cast_array_recursively(&key, target_entry_fields[0].data_type())?,
                        cast_array_recursively(&value, target_entry_fields[1].data_type())?,
                    ],
                    source_entries.nulls().cloned(),
                )?;
                Ok(Arc::new(MapArray::try_new(
                    target_entries.clone(),
                    source.offsets().clone(),
                    entries,
                    source.nulls().cloned(),
                    *sorted,
                )?))
            }
            _ => Ok(source.clone()),
        }
    }

    fn build_missing_nested_array(
        field: &FieldRef,
        iceberg_field: &NestedField,
        num_rows: usize,
    ) -> Result<ArrayRef, DataFusionError> {
        if let Some(array) = Self::default_array_for_field(iceberg_field, num_rows)? {
            return Ok(array);
        }
        if field.is_nullable() {
            return Ok(new_null_array(field.data_type(), num_rows));
        }
        Err(DataFusionError::Plan(format!(
            "Nested column '{}' is required but missing in input batch and has no write default",
            field.name()
        )))
    }

    fn build_missing_column_array(
        field: &FieldRef,
        iceberg_schema: &IcebergSchema,
        num_rows: usize,
    ) -> Result<ArrayRef, DataFusionError> {
        let iceberg_field = iceberg_schema.field_by_name(field.name()).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Column '{}' missing from Iceberg schema during alignment",
                field.name()
            ))
        })?;

        if let Some(array) = Self::default_array_for_field(iceberg_field.as_ref(), num_rows)? {
            return Ok(array);
        }

        if field.is_nullable() {
            return Ok(new_null_array(field.data_type(), num_rows));
        }

        Err(DataFusionError::Plan(format!(
            "Column '{}' is required but missing in input batch and has no default value",
            field.name()
        )))
    }

    fn default_array_for_field(
        field: &NestedField,
        num_rows: usize,
    ) -> Result<Option<ArrayRef>, DataFusionError> {
        if let Some(lit) = field.write_default.as_ref() {
            let scalar = to_scalar(lit, field.field_type.as_ref())?;
            let array = scalar
                .to_array_of_size(num_rows)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            return Ok(Some(array));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, ListArray, StructArray};
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType, Field, Fields};

    use super::*;
    use crate::datasource::type_converter::iceberg_schema_to_arrow;
    use crate::spec::types::{ListType, PrimitiveLiteral, PrimitiveType, StructType, Type};

    #[test]
    fn new_writes_do_not_fallback_to_initial_defaults() -> Result<()> {
        let initial_only = NestedField::required(1, "value", Type::Primitive(PrimitiveType::Int))
            .with_initial_default(Literal::Primitive(PrimitiveLiteral::Int(42)));

        assert!(IcebergTableWriter::default_array_for_field(&initial_only, 1)?.is_none());
        Ok(())
    }

    #[test]
    fn new_writes_apply_write_defaults() -> Result<()> {
        let field = NestedField::required(1, "value", Type::Primitive(PrimitiveType::Int))
            .with_initial_default(Literal::Primitive(PrimitiveLiteral::Int(42)))
            .with_write_default(Literal::Primitive(PrimitiveLiteral::Int(99)));

        let default_array = IcebergTableWriter::default_array_for_field(&field, 2)?
            .ok_or_else(|| DataFusionError::Execution("missing write default array".to_string()))?;
        let array = default_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| DataFusionError::Execution("write default is not int32".to_string()))?;
        assert_eq!(array.values(), &[99, 99]);
        Ok(())
    }

    #[test]
    fn new_writes_apply_nested_write_defaults() -> Result<()> {
        let payload_type = StructType::new(vec![
            Arc::new(NestedField::required(
                2,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(
                NestedField::required(3, "score", Type::Primitive(PrimitiveType::Int))
                    .with_write_default(Literal::Primitive(PrimitiveLiteral::Int(99))),
            ),
        ]);
        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "payload",
                Type::Struct(payload_type),
            ))])
            .build()
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        let table_schema = Arc::new(iceberg_schema_to_arrow(&iceberg_schema)?);

        let input_fields: Fields = vec![Arc::new(Field::new("id", DataType::Int32, false))].into();
        let input_payload = Arc::new(StructArray::new(
            input_fields.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
            None,
        ));
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(input_fields),
            false,
        )]));
        let input = RecordBatch::try_new(input_schema, vec![input_payload])?;

        let padded = IcebergTableWriter::align_batch_with_table_schema(
            &input,
            &table_schema,
            &iceberg_schema,
        )?;
        let aligned = cast_record_batch_relaxed_tz(&padded, &table_schema)?;
        let payload = aligned
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("payload is not a struct".to_string()))?;
        let score = payload
            .column_by_name("score")
            .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| DataFusionError::Execution("score is not int32".to_string()))?;
        assert_eq!(score.values(), &[99, 99]);
        Ok(())
    }

    #[test]
    fn new_writes_apply_defaults_inside_list_elements() -> Result<()> {
        let element_type = StructType::new(vec![
            Arc::new(NestedField::required(
                3,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(
                NestedField::required(4, "score", Type::Primitive(PrimitiveType::Int))
                    .with_write_default(Literal::Primitive(PrimitiveLiteral::Int(99))),
            ),
        ]);
        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "items",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    2,
                    Type::Struct(element_type),
                    false,
                )))),
            ))])
            .build()
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        let table_schema = Arc::new(iceberg_schema_to_arrow(&iceberg_schema)?);

        let source_struct_fields: Fields =
            vec![Arc::new(Field::new("id", DataType::Int32, false))].into();
        let source_values = Arc::new(StructArray::new(
            source_struct_fields.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
            None,
        ));
        let source_element = Arc::new(Field::new(
            "item",
            DataType::Struct(source_struct_fields),
            false,
        ));
        let source_items = Arc::new(ListArray::try_new(
            source_element.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
            source_values,
            None,
        )?);
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(source_element),
            false,
        )]));
        let input = RecordBatch::try_new(input_schema, vec![source_items])?;

        let padded = IcebergTableWriter::align_batch_with_table_schema(
            &input,
            &table_schema,
            &iceberg_schema,
        )?;
        let aligned = cast_record_batch_relaxed_tz(&padded, &table_schema)?;
        let items = aligned
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("items is not a list".to_string()))?;
        let elements = items
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("list element is not a struct".to_string())
            })?;
        let score = elements
            .column_by_name("score")
            .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| DataFusionError::Execution("score is not int32".to_string()))?;
        assert_eq!(score.values(), &[99, 99]);
        Ok(())
    }
}
