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

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use futures::future::BoxFuture;
use object_store::buffered::BufWriter;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::async_writer::{AsyncArrowWriter, AsyncFileWriter};
use parquet::basic::LogicalType;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{SchemaDescriptor, Type, TypePtr};
use parquet_variant_compute::VariantType;
use sail_common_datafusion::schema_evolution::{
    StructFieldMatching, cast_array_with_schema_evolution_relaxed_tz,
};
use tokio::io::AsyncWriteExt;

use crate::datasource::type_converter::iceberg_field_id;

/// Abort uploads abandoned before completion starts.
pub(crate) struct ParquetObjectSink {
    output: Option<BufWriter>,
}

impl ParquetObjectSink {
    pub(crate) fn new(output: BufWriter) -> Self {
        Self {
            output: Some(output),
        }
    }
}

impl AsyncFileWriter for ParquetObjectSink {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            let output = self.output.as_mut().ok_or_else(|| {
                ParquetError::General("Iceberg Parquet sink is closed".to_string())
            })?;
            output.write_all(&bytes).await.map_err(ParquetError::from)
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            let mut output = self.output.take().ok_or_else(|| {
                ParquetError::General("Iceberg Parquet sink is closed".to_string())
            })?;
            output.shutdown().await?;
            Ok(())
        })
    }
}

impl Drop for ParquetObjectSink {
    fn drop(&mut self) {
        if let Some(mut output) = self.output.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                if let Err(error) = output.abort().await {
                    log::warn!("Failed to abort Iceberg Parquet upload: {error}");
                }
            });
        }
    }
}

pub struct ParquetFileMeta {
    pub num_rows: u64,
    pub file_size: u64,
    pub parquet_metadata: ParquetMetaData,
}

pub struct ArrowParquetWriter<W: AsyncFileWriter = Vec<u8>> {
    writer: AsyncArrowWriter<W>,
    storage_schema: SchemaRef,
}

impl<W: AsyncFileWriter> ArrowParquetWriter<W> {
    pub fn try_new(
        schema: &datafusion::arrow::datatypes::Schema,
        props: WriterProperties,
        output: W,
    ) -> Result<Self, String> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| storage_field(field))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let storage_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let parquet_schema = parquet_schema(&storage_schema, &props)?;
        let options = ArrowWriterOptions::new()
            .with_properties(props)
            .with_parquet_schema(parquet_schema);
        let writer =
            AsyncArrowWriter::try_new_with_options(output, storage_schema.clone(), options)
                .map_err(|e| format!("parquet writer error: {e}"))?;
        Ok(Self {
            writer,
            storage_schema,
        })
    }

    pub async fn write_batch(
        &mut self,
        batch: &datafusion::arrow::array::RecordBatch,
    ) -> Result<(), String> {
        let projected;
        let batch = if batch.schema() == self.storage_schema {
            batch
        } else {
            let columns = self
                .storage_schema
                .fields()
                .iter()
                .map(|field| {
                    let source = batch
                        .column_by_name(field.name())
                        .ok_or_else(|| format!("Missing Parquet column '{}'", field.name()))?;
                    cast_array_with_schema_evolution_relaxed_tz(
                        source,
                        field,
                        &Default::default(),
                        StructFieldMatching::Name,
                    )
                    .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            projected = RecordBatch::try_new_with_options(
                self.storage_schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )
            .map_err(|error| error.to_string())?;
            &projected
        };
        self.writer
            .write(batch)
            .await
            .map_err(|e| format!("parquet write: {e}"))
    }

    pub fn estimated_size(&self) -> u64 {
        self.writer
            .bytes_written()
            .saturating_add(self.writer.in_progress_size()) as u64
    }

    pub async fn close(mut self) -> Result<(W, ParquetFileMeta), String> {
        let metadata = self
            .writer
            .finish()
            .await
            .map_err(|e| format!("parquet finish: {e}"))?;
        let file_size = self.writer.bytes_written() as u64;
        let num_rows = metadata.file_metadata().num_rows() as u64;
        Ok((
            self.writer.into_inner(),
            ParquetFileMeta {
                num_rows,
                file_size,
                parquet_metadata: metadata,
            },
        ))
    }
}

fn parquet_schema(
    schema: &Schema,
    properties: &WriterProperties,
) -> Result<SchemaDescriptor, String> {
    let mut variant_ids = HashSet::new();
    for field in schema.flattened_fields() {
        if field.has_valid_extension_type::<VariantType>() {
            let field_id = iceberg_field_id(field)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    format!("Missing Iceberg field ID for Variant '{}'", field.name())
                })?;
            variant_ids.insert(field_id);
        }
    }
    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(properties.coerce_types())
        .convert(schema)
        .map_err(|error| error.to_string())?;
    if variant_ids.is_empty() {
        return Ok(parquet_schema);
    }
    // Field IDs identify Variant groups through Parquet's list/map wrapper groups.
    Ok(SchemaDescriptor::new(annotate_variant_groups(
        &parquet_schema.root_schema_ptr(),
        &variant_ids,
    )?))
}

fn annotate_variant_groups(
    parquet_type: &TypePtr,
    variant_ids: &HashSet<i32>,
) -> Result<TypePtr, String> {
    if parquet_type.is_primitive() {
        return Ok(parquet_type.clone());
    }
    let info = parquet_type.get_basic_info();
    let logical_type = if info.has_id() && variant_ids.contains(&info.id()) {
        Some(LogicalType::variant(None))
    } else {
        info.logical_type_ref().cloned()
    };
    let fields = parquet_type
        .get_fields()
        .iter()
        .map(|field| annotate_variant_groups(field, variant_ids))
        .collect::<Result<Vec<_>, _>>()?;
    let mut builder = Type::group_type_builder(info.name())
        .with_fields(fields)
        .with_converted_type(info.converted_type())
        .with_logical_type(logical_type)
        .with_id(info.has_id().then(|| info.id()));
    if info.has_repetition() {
        builder = builder.with_repetition(info.repetition());
    }
    builder
        .build()
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

fn storage_field(field: &Field) -> Result<Option<Field>, String> {
    let data_type = match field.data_type() {
        DataType::Null => return Ok(None),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| storage_field(field))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => {
            let element = Arc::new(storage_field(element)?.ok_or_else(|| {
                "Cannot write an Iceberg list with unknown elements to Parquet".to_string()
            })?);
            match field.data_type() {
                DataType::LargeList(_) => DataType::LargeList(element),
                DataType::FixedSizeList(_, size) => DataType::FixedSizeList(element, *size),
                _ => DataType::List(element),
            }
        }
        DataType::Map(entries, sorted) => {
            let entries = storage_field(entries)?.ok_or("Missing Iceberg map entry type")?;
            if !matches!(entries.data_type(), DataType::Struct(fields) if fields.len() == 2) {
                return Err(
                    "Cannot write an Iceberg map with unknown keys or values to Parquet"
                        .to_string(),
                );
            }
            DataType::Map(Arc::new(entries), *sorted)
        }
        other => other.clone(),
    };
    Ok(Some(field.clone().with_data_type(data_type)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::basic::Repetition;

    use super::*;

    #[test]
    fn variant_annotations_follow_field_ids_through_nested_types() -> Result<(), String> {
        let field = |name: &str, data_type: DataType, nullable: bool, id: i32| {
            Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };
        for shredded in [false, true] {
            let storage_type = DataType::Struct(
                vec![
                    Field::new("metadata", DataType::Binary, false),
                    if shredded {
                        Field::new("typed_value", DataType::Int64, true)
                    } else {
                        Field::new("value", DataType::Binary, false)
                    },
                ]
                .into(),
            );
            let variant = |name: &str, id: i32| {
                field(name, storage_type.clone(), true, id).with_extension_type(VariantType)
            };
            let schema = Schema::new(vec![
                variant("payload", 1),
                field(
                    "wrapper",
                    DataType::Struct(vec![variant("payload", 3)].into()),
                    true,
                    2,
                ),
                field(
                    "items",
                    DataType::List(Arc::new(variant("element", 5))),
                    true,
                    4,
                ),
                field(
                    "properties",
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![field("key", DataType::Utf8, false, 7), variant("value", 8)]
                                    .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    true,
                    6,
                ),
                field("plain_struct", storage_type, true, 9),
            ]);
            let parquet = parquet_schema(&schema, &WriterProperties::builder().build())?;
            let roots = parquet.root_schema().get_fields();
            let variants = [
                (&roots[0], 1),
                (&roots[1].get_fields()[0], 3),
                (&roots[2].get_fields()[0].get_fields()[0], 5),
                (&roots[3].get_fields()[0].get_fields()[1], 8),
            ];
            for (variant, id) in variants {
                let info = variant.get_basic_info();
                assert_eq!(info.id(), id);
                assert_eq!(info.repetition(), Repetition::OPTIONAL);
                assert_eq!(info.logical_type_ref(), Some(&LogicalType::variant(None)));
                assert_eq!(variant.get_fields()[0].name(), "metadata");
            }
            assert_eq!(roots[1].get_basic_info().logical_type_ref(), None);
            assert_eq!(
                roots[2].get_basic_info().logical_type_ref(),
                Some(&LogicalType::List)
            );
            assert_eq!(
                roots[3].get_basic_info().logical_type_ref(),
                Some(&LogicalType::Map)
            );
            assert_eq!(roots[4].get_basic_info().logical_type_ref(), None);
        }
        Ok(())
    }
}
