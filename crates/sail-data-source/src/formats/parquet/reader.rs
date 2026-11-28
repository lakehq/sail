// SPDX-License-Identifier: Apache-2.0

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::basic::{ConvertedType, Type as PhysicalType};
use datafusion::parquet::errors::{ParquetError, Result};
use datafusion::parquet::file::metadata::page_index::PageIndexBuilder;
use datafusion::parquet::file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataBuilder};
use datafusion::parquet::schema::types::{SchemaDescriptor, Type, TypePtr};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use sail_common_datafusion::parquet_schema::SAIL_PARQUET_STORAGE_METADATA_KEY;

/// Reads the original byte sequences for decimal columns projected as binary or string.
/// The Arrow decimal decoder discards redundant leading sign-extension bytes.
#[derive(Debug)]
pub struct ListingParquetReaderFactory(pub Arc<dyn ParquetFileReaderFactory>);

impl ParquetFileReaderFactory for ListingParquetReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion_common::Result<Box<dyn AsyncFileReader + Send>> {
        let mut raw_columns = Vec::new();
        if let Some(schema) = &file.arrow_schema {
            for (index, field) in schema
                .flattened_fields()
                .into_iter()
                .filter(|field| !field.data_type().is_nested())
                .enumerate()
            {
                if field
                    .metadata()
                    .get(SAIL_PARQUET_STORAGE_METADATA_KEY)
                    .is_some_and(|ty| ty == "BYTE_ARRAY")
                {
                    raw_columns.push(index);
                }
            }
        }
        let inner = self
            .0
            .create_reader(partition_index, file, metadata_size_hint, metrics)?;
        if raw_columns.is_empty() {
            return Ok(inner);
        }
        Ok(Box::new(RawDecimalReader { inner, raw_columns }))
    }
}

struct RawDecimalReader {
    inner: Box<dyn AsyncFileReader + Send>,
    raw_columns: Vec<usize>,
}

impl AsyncFileReader for RawDecimalReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = self.inner.get_metadata(options).await?;
            raw_decimal_metadata(&metadata, &self.raw_columns).map(Arc::new)
        })
    }
}

fn raw_decimal_metadata(
    metadata: &ParquetMetaData,
    raw_columns: &[usize],
) -> Result<ParquetMetaData> {
    let file = metadata.file_metadata();
    let schema = file.schema_descr();
    // Arrow and Parquet traverse nested leaves in the same order, including MAP and LIST wrappers.
    let mut leaf = 0;
    let root = raw_decimal_schema(&schema.root_schema_ptr(), raw_columns, &mut leaf)?;
    let file = FileMetaData::new(
        file.version(),
        file.num_rows(),
        file.created_by().map(str::to_owned),
        file.key_value_metadata().cloned(),
        Arc::new(SchemaDescriptor::new(root)),
        file.column_orders().cloned(),
    );
    // Decimal min/max use signed numeric order. They cannot prune binary or string projections.
    // Clear the on-disk page index locations as the reader can load indexes after this call.
    let mut builder = ParquetMetaDataBuilder::new(file);
    for group in metadata.row_groups() {
        let mut group = group.clone().into_builder();
        let mut columns = group.take_columns();
        for &index in raw_columns {
            let column = columns.get_mut(index).ok_or_else(|| {
                ParquetError::General(format!(
                    "Missing Parquet column {index} for raw decimal read"
                ))
            })?;
            *column = column
                .clone()
                .into_builder()
                .clear_statistics()
                .set_column_index_offset(None)
                .set_column_index_length(None)
                .build()?;
        }
        builder = builder.add_row_group(
            group
                .set_column_metadata(columns)
                .set_sorting_columns(None)
                .build()?,
        );
    }
    if let Some(indexes) = metadata.page_index() {
        let num_columns = schema.num_columns();
        let mut page_index = PageIndexBuilder::try_new(metadata.num_row_groups(), num_columns)?;
        for row_group in 0..metadata.num_row_groups() {
            for column in 0..num_columns {
                if !raw_columns.contains(&column)
                    && let Some(index) = indexes.column_index(row_group, column)
                {
                    page_index.try_put_column_index(index.clone(), row_group, column)?;
                }
                if let Some(index) = indexes.offset_index(row_group, column) {
                    page_index.try_put_offset_index(index.clone(), row_group, column)?;
                }
            }
        }
        builder = builder.set_page_index(Some(Arc::new(page_index.build())));
    }
    Ok(builder.build())
}

fn raw_decimal_schema(node: &TypePtr, raw_columns: &[usize], leaf: &mut usize) -> Result<TypePtr> {
    match node.as_ref() {
        Type::GroupType { basic_info, fields } => {
            let fields = fields
                .iter()
                .map(|field| raw_decimal_schema(field, raw_columns, leaf))
                .collect::<Result<_>>()?;
            Ok(Arc::new(Type::GroupType {
                basic_info: basic_info.clone(),
                fields,
            }))
        }
        Type::PrimitiveType {
            basic_info,
            physical_type,
            ..
        } => {
            let index = *leaf;
            *leaf += 1;
            if raw_columns.binary_search(&index).is_err() {
                return Ok(Arc::clone(node));
            }
            if *physical_type != PhysicalType::BYTE_ARRAY
                || basic_info.converted_type() != ConvertedType::DECIMAL
            {
                return Err(ParquetError::General(format!(
                    "Expected BYTE_ARRAY DECIMAL for raw Parquet column {index}"
                )));
            }
            Ok(Arc::new(
                Type::primitive_type_builder(node.name(), *physical_type)
                    .with_repetition(basic_info.repetition())
                    .with_id(basic_info.has_id().then(|| basic_info.id()))
                    .build()?,
            ))
        }
    }
}
