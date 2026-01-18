use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion_common::arrow::array::ArrayData;

use crate::formats::binary::time_zone_from_read_schema;

#[derive(Debug, Clone)]
pub struct BinaryFileMetadata {
    pub path: String,
    pub modification_time: i64, // microseconds since epoch
    pub length: i64,
}

pub struct BinaryFileReader {
    metadata: BinaryFileMetadata,
    content: Vec<u8>,
    schema: SchemaRef,
}

impl BinaryFileReader {
    pub fn new(metadata: BinaryFileMetadata, content: Vec<u8>, schema: SchemaRef) -> Self {
        Self {
            metadata,
            content,
            schema,
        }
    }

    pub fn read(self) -> Result<RecordBatch, ArrowError> {
        let mut path_array: Option<ArrayRef> = None;
        let mut modification_time_array: Option<ArrayRef> = None;
        let mut length_array: Option<ArrayRef> = None;
        let mut content_array: Option<ArrayRef> = None;
        let mut content_bytes = Some(self.content);

        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            match field.name().as_str() {
                "path" => {
                    let arr = path_array.get_or_insert_with(|| {
                        Arc::new(StringArray::from(vec![self.metadata.path.as_str()])) as _
                    });
                    columns.push(Arc::clone(arr));
                }
                "modificationTime" => {
                    let tz = match field.data_type() {
                        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => tz.clone(),
                        _ => time_zone_from_read_schema(&self.schema)?,
                    };
                    let arr = modification_time_array.get_or_insert_with(|| {
                        Arc::new(
                            TimestampMicrosecondArray::from(vec![self.metadata.modification_time])
                                .with_timezone(tz),
                        ) as _
                    });
                    columns.push(Arc::clone(arr));
                }
                "length" => {
                    let arr = length_array.get_or_insert_with(|| {
                        Arc::new(Int64Array::from(vec![self.metadata.length])) as _
                    });
                    columns.push(Arc::clone(arr));
                }
                "content" => {
                    if content_array.is_none() {
                        let content = content_bytes.take().unwrap_or_default();
                        // create a binary array without copying the content
                        let size = i32::try_from(content.len()).map_err(|e| {
                            ArrowError::ComputeError(format!("file content size too large: {}", e))
                        })?;
                        let array_data = ArrayData::builder(DataType::Binary)
                            .len(1)
                            .add_buffer(vec![0, size].into())
                            .add_buffer(content.into())
                            .build()?;
                        content_array = Some(Arc::new(BinaryArray::from(array_data)) as _);
                    }
                    columns.push(
                        content_array
                            .as_ref()
                            .ok_or_else(|| {
                                ArrowError::ComputeError(
                                    "content_array should be initialized before use".to_string(),
                                )
                            })?
                            .clone(),
                    );
                }
                other => {
                    return Err(ArrowError::ParseError(format!(
                        "Unexpected field '{other}' in BinaryFile schema"
                    )));
                }
            }
        }

        let batch = RecordBatch::try_new_with_options(
            self.schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;

        Ok(batch)
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::binary::read_schema;

    #[test]
    fn test_binary_file_reader() -> Result<(), ArrowError> {
        let metadata = BinaryFileMetadata {
            path: "/test/file.pdf".to_string(),
            modification_time: 1234567890000000,
            length: 10,
        };
        let content = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let schema = read_schema(Arc::from("UTC"));
        let reader = BinaryFileReader::new(metadata, content, schema);

        let batch = reader.read()?;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);

        let path_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path_col.value(0), "/test/file.pdf");

        let time_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(time_col.value(0), 1234567890000000);

        let length_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(length_col.value(0), 10);

        let content_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(content_col.value(0), &[0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        Ok(())
    }

    #[test]
    fn test_multiple_file_formats() -> Result<(), ArrowError> {
        let png_metadata = BinaryFileMetadata {
            path: "/images/photo.png".to_string(),
            modification_time: 1700000000000000,
            length: 8,
        };
        let png_content = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];

        let schema = read_schema(Arc::from("UTC"));
        let png_reader = BinaryFileReader::new(png_metadata, png_content.clone(), schema);
        let png_batch = png_reader.read()?;

        let path_col = png_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path_col.value(0), "/images/photo.png");

        let content_col = png_batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(content_col.value(0), png_content.as_slice());

        let pdf_metadata = BinaryFileMetadata {
            path: "/documents/report.pdf".to_string(),
            modification_time: 1700001000000000,
            length: 9,
        };
        let pdf_content = vec![0x25, 0x50, 0x44, 0x46, 0x2D, 0x31, 0x2E, 0x34, 0x0A];

        let schema = read_schema(Arc::from("UTC"));
        let pdf_reader = BinaryFileReader::new(pdf_metadata, pdf_content.clone(), schema);
        let pdf_batch = pdf_reader.read()?;

        let path_col = pdf_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path_col.value(0), "/documents/report.pdf");

        let length_col = pdf_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(length_col.value(0), 9);

        let content_col = pdf_batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(content_col.value(0), pdf_content.as_slice());

        let jpeg_metadata = BinaryFileMetadata {
            path: "/photos/image.jpeg".to_string(),
            modification_time: 1700002000000000,
            length: 12,
        };
        let jpeg_content = vec![
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01,
        ];

        let schema = read_schema(Arc::from("UTC"));
        let jpeg_reader = BinaryFileReader::new(jpeg_metadata, jpeg_content.clone(), schema);
        let jpeg_batch = jpeg_reader.read()?;

        let path_col = jpeg_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path_col.value(0), "/photos/image.jpeg");

        let time_col = jpeg_batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(time_col.value(0), 1700002000000000);

        let content_col = jpeg_batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(content_col.value(0), jpeg_content.as_slice());

        Ok(())
    }

    #[test]
    fn test_empty_file() -> Result<(), ArrowError> {
        let metadata = BinaryFileMetadata {
            path: "/empty/file.bin".to_string(),
            modification_time: 1600000000000000,
            length: 0,
        };
        let content = vec![];

        let schema = read_schema(Arc::from("UTC"));
        let reader = BinaryFileReader::new(metadata, content, schema);
        let batch = reader.read()?;

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);

        let path_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path_col.value(0), "/empty/file.bin");

        let length_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(length_col.value(0), 0);

        let content_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(content_col.value(0).len(), 0);

        Ok(())
    }
}
