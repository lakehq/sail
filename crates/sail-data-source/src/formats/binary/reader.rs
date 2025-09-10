use std::io::Read;
use std::sync::Arc;

use datafusion::arrow::array::{
    BinaryArray, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;

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
    has_read: bool,
}

impl BinaryFileReader {
    pub fn new(metadata: BinaryFileMetadata, content: Vec<u8>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new(
                "modificationTime",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("length", DataType::Int64, false),
            Field::new("content", DataType::Binary, false),
        ]));

        Self {
            metadata,
            content,
            schema,
            has_read: false,
        }
    }

    pub fn from_reader<R: Read>(
        metadata: BinaryFileMetadata,
        mut reader: R,
    ) -> Result<Self, std::io::Error> {
        let mut content = Vec::new();
        reader.read_to_end(&mut content)?;
        Ok(Self::new(metadata, content))
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.has_read {
            return Ok(None);
        }

        let path_array = StringArray::from(vec![self.metadata.path.as_str()]);
        let modification_time_array =
            TimestampMicrosecondArray::from(vec![self.metadata.modification_time]);
        let length_array = Int64Array::from(vec![self.metadata.length]);
        let content_array = BinaryArray::from_vec(vec![self.content.as_slice()]);
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            vec![
                Arc::new(path_array),
                Arc::new(modification_time_array),
                Arc::new(length_array),
                Arc::new(content_array),
            ],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;

        self.has_read = true;

        Ok(Some(batch))
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_file_reader() -> Result<(), ArrowError> {
        let metadata = BinaryFileMetadata {
            path: "/test/file.pdf".to_string(),
            modification_time: 1234567890000000,
            length: 10,
        };
        let content = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        let mut reader = BinaryFileReader::new(metadata, content);

        let batch = reader.next_batch()?.unwrap();
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

        assert!(reader.next_batch()?.is_none());

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

        let mut png_reader = BinaryFileReader::new(png_metadata, png_content.clone());
        let png_batch = png_reader.next_batch()?.unwrap();

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

        let mut pdf_reader = BinaryFileReader::new(pdf_metadata, pdf_content.clone());
        let pdf_batch = pdf_reader.next_batch()?.unwrap();

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

        let mut jpeg_reader = BinaryFileReader::new(jpeg_metadata, jpeg_content.clone());
        let jpeg_batch = jpeg_reader.next_batch()?.unwrap();

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

        let mut reader = BinaryFileReader::new(metadata, content);
        let batch = reader.next_batch()?.unwrap();

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
