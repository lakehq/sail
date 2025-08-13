use std::any::Any;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DataFusionError, GetExt, Result, Statistics};
use futures::stream::BoxStream;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use object_store::{delimited::newline_delimited_stream, ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub struct TableTextOptions {
    pub whole_text: bool,
    pub line_sep: Option<char>,
    pub compression: CompressionTypeVariant,
}

impl Default for TableTextOptions {
    fn default() -> Self {
        Self {
            whole_text: false,
            line_sep: None,
            compression: CompressionTypeVariant::UNCOMPRESSED,
        }
    }
}

#[derive(Debug)]
pub struct TextFileFormat {
    options: TableTextOptions,
}

impl TextFileFormat {
    pub fn new(table_text_options: TableTextOptions) -> Self {
        Self {
            options: table_text_options,
        }
    }

    pub fn options(&self) -> &TableTextOptions {
        &self.options
    }

    pub fn with_whole_text(mut self, enable: bool) -> Self {
        self.options.whole_text = enable;
        self
    }

    pub fn whole_text(&self) -> bool {
        self.options.whole_text
    }

    pub fn with_line_sep(mut self, line_sep: char) -> Self {
        self.options.line_sep = Some(line_sep);
        self
    }

    pub fn line_sep(&self) -> Option<char> {
        self.options.line_sep
    }

    pub fn with_compression(mut self, compression: CompressionTypeVariant) -> Self {
        self.options.compression = compression;
        self
    }

    pub fn compression(&self) -> CompressionTypeVariant {
        self.options.compression
    }

    async fn read_to_delimited_chunks<'a>(
        &self,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> BoxStream<'a, Result<Bytes>> {
        let stream = store
            .get(&object.location)
            .await
            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)));
        let stream = match stream {
            Ok(stream) => self
                .read_to_delimited_chunks_from_stream(
                    stream
                        .into_stream()
                        .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))
                        .boxed(),
                )
                .await
                .map_err(DataFusionError::from)
                .left_stream(),
            Err(e) => futures::stream::once(futures::future::ready(Err(e))).right_stream(),
        };
        stream.boxed()
    }

    pub async fn read_to_delimited_chunks_from_stream<'a>(
        &self,
        stream: BoxStream<'a, Result<Bytes>>,
    ) -> BoxStream<'a, Result<Bytes>> {
        let file_compression_type: FileCompressionType = self.options.compression.into();
        let decoder = file_compression_type.convert_stream(stream);
        let stream = match decoder {
            Ok(decoded_stream) => newline_delimited_stream(decoded_stream.map_err(|e| match e {
                DataFusionError::ObjectStore(e) => *e,
                err => object_store::Error::Generic {
                    store: "read to delimited chunks failed",
                    source: Box::new(err),
                },
            }))
            .map_err(DataFusionError::from)
            .left_stream(),
            Err(e) => futures::stream::once(futures::future::ready(Err(e))).right_stream(),
        };
        stream.boxed()
    }
}

#[async_trait::async_trait]
impl FileFormat for TextFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "txt".to_string()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        // CHECK HERE DO NOT MERGE. Make sure supported compression matches data source option doc.
        Ok(format!("{ext}{}", file_compression_type.get_ext()))
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        Some(self.options.compression.into())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let schema = SchemaRef::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "value",
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            ),
        ]));
        Ok(schema)
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
    }

    fn file_source(&self) -> Arc<dyn FileSource> {}
}
