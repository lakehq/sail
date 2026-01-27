use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr_common::sort_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{not_impl_err, GetExt, Result, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::TableSchema;
use datafusion_session::Session;
use object_store::{ObjectMeta, ObjectStore};

use crate::formats::text::source::TextSource;
use crate::formats::text::writer::{TextSink, TextWriterOptions};
use crate::formats::text::{TableTextOptions, DEFAULT_TEXT_EXTENSION};

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

    #[allow(unused)]
    pub fn options(&self) -> &TableTextOptions {
        &self.options
    }

    #[allow(unused)]
    pub fn with_whole_text(mut self, enable: bool) -> Self {
        self.options.whole_text = enable;
        self
    }

    #[allow(unused)]
    pub fn whole_text(&self) -> bool {
        self.options.whole_text
    }

    #[allow(unused)]
    pub fn with_line_sep(mut self, line_sep: char) -> Self {
        self.options.line_sep = Some(line_sep);
        self
    }

    #[allow(unused)]
    pub fn line_sep(&self) -> Option<char> {
        self.options.line_sep
    }

    #[allow(unused)]
    pub fn with_compression(mut self, compression: CompressionTypeVariant) -> Self {
        self.options.compression = compression;
        self
    }

    #[allow(unused)]
    pub fn compression(&self) -> CompressionTypeVariant {
        self.options.compression
    }
}

#[async_trait::async_trait]
impl FileFormat for TextFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        DEFAULT_TEXT_EXTENSION[1..].to_string()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
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
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_compression_type(FileCompressionType::from(self.options.compression))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Text files");
        }
        let writer_options = TextWriterOptions::try_from(&self.options)?;
        let sink = Arc::new(TextSink::new(conf, writer_options));
        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        let line_sep = self.options.line_sep.map(|c| c as u8);
        Arc::new(TextSource::new(
            table_schema,
            self.options.whole_text,
            line_sep,
        ))
    }
}
