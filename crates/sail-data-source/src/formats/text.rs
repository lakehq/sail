use std::any::Any;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{GetExt, Result, Statistics};
use object_store::{ObjectMeta, ObjectStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TableTextOptions {
    pub whole_text: Option<bool>,
    pub line_sep: Option<char>,
    pub compression: Option<String>,
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
        self.options.whole_text = Some(enable);
        self
    }

    pub fn whole_text(&self) -> Option<bool> {
        self.options.whole_text
    }

    pub fn with_line_sep(mut self, line_sep: char) -> Self {
        self.options.line_sep = Some(line_sep);
        self
    }

    pub fn line_sep(&self) -> Option<char> {
        self.options.line_sep
    }

    pub fn with_compression(mut self, compression: String) -> Self {
        self.options.compression = Some(compression);
        self
    }

    pub fn compression(&self) -> Option<String> {
        self.options.compression
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
        // CHECK HERE DO NOT MERGE. Need validation for compression type.
        self.options
            .compression
            .as_ref()
            .and_then(|c| FileCompressionType::from_str(c).ok())
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
