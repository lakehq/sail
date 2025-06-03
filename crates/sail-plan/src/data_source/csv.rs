use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
use datafusion_expr::SortExpr;

/// Datasource Options that control the reading of CSV files.
/// Reference: [`datafusion::datasource::file_format::options::CsvReadOptions`]
#[derive(Clone)]
pub struct CsvReadOptions<'a> {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
    pub comment: Option<u8>,
    pub header: bool,
    pub null_value: Option<String>,
    pub null_regex: Option<String>,
    pub line_sep: Option<u8>,
    pub schema_infer_max_records: usize,
    pub newlines_in_values: bool,
    pub file_extension: &'a str,
    pub compression: FileCompressionType,
}
