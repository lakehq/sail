use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
use datafusion_expr::SortExpr;

/// Datasource Options that control the reading of CSV files.
/// Reference: [`datafusion::datasource::file_format::options::CsvReadOptions`]
#[derive(Clone)]
pub struct CsvReadOptions<'a> {
    /// If true, uses the first line as the names of the columns.
    pub has_header: bool,
    /// An optional column delimiter. Sets a separator that can be one or more characters for each
    /// field and value. Defaults to `,`.
    pub delimiter: u8,
    /// An optional quote character. Sets a single character used for escaping quoted values where
    /// the delimiter can be part of the value. Defaults to `"`.
    pub quote: u8,
    /// An optional terminator character. Defines the line separator to be used for parsing.
    /// Defaults to None (CRLF).
    pub terminator: Option<u8>,
    /// An optional escape character. Sets a single character used for escaping quotes inside an
    /// already quoted value. Defaults to `\`.
    pub escape: Option<u8>,
    /// If enabled, sets a single character used for skipping lines that begin with this character.
    /// Defaults to None (disabled).
    pub comment: Option<u8>,
    /// Specifies whether newlines in quoted values are supported.
    ///
    /// Parsing newlines in quoted values may be affected by execution behavior such as
    /// parallel file scanning. Setting this to `true` ensures that newlines in values are
    /// parsed successfully, which may reduce performance.
    pub newlines_in_values: bool,
    /// An optional schema representing the CSV files. If None, the CSV reader will try to infer it
    /// based on the data in the files.
    pub schema: Option<&'a Schema>,
    /// The maximum number of rows to read from CSV files for schema inference if needed.
    /// Defaults to [`DEFAULT_SCHEMA_INFER_MAX_RECORD`].
    pub schema_infer_max_records: usize,
    /// Only files with this extension are selected for data input. Defaults to `.csv`.
    pub file_extension: &'a str,
    /// The expected partition column names in the folder structure.
    /// See [`datafusion::datasource::listing::table::ListingOptions::with_table_partition_cols`]
    /// for more details.
    pub table_partition_cols: Vec<(String, DataType)>,
    /// Specifies the file compression type. Defaults to `UNCOMPRESSED`.
    pub file_compression_type: FileCompressionType,
    /// Indicates how the file is sorted.
    /// See [`datafusion::datasource::listing::table::ListingOptions`] for more details.
    pub file_sort_order: Vec<Vec<SortExpr>>,
    /// Optional regex to match null values. Defaults to treating empty values as null.
    pub null_regex: Option<String>,
}

impl Default for CsvReadOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> CsvReadOptions<'a> {
    pub fn new() -> Self {
        Self {
            has_header: false,
            schema: None,
            schema_infer_max_records: DEFAULT_SCHEMA_INFER_MAX_RECORD,
            delimiter: b',',
            quote: b'"',
            terminator: None,
            escape: Some(b'\\'),
            newlines_in_values: false,
            file_extension: ".csv",
            table_partition_cols: vec![],
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            file_sort_order: vec![],
            comment: None,
            null_regex: None,
        }
    }
}
