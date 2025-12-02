use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_datasource::file_format::FileFormat;

use crate::formats::csv::options::{resolve_csv_read_options, resolve_csv_write_options};
use crate::formats::listing::{ListingFormat, ListingTableFormat};

mod options;

pub type CsvTableFormat = ListingTableFormat<CsvListingFormat>;

#[derive(Debug, Default)]
pub struct CsvListingFormat;

impl ListingFormat for CsvListingFormat {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn create_read_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
        compression: Option<CompressionTypeVariant>,
    ) -> datafusion_common::Result<Arc<dyn FileFormat>> {
        let mut options = resolve_csv_read_options(ctx, options)?;
        if let Some(compression) = compression {
            options.compression = compression;
        }
        Ok(Arc::new(CsvFormat::default().with_options(options)))
    }

    fn create_write_format(
        &self,
        ctx: &dyn Session,
        options: Vec<HashMap<String, String>>,
    ) -> datafusion_common::Result<(Arc<dyn FileFormat>, Option<String>)> {
        let options = resolve_csv_write_options(ctx, options)?;
        Ok((Arc::new(CsvFormat::default().with_options(options)), None))
    }
}
