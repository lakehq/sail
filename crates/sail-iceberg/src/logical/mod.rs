pub(crate) mod delete;
pub mod merge;
pub(crate) mod row_level;
pub mod table_source;
pub(crate) mod update;

pub use table_source::IcebergTableSource;
