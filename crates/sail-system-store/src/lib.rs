//! Storage for Sail system tables.
//!
//! The system store is an in-process implementation detail. Persistent backends are intended to
//! be opened by one Sail process at a time, and their on-disk format has no compatibility
//! guarantee across Sail versions.

mod access;
mod actor;
pub mod backend;
mod engine;
mod error;
mod event;
mod handle;
mod model;
pub mod predicate;
mod reader;
pub mod types;

pub mod catalog {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};

    include!(concat!(env!("OUT_DIR"), "/system.catalog.rs"));
    include!(concat!(env!("OUT_DIR"), "/system.database.rs"));
    include!(concat!(env!("OUT_DIR"), "/system.table.rs"));
    include!(concat!(env!("OUT_DIR"), "/system.row.rs"));

    impl SystemTableColumn {
        pub fn field(&self) -> Field {
            let field = Field::new(self.name, self.arrow_type.clone(), self.nullable);
            if self.sql_type == "VARIANT" {
                field.with_metadata(crate::types::variant_field_metadata())
            } else {
                field
            }
        }
    }

    impl SystemTable {
        pub fn schema(&self) -> SchemaRef {
            let fields = self
                .columns()
                .iter()
                .map(|column| Arc::new(column.field()))
                .collect::<Vec<_>>();
            Arc::new(Schema::new(fields))
        }
    }
}

pub use engine::MetricSample;
pub(crate) use engine::SystemStoreQuery;
pub use error::{SystemStoreError, SystemStoreResult};
pub use event::SystemEvent;
pub use handle::SystemStoreHandle;
pub use reader::SystemStoreReader;
