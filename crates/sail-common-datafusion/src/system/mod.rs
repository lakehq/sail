pub mod observable;
pub mod predicate;
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
            Field::new(self.name, self.arrow_type.clone(), self.nullable)
        }
    }

    impl SystemTable {
        pub fn schema(&self) -> SchemaRef {
            let fields = self
                .columns()
                .iter()
                .map(|col| Arc::new(col.field()))
                .collect::<Vec<_>>();
            Arc::new(Schema::new(fields))
        }
    }
}
