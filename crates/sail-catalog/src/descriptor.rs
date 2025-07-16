use serde::{Deserialize, Serialize};

use crate::provider::{NamespaceMetadata, TableColumnMetadata, TableMetadata};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmptyDescriptor {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleValueDescriptor<T> {
    pub value: T,
}

pub trait DescriptorFactory {
    type Catalog: Serialize + for<'de> Deserialize<'de>;
    type Database: Serialize + for<'de> Deserialize<'de>;
    type Table: Serialize + for<'de> Deserialize<'de>;
    type TableColumn: Serialize + for<'de> Deserialize<'de>;
    type Function: Serialize + for<'de> Deserialize<'de>;

    fn catalog(name: String) -> Self::Catalog;

    fn database(metadata: NamespaceMetadata) -> Self::Database;

    fn table(metadata: TableMetadata) -> Self::Table;

    fn table_column(metadata: TableColumnMetadata) -> Self::TableColumn;
}
