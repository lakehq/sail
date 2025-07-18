use serde::{Deserialize, Serialize};

use crate::provider::{NamespaceStatus, TableColumnStatus, TableStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmptyDisplay {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleValueDisplay<T> {
    pub value: T,
}

pub trait CatalogDisplay {
    type Catalog: Serialize + for<'de> Deserialize<'de>;
    type Database: Serialize + for<'de> Deserialize<'de>;
    type Table: Serialize + for<'de> Deserialize<'de>;
    type TableColumn: Serialize + for<'de> Deserialize<'de>;
    type Function: Serialize + for<'de> Deserialize<'de>;

    fn catalog(name: String) -> Self::Catalog;

    fn database(status: NamespaceStatus) -> Self::Database;

    fn table(status: TableStatus) -> Self::Table;

    fn table_column(status: TableColumnStatus) -> Self::TableColumn;
}
