use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion_common::internal_datafusion_err;
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::to_arrow;

use crate::catalog::{DatabaseStatus, TableColumnStatus, TableStatus};

/// A trait for displaying catalog objects in a structured format.
/// This is useful for defining output schemas and producing outputs
/// for various SQL catalog commands.
pub trait CatalogObjectDisplay {
    type Catalog: Serialize + for<'de> Deserialize<'de>;
    type Database: Serialize + for<'de> Deserialize<'de>;
    type Table: Serialize + for<'de> Deserialize<'de>;
    type TableColumn: Serialize + for<'de> Deserialize<'de>;
    type Function: Serialize + for<'de> Deserialize<'de>;

    fn catalog(name: String) -> Self::Catalog;

    fn database(status: DatabaseStatus) -> Self::Database;

    fn table(status: TableStatus) -> Self::Table;

    fn table_column(status: TableColumnStatus) -> Self::TableColumn;

    fn function(name: String) -> Self::Function;
}

/// A trait for providing output display for catalog command.
/// Different from [`CatalogObjectDisplay`], this trait provides
/// a higher-level abstraction and is dyn-compatible, so that
/// an implementation can be stored in the session context
/// as a trait object.
pub trait CatalogDisplay: Send + Sync {
    fn empty(&self) -> Box<dyn OutputDisplay<()>>;
    fn bools(&self) -> Box<dyn OutputDisplay<bool>>;
    fn strings(&self) -> Box<dyn OutputDisplay<String>>;
    fn catalogs(&self) -> Box<dyn OutputDisplay<String>>;
    fn tables(&self) -> Box<dyn OutputDisplay<TableStatus>>;
    fn databases(&self) -> Box<dyn OutputDisplay<DatabaseStatus>>;
    fn table_columns(&self) -> Box<dyn OutputDisplay<TableColumnStatus>>;
    fn functions(&self) -> Box<dyn OutputDisplay<String>>;
}

pub trait OutputDisplay<T> {
    fn schema(&self) -> Result<SchemaRef>;
    fn to_record_batch(&self, values: Vec<T>) -> Result<RecordBatch>;
}

struct MappedOutputDisplay<T, U, F> {
    phantom: std::marker::PhantomData<(T, U)>,
    mapper: F,
}

impl<T, U, F> MappedOutputDisplay<T, U, F> {
    fn new(mapper: F) -> Self {
        Self {
            phantom: std::marker::PhantomData,
            mapper,
        }
    }
}

impl<T, U, F> OutputDisplay<T> for MappedOutputDisplay<T, U, F>
where
    U: Serialize + for<'de> Deserialize<'de>,
    F: Fn(T) -> U,
{
    fn schema(&self) -> Result<SchemaRef> {
        build_schema::<U>()
    }

    fn to_record_batch(&self, items: Vec<T>) -> Result<RecordBatch> {
        let schema = self.schema()?;
        let items = items.into_iter().map(&self.mapper).collect::<Vec<_>>();
        build_record_batch(schema, &items)
    }
}

#[derive(Default)]
pub struct DefaultCatalogDisplay<D> {
    phantom: std::marker::PhantomData<D>,
}

impl<D> CatalogDisplay for DefaultCatalogDisplay<D>
where
    D: CatalogObjectDisplay + Send + Sync + 'static,
{
    fn empty(&self) -> Box<dyn OutputDisplay<()>> {
        Box::new(<MappedOutputDisplay<(), _, _>>::new(|()| EmptyOutput {}))
    }

    fn bools(&self) -> Box<dyn OutputDisplay<bool>> {
        Box::new(MappedOutputDisplay::<bool, _, _>::new(|value| {
            SingleValueOutput { value }
        }))
    }

    fn strings(&self) -> Box<dyn OutputDisplay<String>> {
        Box::new(MappedOutputDisplay::<String, _, _>::new(|value| {
            SingleValueOutput { value }
        }))
    }

    fn catalogs(&self) -> Box<dyn OutputDisplay<String>> {
        Box::new(<MappedOutputDisplay<String, D::Catalog, _>>::new(
            D::catalog,
        ))
    }

    fn tables(&self) -> Box<dyn OutputDisplay<TableStatus>> {
        Box::new(<MappedOutputDisplay<TableStatus, D::Table, _>>::new(
            D::table,
        ))
    }

    fn databases(&self) -> Box<dyn OutputDisplay<DatabaseStatus>> {
        Box::new(<MappedOutputDisplay<DatabaseStatus, D::Database, _>>::new(
            D::database,
        ))
    }

    fn table_columns(&self) -> Box<dyn OutputDisplay<TableColumnStatus>> {
        Box::new(<MappedOutputDisplay<TableColumnStatus, D::TableColumn, _>>::new(D::table_column))
    }

    fn functions(&self) -> Box<dyn OutputDisplay<String>> {
        Box::new(<MappedOutputDisplay<String, D::Function, _>>::new(
            D::function,
        ))
    }
}

#[derive(Serialize, Deserialize)]
pub struct EmptyOutput {}

#[derive(Serialize, Deserialize)]
pub struct SingleValueOutput<T> {
    pub value: T,
}

fn build_schema<T>() -> Result<SchemaRef>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
        .map_err(|e| internal_datafusion_err!("{e}"))?;
    Ok(Arc::new(Schema::new(fields)))
}

fn build_record_batch<T>(schema: SchemaRef, items: &[T]) -> Result<RecordBatch>
where
    T: Serialize,
{
    let arrays = to_arrow(schema.fields().deref(), items)
        .map_err(|e| internal_datafusion_err!("failed to create record batch: {e}"))?;
    // We must specify the row count if the schema has no fields.
    let options = RecordBatchOptions::new().with_row_count(Some(items.len()));
    let batch = RecordBatch::try_new_with_options(schema, arrays, &options)
        .map_err(|e| internal_datafusion_err!("failed to create record batch: {e}"))?;
    Ok(batch)
}
