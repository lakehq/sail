use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::Constraints;
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan};

use crate::error::{CatalogError, CatalogResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct Namespace {
    pub head: Arc<str>,
    pub tail: Vec<Arc<str>>,
}

impl From<Namespace> for Vec<Arc<str>> {
    fn from(namespace: Namespace) -> Self {
        let mut result = vec![namespace.head];
        result.extend(namespace.tail);
        result
    }
}

impl From<Namespace> for Vec<String> {
    fn from(namespace: Namespace) -> Self {
        let mut result = vec![namespace.head.to_string()];
        result.extend(namespace.tail.iter().map(|s| s.to_string()));
        result
    }
}

impl<T: Into<Arc<str>>> TryFrom<Vec<T>> for Namespace {
    type Error = CatalogError;

    fn try_from(value: Vec<T>) -> CatalogResult<Self> {
        let mut iter = value.into_iter().map(Into::into);
        let head = iter
            .next()
            .ok_or_else(|| CatalogError::InvalidArgument("empty namespace".to_string()))?;
        let tail = iter.collect();
        Ok(Self { head, tail })
    }
}

impl<T: AsRef<str>> TryFrom<&[T]> for Namespace {
    type Error = CatalogError;

    fn try_from(value: &[T]) -> CatalogResult<Self> {
        let mut iter = value.iter().map(AsRef::as_ref);
        let head = iter
            .next()
            .ok_or_else(|| CatalogError::InvalidArgument("empty namespace".to_string()))?
            .into();
        let tail = iter.map(|s| s.into()).collect();
        Ok(Self { head, tail })
    }
}

impl<T: AsRef<str>> PartialEq<&[T]> for Namespace {
    fn eq(&self, other: &&[T]) -> bool {
        let mut iter = other.iter();
        iter.next()
            .is_some_and(|x| x.as_ref() == self.head.as_ref())
            && iter
                .map(|x| x.as_ref())
                .eq(self.tail.iter().map(|x| x.as_ref()))
    }
}

#[derive(Debug, Clone)]
pub struct NamespaceMetadata {
    pub catalog: String,
    pub namespace: Vec<String>,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct CreateNamespaceOptions {
    pub if_not_exists: bool,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DeleteNamespaceOptions {
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTableOptions {
    pub schema: SchemaRef,
    pub file_format: String,
    pub if_not_exists: bool,
    pub or_replace: bool,
    pub comment: Option<String>,
    pub location: Option<String>,
    pub column_defaults: Vec<(String, Expr)>,
    pub constraints: Constraints,
    pub table_partition_cols: Vec<String>,
    pub file_sort_order: Vec<Vec<Sort>>,
    pub definition: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DeleteTableOptions {
    pub if_exists: bool,
    pub purge: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableKind {
    Table {
        catalog: String,
        namespace: Vec<String>,
        schema: SchemaRef,
        format: String,
        comment: Option<String>,
        location: Option<String>,
        properties: Vec<(String, String)>,
    },
    View {
        catalog: String,
        namespace: Vec<String>,
        schema: SchemaRef,
        definition: String,
        comment: Option<String>,
        location: Option<String>,
        properties: Vec<(String, String)>,
    },
    TemporaryView {
        plan: Arc<LogicalPlan>,
    },
    GlobalTemporaryView {
        namespace: Vec<String>,
        plan: Arc<LogicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub kind: TableKind,
}

impl TableMetadata {
    pub fn catalog(&self) -> Option<String> {
        match &self.kind {
            TableKind::Table { catalog, .. } => Some(catalog.clone()),
            TableKind::View { catalog, .. } => Some(catalog.clone()),
            TableKind::TemporaryView { .. } => None,
            TableKind::GlobalTemporaryView { .. } => None,
        }
    }

    pub fn namespace(&self) -> Vec<String> {
        match &self.kind {
            TableKind::Table { namespace, .. } => namespace.clone(),
            TableKind::View { namespace, .. } => namespace.clone(),
            TableKind::TemporaryView { .. } => vec![],
            TableKind::GlobalTemporaryView { namespace, .. } => namespace.clone(),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match &self.kind {
            TableKind::Table { schema, .. } => schema.clone(),
            TableKind::View { schema, .. } => schema.clone(),
            TableKind::TemporaryView { plan } => plan.schema().inner().clone(),
            TableKind::GlobalTemporaryView { plan, .. } => plan.schema().inner().clone(),
        }
    }

    pub fn description(&self) -> Option<String> {
        match &self.kind {
            TableKind::Table { comment, .. } => comment.clone(),
            TableKind::View { comment, .. } => comment.clone(),
            TableKind::TemporaryView { .. } => None,
            TableKind::GlobalTemporaryView { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableColumnMetadata {
    pub name: String,
    pub description: Option<String>,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_partition: bool,
    pub is_bucket: bool,
    pub metadata: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct CreateViewOptions {
    pub definition: String,
    pub schema: SchemaRef,
    pub replace: bool,
    pub comment: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DeleteViewOptions {
    pub if_exists: bool,
}

#[async_trait::async_trait]
pub trait CatalogProvider: Send + Sync {
    async fn create_namespace(
        &self,
        namespace: &Namespace,
        options: CreateNamespaceOptions,
    ) -> CatalogResult<()>;

    async fn delete_namespace(
        &self,
        namespace: &Namespace,
        options: DeleteNamespaceOptions,
    ) -> CatalogResult<()>;

    async fn get_namespace(&self, namespace: &Namespace) -> CatalogResult<NamespaceMetadata>;

    async fn list_namespaces(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<NamespaceMetadata>>;

    async fn create_table(
        &self,
        namespace: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<()>;

    async fn get_table(&self, namespace: &Namespace, table: &str) -> CatalogResult<TableMetadata>;

    async fn list_tables(&self, namespace: &Namespace) -> CatalogResult<Vec<TableMetadata>>;

    async fn delete_table(
        &self,
        namespace: &Namespace,
        table: &str,
        options: DeleteTableOptions,
    ) -> CatalogResult<()>;

    async fn create_view(
        &self,
        namespace: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<()>;

    async fn get_view(&self, namespace: &Namespace, view: &str) -> CatalogResult<TableMetadata>;

    async fn list_views(&self, namespace: &Namespace) -> CatalogResult<Vec<TableMetadata>>;

    async fn delete_view(
        &self,
        namespace: &Namespace,
        view: &str,
        options: DeleteViewOptions,
    ) -> CatalogResult<()>;
}
