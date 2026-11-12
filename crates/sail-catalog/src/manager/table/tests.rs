use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use sail_common_datafusion::catalog::{
    CatalogPartitionField, DatabaseStatus, PartitionTransform, TableColumnStatus,
};

use super::*;
use crate::manager::CatalogManagerOptions;
use crate::provider::{
    CatalogProvider, CreateDatabaseOptions, CreateTableMode, CreateViewOptions,
    DropDatabaseOptions, DropViewOptions,
};

/// Returns historical metadata unchanged, including on create. Normalization must happen
/// above the provider rather than depending on a particular catalog's persistence rules.
struct HistoricalProvider {
    tables: Vec<TableStatus>,
}

#[async_trait]
impl CatalogProvider for HistoricalProvider {
    fn get_name(&self) -> &str {
        "test"
    }

    async fn create_database(
        &self,
        _: &Namespace,
        _: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        unreachable!()
    }

    async fn get_database(&self, _: &Namespace) -> CatalogResult<DatabaseStatus> {
        unreachable!()
    }

    async fn list_databases(&self, _: Option<&Namespace>) -> CatalogResult<Vec<DatabaseStatus>> {
        unreachable!()
    }

    async fn drop_database(&self, _: &Namespace, _: DropDatabaseOptions) -> CatalogResult<()> {
        unreachable!()
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        _: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        self.get_table(database, table).await
    }

    async fn get_table(&self, _: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        self.tables
            .iter()
            .find(|status| status.name == table)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(CatalogObject::Table, table.into()))
    }

    async fn list_tables(&self, _: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        Ok(self.tables.clone())
    }

    async fn drop_table(&self, _: &Namespace, _: &str, _: DropTableOptions) -> CatalogResult<()> {
        unreachable!()
    }

    async fn alter_table(&self, _: &Namespace, _: &str, _: AlterTableOptions) -> CatalogResult<()> {
        unreachable!()
    }

    async fn create_view(
        &self,
        _: &Namespace,
        _: &str,
        _: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        unreachable!()
    }

    async fn get_view(&self, _: &Namespace, _: &str) -> CatalogResult<TableStatus> {
        unreachable!()
    }

    async fn list_views(&self, _: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        unreachable!()
    }

    async fn drop_view(&self, _: &Namespace, _: &str, _: DropViewOptions) -> CatalogResult<()> {
        unreachable!()
    }
}

fn table_status(name: &str, format: &str, names: &[&str], partition_names: &[&str]) -> TableStatus {
    TableStatus {
        catalog: Some("test".into()),
        database: vec!["default".into()],
        name: name.into(),
        kind: TableKind::Table {
            columns: names
                .iter()
                .map(|name| TableColumnStatus {
                    name: (*name).into(),
                    metadata: [("custom".into(), (*name).into())].into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    comment: Some(format!("comment for {name}")),
                    default: Some("42".into()),
                    generated_always_as: None,
                    identity: None,
                    is_partition: partition_names.contains(name),
                    is_bucket: false,
                    is_cluster: false,
                })
                .collect(),
            comment: Some("table comment".into()),
            constraints: vec![],
            location: Some("file:///historical/table".into()),
            format: format.into(),
            partition_by: partition_names
                .iter()
                .map(|name| CatalogPartitionField {
                    column: (*name).into(),
                    transform: None,
                })
                .collect(),
            sort_by: vec![],
            bucket_by: None,
            properties: vec![("custom".into(), "retained".into())],
            is_external: true,
        },
    }
}

fn create_options() -> CreateTableOptions {
    CreateTableOptions {
        columns: vec![],
        comment: None,
        constraints: vec![],
        location: None,
        format: "parquet".into(),
        partition_by: vec![],
        sort_by: vec![],
        bucket_by: None,
        mode: CreateTableMode::Create,
        properties: vec![],
        is_external: true,
        is_write_precondition: false,
    }
}

fn manager(provider: Arc<HistoricalProvider>) -> CatalogResult<CatalogManager> {
    CatalogManager::try_new(CatalogManagerOptions {
        catalogs: [("test".into(), provider as Arc<dyn CatalogProvider>)].into(),
        default_catalog: "test".into(),
        default_database: vec!["default".into()],
        global_temporary_database: vec!["global_temp".into()],
    })
}

fn assert_status(actual: &TableStatus, expected: &TableStatus) {
    assert_eq!(actual.catalog, expected.catalog);
    assert_eq!(actual.database, expected.database);
    assert_eq!(actual.name, expected.name);
    assert_eq!(actual.kind, expected.kind);
}

#[tokio::test]
async fn all_table_status_exits_normalize_historical_parquet_metadata() -> CatalogResult<()> {
    let mut cases = vec![];
    for format in ["parquet", "PARQUET", "delta", "iceberg", "csv"] {
        let original = table_status(format, format, &["a", "p", "b"], &["p"]);
        let mut expected = original.clone();
        if format.eq_ignore_ascii_case("parquet")
            && let TableKind::Table { columns, .. } = &mut expected.kind
        {
            columns.swap(1, 2);
        }
        cases.push((original, expected));
    }
    let original = table_status("multi", "parquet", &["q", "a", "p", "b"], &["p", "q"]);
    let mut expected = original.clone();
    if let TableKind::Table { columns, .. } = &mut expected.kind {
        *columns = [1, 3, 2, 0].map(|index| columns[index].clone()).to_vec();
    }
    cases.push((original, expected));
    for (name, names, partition) in [
        ("case_distinct", vec!["p", "P", "msg"], "p"),
        ("fallback", vec!["PART", "a", "b"], "part"),
    ] {
        let original = table_status(name, "parquet", &names, &[partition]);
        let mut expected = original.clone();
        if let TableKind::Table { columns, .. } = &mut expected.kind {
            columns.rotate_left(1);
        }
        cases.push((original, expected));
    }
    let original = table_status("unpartitioned", "parquet", &["a", "p", "b"], &[]);
    cases.push((original.clone(), original));
    // Schema inference may be deferred until the data source is opened.
    let original = table_status("inferred", "parquet", &[], &["p"]);
    cases.push((original.clone(), original));
    let mut transformed = table_status("transformed", "parquet", &["a", "p", "b"], &["p"]);
    if let TableKind::Table { partition_by, .. } = &mut transformed.kind {
        partition_by[0].transform = Some(PartitionTransform::Bucket(4));
    }
    cases.push((transformed.clone(), transformed));
    let mut view = table_status("view", "parquet", &["a", "p", "b"], &["p"]);
    view.kind = TableKind::View {
        columns: view.kind.columns(),
        definition: "SELECT 1 AS a, 2 AS p, 3 AS b".into(),
        comment: Some("view comment".into()),
        properties: vec![],
    };
    cases.push((view.clone(), view));
    let provider = Arc::new(HistoricalProvider {
        tables: cases.iter().map(|(original, _)| original.clone()).collect(),
    });
    let manager = manager(provider.clone())?;
    for (original, expected) in &cases {
        let name = [original.name.as_str()];
        assert_status(
            &manager.create_table(&name, create_options()).await?,
            expected,
        );
        assert_status(&manager.get_table(&name).await?, expected);
        let reference = manager.resolve_table_reference_with_default_catalog("test", &name)?;
        assert_status(&manager.get_table_by_reference(&reference).await?, expected);
    }
    let listed = manager.list_tables(&["default"], None).await?;
    assert_eq!(listed.len(), cases.len());
    for (actual, (original, expected)) in listed.iter().zip(&cases) {
        assert_status(actual, expected);
        // Exposure does not migrate or mutate the provider's stored metadata.
        assert_status(
            &provider
                .get_table(&Namespace::try_from(vec!["default"])?, &original.name)
                .await?,
            original,
        );
    }
    let filtered = manager.list_tables(&["default"], Some("multi")).await?;
    assert_eq!(filtered.len(), 1);
    assert_status(&filtered[0], &cases[5].1);
    Ok(())
}

#[tokio::test]
async fn all_table_status_exits_reject_ambiguous_partition_fallback() -> CatalogResult<()> {
    let provider = Arc::new(HistoricalProvider {
        tables: vec![table_status(
            "ambiguous",
            "parquet",
            &["PART", "Part", "a"],
            &["part"],
        )],
    });
    let manager = manager(provider)?;
    let name = ["ambiguous"];
    let reference = manager.resolve_table_reference_with_default_catalog("test", &name)?;
    assert!(manager.create_table(&name, create_options()).await.is_err());
    assert!(manager.get_table(&name).await.is_err());
    assert!(manager.get_table_by_reference(&reference).await.is_err());
    assert!(manager.list_tables(&["default"], None).await.is_err());
    Ok(())
}
