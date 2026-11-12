use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, execute_stream,
};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err, internal_err,
};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use percent_encoding::percent_decode_str;
use sail_common_datafusion::error::CommitStateUnknownError;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Publishes completed worker manifests on the coordinator. Copies precede all
/// deletes, but this is not a multi-object transaction or concurrent-writer isolation.
/// Durable operation records fence retries after an interrupted publication.
#[derive(Debug)]
pub struct FilePartitionCommitExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    table_path: Path,
    staging_path: Path,
    partition_columns: Vec<String>,
    partition_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FilePartitionCommitExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        table_path: Path,
        staging_path: Path,
        partition_columns: Vec<String>,
        partition_schema: SchemaRef,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            object_store_url,
            table_path,
            staging_path,
            partition_columns,
            partition_schema,
            properties,
        }
    }
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }
    pub fn table_path(&self) -> &Path {
        &self.table_path
    }
    pub fn staging_path(&self) -> &Path {
        &self.staging_path
    }
    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }
    pub fn partition_schema(&self) -> &SchemaRef {
        &self.partition_schema
    }
}

impl DisplayAs for FilePartitionCommitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}: table={}, staging={}, partition_columns={:?}",
            self.name(),
            self.table_path,
            self.staging_path,
            self.partition_columns
        )
    }
}

impl ExecutionPlan for FilePartitionCommitExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("FilePartitionCommitExec requires one child");
        };
        Ok(Arc::new(Self::new(
            input.clone(),
            self.object_store_url.clone(),
            self.table_path.clone(),
            self.staging_path.clone(),
            self.partition_columns.clone(),
            self.partition_schema.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("FilePartitionCommitExec expects partition 0, got {partition}");
        }
        validate_paths(
            &self.table_path,
            &self.staging_path,
            &self.partition_columns,
        )?;
        let input = self.input.clone();
        let object_store_url = self.object_store_url.clone();
        let table = self.table_path.clone();
        let staging = self.staging_path.clone();
        let columns = self.partition_columns.clone();
        let partition_schema = self.partition_schema.clone();
        let stream = futures::stream::once(async move {
            let store = context.runtime_env().object_store(&object_store_url)?;
            let result = async {
                // Exhaust all completions and final stream status before any mutation.
                let batches = execute_stream(input, context)?
                    .try_collect::<Vec<_>>()
                    .await?;
                let mut files = HashSet::new();
                for batch in batches {
                    let [column] = batch.columns() else {
                        return exec_err!("invalid staged Parquet manifest column count");
                    };
                    let paths = column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| exec_datafusion_err!("invalid staged Parquet manifest"))?;
                    if paths.null_count() != 0 {
                        return exec_err!("null path in staged Parquet manifest");
                    }
                    for path in paths.iter().flatten() {
                        files.insert(Path::parse(path)?);
                    }
                }
                commit_partitions(
                    store.as_ref(),
                    &table,
                    &staging,
                    &columns,
                    &partition_schema,
                    files.into_iter().collect(),
                )
                .await
            }
            .await;
            // Keep records for replay. Retain successful attempt files on failure:
            // scheduler retries may reuse their manifests, or recovery may need them.
            if result.is_ok()
                && let Err(error) =
                    cleanup_staging(store.as_ref(), &staging.clone().join("attempts")).await
            {
                log::warn!("failed to clean listing write staging path {staging}: {error}");
            }
            result?;
            Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Match decoded complete keys, never raw path prefixes. Different encoders may
/// spell the same partition as `=` or `%3D` without changing its identity.
pub(super) fn partition_key(path: &Path, root: &Path, columns: &[String]) -> Option<Vec<String>> {
    let parts = path.prefix_match(root)?.collect::<Vec<_>>();
    if parts.len() < columns.len() + 1 {
        return None;
    }
    parts
        .iter()
        .zip(columns)
        .map(|(part, column)| {
            let (name, value) = part.as_ref().split_once('=')?;
            if percent_decode_str(name).decode_utf8().ok()? != column.as_str() {
                return None;
            }
            Some(percent_decode_str(value).decode_utf8().ok()?.into_owned())
        })
        .collect()
}

// [CREDIT]: https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/catalog/ExternalCatalogUtils.scala#L44-L98
fn escape_partition(value: &str) -> String {
    let mut escaped = String::new();
    for c in value.chars() {
        if matches!(
            c,
            '\u{1}'
                ..='\u{1f}'
                    | '\u{7f}'
                    | '"'
                    | '#'
                    | '%'
                    | '\''
                    | '*'
                    | '/'
                    | ':'
                    | '='
                    | '?'
                    | '\\'
                    | '{'
                    | '['
                    | ']'
                    | '^'
        ) {
            use std::fmt::Write;
            let _ = write!(escaped, "%{:02X}", c as u32);
        } else {
            escaped.push(c);
        }
    }
    escaped
}

pub(super) fn canonical_partition_path(
    root: &Path,
    columns: &[String],
    values: &[String],
) -> Result<Path> {
    if columns.len() != values.len() {
        return exec_err!("partition column/value count mismatch");
    }
    let mut path = root.to_string();
    for (column, value) in columns.iter().zip(values) {
        if !path.is_empty() {
            path.push('/');
        }
        path.push_str(&escape_partition(column));
        path.push('=');
        path.push_str(&escape_partition(value));
    }
    Ok(Path::parse(path)?)
}

fn validate_paths(table: &Path, staging: &Path, columns: &[String]) -> Result<()> {
    if columns.is_empty() || staging.prefix_matches(table) || table.prefix_matches(staging) {
        return exec_err!("invalid partition overwrite staging path or partition columns");
    }
    Ok(())
}

fn unknown(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::External(Box::new(CommitStateUnknownError::from(error.to_string())))
}

async fn exists(store: &dyn ObjectStore, path: &Path) -> Result<bool> {
    match store.head(path).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[derive(Serialize, Deserialize)]
struct CommitRecord {
    table: String,
    old_files: Vec<String>,
    copies: Vec<(String, String)>,
}

fn typed_partition_key(values: &[String], schema: &Schema) -> Result<Vec<ScalarValue>> {
    if values.len() != schema.fields().len() {
        return exec_err!("partition key/schema length mismatch");
    }
    values
        .iter()
        .zip(schema.fields())
        .map(|(value, field)| {
            if value == "__HIVE_DEFAULT_PARTITION__" {
                ScalarValue::try_from(field.data_type())
            } else {
                ScalarValue::try_from_string(value.clone(), field.data_type())
            }
        })
        .collect()
}

async fn commit_partitions(
    store: &dyn ObjectStore,
    table: &Path,
    staging: &Path,
    columns: &[String],
    partition_schema: &Schema,
    files: Vec<Path>,
) -> Result<()> {
    validate_paths(table, staging, columns)?;
    let started = staging.clone().join("commit.json");
    let complete = staging.clone().join("complete");
    if exists(store, &complete).await.map_err(unknown)? {
        return Ok(());
    }
    if exists(store, &started).await.map_err(unknown)? {
        return Err(unknown(format!(
            "partition overwrite {staging} was started without a completion record"
        )));
    }
    let attempts = staging.clone().join("attempts");
    let mut touched = HashSet::new();
    let mut copies = Vec::with_capacity(files.len());
    for source in files {
        let relative = source
            .prefix_match(&attempts)
            .ok_or_else(|| {
                exec_datafusion_err!("manifest file outside staging attempts: {source}")
            })?
            .collect::<Vec<_>>();
        if relative.len() != columns.len() + 2 {
            return exec_err!("invalid staged partition depth: {source}");
        }
        let attempt_root = attempts.clone().join(relative[0].clone());
        let key = partition_key(&source, &attempt_root, columns)
            .ok_or_else(|| exec_datafusion_err!("invalid staged partition file: {source}"))?;
        let filename = source
            .filename()
            .ok_or_else(|| exec_datafusion_err!("missing staged Parquet filename: {source}"))?;
        let destination = canonical_partition_path(table, columns, &key)?
            .join(format!("{}-{filename}", Uuid::new_v4()));
        touched.insert(typed_partition_key(&key, partition_schema)?);
        copies.push((source, destination));
    }
    let old = if touched.is_empty() {
        vec![]
    } else {
        store
            .list(Some(table))
            .map_err(DataFusionError::from)
            .try_filter_map(|file| {
                let result = (|| {
                    let Some(key) = partition_key(&file.location, table, columns) else {
                        return Ok(None);
                    };
                    let key = typed_partition_key(&key, partition_schema)?;
                    Ok(touched.contains(&key).then_some(file.location))
                })();
                futures::future::ready(result)
            })
            .try_collect::<Vec<_>>()
            .await?
    };
    let record = CommitRecord {
        table: table.to_string(),
        old_files: old.iter().map(ToString::to_string).collect(),
        copies: copies
            .iter()
            .map(|(s, d)| (s.to_string(), d.to_string()))
            .collect(),
    };
    let payload = serde_json::to_vec(&record)
        .map_err(|e| exec_datafusion_err!("failed to encode listing commit: {e}"))?;
    // A durable, create-only fence precedes publication. An interrupted/uncertain
    // operation must be inspected rather than retried with a fresh old-file snapshot.
    store
        .put_opts(
            &started,
            payload.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .map_err(unknown)?;
    publish_files(store, copies).await.map_err(unknown)?;
    // Never roll back new files after deletion has begun.
    for path in old {
        store.delete(&path).await.map_err(unknown)?;
    }
    store
        .put_opts(
            &complete,
            bytes::Bytes::from_static(b"complete").into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .map_err(unknown)?;
    Ok(())
}

async fn publish_files(store: &dyn ObjectStore, copies: Vec<(Path, Path)>) -> Result<()> {
    let mut published = Vec::with_capacity(copies.len());
    for (source, destination) in copies {
        published.push(destination.clone());
        if let Err(error) = store.copy(&source, &destination).await {
            for path in &published {
                if let Err(rollback) = store.delete(path).await {
                    log::warn!("failed to roll back listing write object {path}: {rollback}");
                }
            }
            return Err(error.into());
        }
    }
    Ok(())
}

pub(super) async fn cleanup_staging(store: &dyn ObjectStore, staging: &Path) -> Result<()> {
    let files = store
        .list(Some(staging))
        .map_ok(|file| file.location)
        .boxed();
    store
        .delete_stream(files)
        .try_for_each(|_| futures::future::ready(Ok(())))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};
    use object_store::memory::InMemory;

    use super::*;

    fn string_schema(columns: &[String]) -> Schema {
        Schema::new(
            columns
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn typed_keys_preserve_string_identity_and_match_float_aliases() -> Result<()> {
        for dt in [DataType::Float32, DataType::Float64] {
            let schema = Schema::new(vec![Field::new("p", dt, true)]);
            assert_eq!(
                typed_partition_key(&["1".into()], &schema)?,
                typed_partition_key(&["1.0".into()], &schema)?
            );
        }
        let schema = string_schema(&["p".into()]);
        assert_ne!(
            typed_partition_key(&["1".into()], &schema)?,
            typed_partition_key(&["1.0".into()], &schema)?
        );
        Ok(())
    }

    #[test]
    fn complete_partition_keys() -> Result<()> {
        let root = Path::from("table");
        let columns = vec!["p".into(), "q".into()];
        for path in ["table/p=a%2Fb%3Dc/q=one/file", "table/p=a%2Fb=c/q=one/file"] {
            assert_eq!(
                partition_key(&Path::parse(path)?, &root, &columns),
                Some(vec!["a/b=c".into(), "one".into()])
            );
        }
        for path in [
            "table/p=a/file",
            "table/p=a/r=one/file",
            "table2/p=a/q=one/file",
        ] {
            assert_eq!(partition_key(&Path::parse(path)?, &root, &columns), None);
        }
        assert_eq!(escape_partition("a/b=c%?#'é"), "a%2Fb%3Dc%25%3F%23%27é");
        Ok(())
    }

    #[tokio::test]
    async fn failed_publish_rolls_back_without_deleting_old_files() -> Result<()> {
        let store = InMemory::new();
        let old = Path::from("table/p=one/old.parquet");
        let staged = Path::from("staging/p=one/new.parquet");
        let published = Path::from("table/p=one/new.parquet");
        for path in [&old, &staged] {
            store
                .put(path, bytes::Bytes::from_static(b"data").into())
                .await?;
        }
        assert!(
            publish_files(
                &store,
                vec![
                    (staged, published.clone()),
                    (
                        Path::from("staging/missing"),
                        Path::from("table/p=one/second.parquet")
                    )
                ]
            )
            .await
            .is_err()
        );
        assert!(store.head(&published).await.is_err());
        assert_eq!(store.get(&old).await?.bytes().await?, "data");
        Ok(())
    }

    #[tokio::test]
    async fn manifest_excludes_orphans_and_commit_replay_is_safe() -> Result<()> {
        let store = InMemory::new();
        let table = Path::from("table");
        let staging = Path::from("staging");
        let columns = vec!["p".into(), "q".into()];
        for path in [
            "table/p=a%3Db/q=one/old.parquet",
            "table/p=a%3Db/q=two/keep.parquet",
            "staging/attempts/failed/p=other/q=one/orphan.parquet",
        ] {
            store
                .put(
                    &Path::parse(path)?,
                    bytes::Bytes::from_static(b"old").into(),
                )
                .await?;
        }
        let source = Path::from("staging/attempts/success/p=a=b/q=one/new.gz.parquet");
        store
            .put(&source, bytes::Bytes::from_static(b"new").into())
            .await?;
        commit_partitions(
            &store,
            &table,
            &staging,
            &columns,
            &string_schema(&columns),
            vec![source.clone()],
        )
        .await?;
        let files = store.list(Some(&table)).try_collect::<Vec<_>>().await?;
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|file| {
            file.location
                .filename()
                .is_some_and(|name| name.ends_with("-new.gz.parquet"))
        }));
        assert!(
            !files
                .iter()
                .any(|f| f.location.as_ref().ends_with("old.parquet"))
        );
        commit_partitions(
            &store,
            &table,
            &staging,
            &columns,
            &string_schema(&columns),
            vec![source],
        )
        .await?;
        assert_eq!(
            store.list(Some(&table)).try_collect::<Vec<_>>().await?,
            files
        );
        assert_eq!(
            store
                .get(&Path::parse("table/p=a%3Db/q=two/keep.parquet")?)
                .await?
                .bytes()
                .await?,
            "old"
        );
        Ok(())
    }

    #[tokio::test]
    async fn empty_manifest_does_not_touch_table_objects() -> Result<()> {
        let store = InMemory::new();
        let table = Path::from("table");
        let old = Path::from("table/p=1/old.parquet");
        store
            .put(&old, bytes::Bytes::from_static(b"old").into())
            .await?;
        let before = store.head(&old).await?;
        commit_partitions(
            &store,
            &table,
            &Path::from("staging"),
            &["p".into()],
            &string_schema(&["p".into()]),
            vec![],
        )
        .await?;
        assert_eq!(store.head(&old).await?, before);
        assert_eq!(store.get(&old).await?.bytes().await?, "old");
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_commit_is_not_retryable() -> Result<()> {
        let store = InMemory::new();
        store
            .put(
                &Path::from("staging/commit.json"),
                bytes::Bytes::from_static(b"started").into(),
            )
            .await?;
        let error = commit_partitions(
            &store,
            &Path::from("table"),
            &Path::from("staging"),
            &["p".into()],
            &string_schema(&["p".into()]),
            vec![],
        )
        .await
        .err()
        .ok_or_else(|| exec_datafusion_err!("incomplete commit was not fenced"))?;
        assert!(
            matches!(error, DataFusionError::External(error) if error.downcast_ref::<CommitStateUnknownError>().is_some())
        );
        Ok(())
    }
}
