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

use crate::listing::internal::{STAGING_DIRECTORY, is_reserved_path};

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
        .map(|(part, column)| decode_partition_component(part.as_ref(), column))
        .collect()
}

fn decode_partition_component(part: &str, column: &str) -> Option<String> {
    let (name, value) = part.split_once('=')?;
    if percent_decode_str(name).decode_utf8().ok()? != column {
        return None;
    }
    let value = percent_decode_str(value).decode_utf8().ok()?;
    // Legacy empty partition paths have the same identity as Hive null partitions.
    Some(if value.is_empty() {
        "__HIVE_DEFAULT_PARTITION__".into()
    } else {
        value.into_owned()
    })
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
    let reserved_operation = staging.prefix_match(table).is_some_and(|parts| {
        let parts = parts.collect::<Vec<_>>();
        matches!(parts.as_slice(), [reserved, id]
            if reserved.as_ref() == STAGING_DIRECTORY
                && Uuid::parse_str(id.as_ref()).is_ok_and(|uuid| uuid.to_string() == id.as_ref()))
    });
    if columns.is_empty()
        || table.prefix_matches(staging)
        || (staging.prefix_matches(table) && !reserved_operation)
    {
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
        .map(|(value, field)| typed_partition_value(value, field.data_type()))
        .collect()
}

fn typed_partition_value(
    value: &str,
    data_type: &arrow::datatypes::DataType,
) -> Result<ScalarValue> {
    if value == "__HIVE_DEFAULT_PARTITION__" {
        ScalarValue::try_from(data_type)
    } else {
        ScalarValue::try_from_string(value.to_owned(), data_type)
    }
}

/// Index partial keys once; complete keys keep using the original touched set.
fn touched_partition_prefixes(
    touched: &HashSet<Vec<ScalarValue>>,
    depth: usize,
) -> Vec<HashSet<Vec<ScalarValue>>> {
    (1..depth)
        .map(|length| touched.iter().map(|key| key[..length].to_vec()).collect())
        .collect()
}

/// Discover actual directory spellings, pruning only classified typed prefixes.
/// Corruption under pruned, unrelated prefixes is not inspected; errors in visited
/// branches propagate. Unclassifiable branches retain the full object matcher.
async fn old_partition_files(
    store: &dyn ObjectStore,
    table: &Path,
    columns: &[String],
    schema: &Schema,
    touched: &HashSet<Vec<ScalarValue>>,
) -> Result<Vec<Path>> {
    if touched.is_empty() {
        return Ok(vec![]);
    }
    if columns.len() != schema.fields().len() {
        return exec_err!("partition column/schema length mismatch");
    }
    let touched_prefixes = touched_partition_prefixes(touched, columns.len());
    let mut old = HashSet::new();
    let mut visited = HashSet::new();
    let mut pending = vec![(table.clone(), vec![])];
    let mut include = |path: Path| -> Result<()> {
        if !is_reserved_path(table, &path)
            && let Some(key) = partition_key(&path, table, columns)
            && touched.contains(&typed_partition_key(&key, schema)?)
        {
            old.insert(path);
        }
        Ok(())
    };
    while let Some((prefix, values)) = pending.pop() {
        if !visited.insert(prefix.clone()) {
            continue;
        }
        let listed = store.list_with_delimiter(Some(&prefix)).await?;
        for meta in listed.objects {
            include(meta.location)?;
        }
        for child in listed.common_prefixes {
            if is_reserved_path(table, &child) {
                continue;
            }
            let classified = child.prefix_match(&prefix).and_then(|mut parts| {
                let part = parts.next()?;
                if parts.next().is_some() {
                    return None;
                }
                let value = decode_partition_component(part.as_ref(), &columns[values.len()])?;
                typed_partition_value(&value, schema.field(values.len()).data_type()).ok()
            });
            if let Some(value) = classified {
                let mut key = values.clone();
                key.push(value);
                if key.len() < columns.len() {
                    if touched_prefixes[key.len() - 1].contains(&key) {
                        pending.push((child, key));
                    }
                    continue;
                }
                if !touched.contains(&key) {
                    continue;
                }
            }
            // Full matched partition, or a branch we cannot soundly classify.
            if !visited.insert(child.clone()) {
                continue;
            }
            let mut files = store.list(Some(&child));
            while let Some(meta) = files.try_next().await? {
                include(meta.location)?;
            }
        }
    }
    let mut old = old.into_iter().collect::<Vec<_>>();
    old.sort_unstable();
    Ok(old)
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
    let old = old_partition_files(store, table, columns, partition_schema, &touched).await?;
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
    // Delete the captured snapshot in one stream, never re-listing published files.
    // Drain even on per-object errors; never roll back once deletion has begun.
    let mut deleted = store.delete_stream(futures::stream::iter(old.into_iter().map(Ok)).boxed());
    let mut deletion_error = None;
    while let Some(result) = deleted.next().await {
        if let Err(error) = result {
            deletion_error.get_or_insert(error);
        }
    }
    if let Some(error) = deletion_error {
        return Err(unknown(error));
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

    fn attempt_file(staging: &Path, attempt: &str) -> Path {
        staging
            .clone()
            .join("attempts")
            .join(attempt)
            .join("p=one")
            .join("new")
    }

    fn string_schema(columns: &[String]) -> Schema {
        Schema::new(
            columns
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn staging_paths_allow_only_exact_reserved_operations_or_legacy_disjoint_paths() -> Result<()> {
        let id = "12345678-1234-4234-abcd-123456789abc";
        let columns = ["p".into()];
        for table in [
            Path::from("table"),
            Path::default(),
            Path::from("local/warehouse/table"),
        ] {
            let stage = table.clone().join(STAGING_DIRECTORY).join(id);
            validate_paths(&table, &stage, &columns)?;
            for suffix in [
                String::new(),
                STAGING_DIRECTORY.into(),
                format!("nested/{STAGING_DIRECTORY}/{id}"),
                format!("%2Esail-staging/{id}"),
                format!("{STAGING_DIRECTORY}/{}", id.to_uppercase()),
                format!("{STAGING_DIRECTORY}/not-a-uuid"),
                format!("{STAGING_DIRECTORY}/{id}/attempts"),
            ] {
                let candidate = Path::parse(format!("{table}/{suffix}"))?;
                assert!(
                    validate_paths(&table, &candidate, &columns).is_err(),
                    "{candidate}"
                );
            }
            assert!(validate_paths(&table, &stage, &[]).is_err());
        }
        validate_paths(&Path::from("table"), &Path::from("legacy-stage"), &columns)?;
        assert!(
            validate_paths(&Path::from("parent/table"), &Path::from("parent"), &columns).is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn discovery_matches_typed_aliases_without_listing_unrelated_objects() -> Result<()> {
        use std::sync::atomic::Ordering;

        use arrow::datatypes::TimeUnit;

        use crate::listing::internal::tests::{Event, RecordingStore};

        for (data_type, column, aliases, value, other) in [
            (
                DataType::Utf8,
                "p=key",
                vec!["p%3Dkey=a=b", "%70%3dkey=%61%3Db", "p%3dkey=a%3db"],
                "a=b",
                "other",
            ),
            (
                DataType::Float64,
                "p",
                vec!["p=1", "%70=1.0", "p=%31.00"],
                "1.0",
                "2.0",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "p",
                vec![
                    "p=2026-01-02 03:04:05",
                    "p=2026-01-02%2003%3A04%3A05.000000",
                ],
                "2026-01-02 03:04:05",
                "2027-01-02 03:04:05",
            ),
            (
                DataType::Utf8,
                "p",
                vec![
                    "p=",
                    "%70=__HIVE_DEFAULT_PARTITION__",
                    "p=%5F_HIVE_DEFAULT_PARTITION__",
                ],
                "__HIVE_DEFAULT_PARTITION__",
                "other",
            ),
        ] {
            let store = RecordingStore::default();
            let table = Path::from("table");
            let columns = [column.into(), "q".into()];
            let schema = Schema::new(vec![
                Field::new(column, data_type, true),
                Field::new("q", DataType::Utf8, true),
            ]);
            let touched =
                HashSet::from([typed_partition_key(&[value.into(), "one".into()], &schema)?]);
            let mut expected = vec![];
            for alias in &aliases {
                let old = Path::parse(format!("table/{alias}/q=one/old"))?;
                store
                    .put(&old, bytes::Bytes::from_static(b"old").into())
                    .await?;
                expected.push(old);
                for index in 0..20 {
                    store
                        .put(
                            &Path::parse(format!("table/{alias}/q=two/{index}"))?,
                            bytes::Bytes::from_static(b"keep").into(),
                        )
                        .await?;
                }
            }
            let unrelated =
                canonical_partition_path(&table, &columns, &[other.into(), "one".into()])?;
            for index in 0..100 {
                store
                    .put(
                        &unrelated.clone().join(index.to_string()),
                        bytes::Bytes::from_static(b"keep").into(),
                    )
                    .await?;
            }
            store
                .put(
                    &Path::from("table/.sail-staging/operation/attempts/file"),
                    bytes::Bytes::from_static(b"stage").into(),
                )
                .await?;
            let old = old_partition_files(&store, &table, &columns, &schema, &touched).await?;
            expected.sort_unstable();
            assert_eq!(old, expected);
            assert_eq!(store.yielded.load(Ordering::SeqCst), aliases.len());
            for event in store
                .events
                .lock()
                .map_err(|e| exec_datafusion_err!("{e}"))?
                .iter()
            {
                if let Event::List(prefix) = event {
                    assert!(expected.iter().any(|path| path.prefix_matches(prefix)));
                    assert_ne!(prefix, &table);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn discovery_indexes_many_touched_and_disjoint_single_column_keys() -> Result<()> {
        use std::sync::atomic::Ordering;

        use crate::listing::internal::tests::{Event, RecordingStore};
        let store = RecordingStore::default();
        let table = Path::from("table");
        let columns = ["p".into()];
        let schema = Schema::new(vec![Field::new("p", DataType::Int32, true)]);
        for index in 0..256 {
            store
                .put(
                    &Path::from(format!("table/p={index}/file")),
                    bytes::Bytes::from_static(b"old").into(),
                )
                .await?;
        }
        for offset in [0, 1000] {
            store.yielded.store(0, Ordering::SeqCst);
            store
                .events
                .lock()
                .map_err(|e| exec_datafusion_err!("{e}"))?
                .clear();
            let touched = (offset..offset + 256)
                .map(|index| vec![ScalarValue::Int32(Some(index))])
                .collect::<HashSet<_>>();
            // Single-column discovery reuses the full-key set, without copying an index.
            assert!(touched_partition_prefixes(&touched, 1).is_empty());
            let old = old_partition_files(&store, &table, &columns, &schema, &touched).await?;
            let expected = if offset == 0 { 256 } else { 0 };
            assert_eq!(old.len(), expected);
            assert_eq!(store.yielded.load(Ordering::SeqCst), expected);
            let events = store
                .events
                .lock()
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, Event::Delimiter(_)))
                    .count(),
                1
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, Event::List(_)))
                    .count(),
                expected
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn discovery_indexes_partial_prefixes_without_cross_matching_siblings() -> Result<()> {
        use std::sync::atomic::Ordering;

        use crate::listing::internal::tests::{Event, RecordingStore};
        let store = RecordingStore::default();
        let table = Path::from("table");
        let columns = ["p".into(), "q".into(), "r".into()];
        let schema = Schema::new(
            columns
                .iter()
                .map(|name| Field::new(name, DataType::Int32, true))
                .collect::<Vec<_>>(),
        );
        let mut expected = vec![];
        for p in 0..64 {
            for q in 0..8 {
                for r in 1..=2 {
                    let path = Path::from(format!("table/p={p}/q={q}/r={r}/file"));
                    store
                        .put(&path, bytes::Bytes::from_static(b"old").into())
                        .await?;
                    if p < 32 && q < 4 && r == 1 {
                        expected.push(path);
                    }
                }
            }
        }
        expected.sort_unstable();
        for offset in [0, 1000] {
            store.yielded.store(0, Ordering::SeqCst);
            store
                .events
                .lock()
                .map_err(|e| exec_datafusion_err!("{e}"))?
                .clear();
            let touched = (0..128)
                .map(|index| {
                    vec![
                        ScalarValue::Int32(Some(offset + index / 4)),
                        ScalarValue::Int32(Some(index % 4)),
                        ScalarValue::Int32(Some(1)),
                    ]
                })
                .collect::<HashSet<_>>();
            let prefixes = touched_partition_prefixes(&touched, 3);
            assert_eq!(
                prefixes.iter().map(HashSet::len).collect::<Vec<_>>(),
                vec![32, 128]
            );
            assert!(prefixes[0].contains(&vec![ScalarValue::Int32(Some(offset))]));
            assert!(!prefixes[1].contains(&vec![
                ScalarValue::Int32(Some(offset)),
                ScalarValue::Int32(Some(7))
            ]));
            let old = old_partition_files(&store, &table, &columns, &schema, &touched).await?;
            assert_eq!(
                old,
                if offset == 0 {
                    expected.clone()
                } else {
                    vec![]
                }
            );
            assert_eq!(store.yielded.load(Ordering::SeqCst), old.len());
            let events = store
                .events
                .lock()
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, Event::Delimiter(_)))
                    .count(),
                if offset == 0 { 1 + 32 + 128 } else { 1 }
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, Event::List(_)))
                    .count(),
                old.len()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn discovery_falls_back_and_limits_corruption_checks_to_visited_branches() -> Result<()> {
        use crate::listing::internal::tests::{Event, RecordingStore};
        let store = RecordingStore::default();
        let table = Path::from("table");
        let columns = ["p".into(), "q".into()];
        let schema = Schema::new(vec![
            Field::new("p", DataType::Int32, true),
            Field::new("q", DataType::Int32, true),
        ]);
        let touched = HashSet::from([typed_partition_key(&["1".into(), "2".into()], &schema)?]);
        for path in [
            "table/p=1/q=2/old",
            "table/p=9/q=invalid/untouched",
            "table/not-a-partition/file",
        ] {
            store
                .put(&Path::from(path), bytes::Bytes::from_static(b"old").into())
                .await?;
        }
        assert_eq!(
            old_partition_files(&store, &table, &columns, &schema, &touched).await?,
            vec![Path::from("table/p=1/q=2/old")]
        );
        assert!(
            store
                .events
                .lock()
                .map_err(|e| exec_datafusion_err!("{e}"))?
                .contains(&Event::List(Path::from("table/not-a-partition")))
        );
        store
            .put(
                &Path::from("table/p=1/q=invalid/file"),
                bytes::Bytes::from_static(b"bad").into(),
            )
            .await?;
        assert!(
            old_partition_files(&store, &table, &columns, &schema, &touched)
                .await
                .is_err()
        );
        let denied = RecordingStore {
            inner: store.inner,
            denied_listing: Some(Path::from("table/p=1/q=2")),
            ..Default::default()
        };
        assert!(
            old_partition_files(&denied, &table, &columns, &schema, &touched)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_deletion_stream_uses_snapshot_after_all_copies() -> Result<()> {
        use std::sync::atomic::Ordering;

        use crate::listing::internal::tests::{Event, RecordingStore};
        for count in [0, 1, 1001] {
            let store = RecordingStore {
                allowed: Some(Path::from("table")),
                ..Default::default()
            };
            let table = Path::from("table");
            let staging = table
                .clone()
                .join(STAGING_DIRECTORY)
                .join(Uuid::new_v4().to_string());
            let mut old = vec![];
            for index in 0..count {
                let path = Path::from(format!("table/p=one/{index}"));
                store
                    .put(&path, bytes::Bytes::from_static(b"old").into())
                    .await?;
                old.push(path);
            }
            let keep = Path::from("table/p=two/keep");
            store
                .put(&keep, bytes::Bytes::from_static(b"keep").into())
                .await?;
            let files = vec![attempt_file(&staging, "a"), attempt_file(&staging, "b")];
            for file in &files {
                store
                    .put(file, bytes::Bytes::from_static(b"new").into())
                    .await?;
            }
            let columns = ["p".into()];
            commit_partitions(
                &store,
                &table,
                &staging,
                &columns,
                &string_schema(&columns),
                files.clone(),
            )
            .await?;
            assert_eq!(store.deletion_streams.load(Ordering::SeqCst), 1);
            let events = store
                .events
                .lock()
                .map_err(|e| exec_datafusion_err!("{e}"))?
                .clone();
            let deleted = events
                .iter()
                .filter_map(|event| {
                    if let Event::Delete(paths) = event {
                        Some(paths)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            old.sort_unstable();
            assert_eq!(deleted, vec![&old]);
            let mut copies = 0;
            let mut deleting = false;
            for event in &events {
                match event {
                    Event::Copy(_, destination) => {
                        assert!(!deleting);
                        assert!(store.head(destination).await.is_ok());
                        copies += 1;
                    }
                    Event::Delete(_) => {
                        assert_eq!(copies, 2);
                        deleting = true;
                    }
                    Event::List(_) | Event::Delimiter(_) => assert!(!deleting),
                }
            }
            assert_eq!(store.get(&keep).await?.bytes().await?, "keep");
            assert!(exists(&store, &staging.clone().join("complete")).await?);
            commit_partitions(
                &store,
                &table,
                &staging,
                &columns,
                &string_schema(&columns),
                files,
            )
            .await?;
            assert_eq!(
                *store
                    .events
                    .lock()
                    .map_err(|e| exec_datafusion_err!("{e}"))?,
                events
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn deletion_failures_are_drained_and_fence_replay_without_rollback() -> Result<()> {
        use crate::listing::internal::tests::{DeleteFailure, Event, RecordingStore};
        for failure in [DeleteFailure::PerObject(1), DeleteFailure::Transport] {
            let store = RecordingStore {
                delete_failure: Some(failure),
                ..Default::default()
            };
            let table = Path::from("table");
            let staging = table
                .clone()
                .join(STAGING_DIRECTORY)
                .join(Uuid::new_v4().to_string());
            for index in 0..3 {
                store
                    .put(
                        &Path::from(format!("table/p=one/{index}")),
                        bytes::Bytes::from_static(b"old").into(),
                    )
                    .await?;
            }
            let source = attempt_file(&staging, "a");
            store
                .put(&source, bytes::Bytes::from_static(b"new").into())
                .await?;
            let columns = ["p".into()];
            for _ in 0..2 {
                let error = commit_partitions(
                    &store,
                    &table,
                    &staging,
                    &columns,
                    &string_schema(&columns),
                    vec![source.clone()],
                )
                .await
                .err()
                .ok_or_else(|| exec_datafusion_err!("deletion must fail"))?;
                assert!(
                    matches!(error, DataFusionError::External(error) if error.downcast_ref::<CommitStateUnknownError>().is_some())
                );
            }
            assert!(exists(&store, &staging.clone().join("commit.json")).await?);
            assert!(!exists(&store, &staging.clone().join("complete")).await?);
            let events = store
                .events
                .lock()
                .map_err(|e| exec_datafusion_err!("{e}"))?
                .clone();
            let published = events
                .iter()
                .filter_map(|event| {
                    if let Event::Copy(_, to) = event {
                        Some(to)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(published.len(), 1);
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, Event::Delete(_)))
                    .count(),
                1
            );
            assert_eq!(store.get(published[0]).await?.bytes().await?, "new");
            assert_eq!(store.get(&source).await?.bytes().await?, "new");
            if matches!(failure, DeleteFailure::PerObject(_)) {
                assert!(!exists(&store, &Path::from("table/p=one/0")).await?);
                assert!(exists(&store, &Path::from("table/p=one/1")).await?);
                assert!(!exists(&store, &Path::from("table/p=one/2")).await?);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn static_overwrite_preserves_complete_and_unknown_replay_state() -> Result<()> {
        use datafusion::prelude::SessionContext;

        use crate::listing::delete::FileDeleteExec;
        use crate::listing::internal::tests::RecordingStore;
        let store = Arc::new(RecordingStore {
            allowed: Some(Path::from("table")),
            ..Default::default()
        });
        let table = Path::from("table");
        let staging = table
            .clone()
            .join(STAGING_DIRECTORY)
            .join(Uuid::new_v4().to_string());
        let unknown_stage = table
            .clone()
            .join(STAGING_DIRECTORY)
            .join(Uuid::new_v4().to_string());
        let source = attempt_file(&staging, "a");
        for path in [
            &source,
            &unknown_stage.clone().join("commit.json"),
            &attempt_file(&unknown_stage, "a"),
            &table.clone().join(".ordinary").join("file"),
        ] {
            store
                .put(path, bytes::Bytes::from_static(b"data").into())
                .await?;
        }
        let columns = ["p".into()];
        commit_partitions(
            store.as_ref(),
            &table,
            &staging,
            &columns,
            &string_schema(&columns),
            vec![source.clone()],
        )
        .await?;
        cleanup_staging(store.as_ref(), &staging.clone().join("attempts")).await?;
        assert!(!exists(store.as_ref(), &source).await?);
        assert!(exists(store.as_ref(), &attempt_file(&unknown_stage, "a")).await?);
        let reserved = table.clone().join(STAGING_DIRECTORY);
        let before = store
            .inner
            .list(Some(&reserved))
            .try_collect::<Vec<_>>()
            .await?;
        let ctx = SessionContext::new();
        let url = datafusion_datasource::ListingTableUrl::parse("memory://bucket/")?
            .get_url()
            .clone();
        ctx.runtime_env().register_object_store(&url, store.clone());
        let delete = Arc::new(FileDeleteExec::new(
            ObjectStoreUrl::parse(url.as_str())?,
            table.clone(),
        ));
        execute_stream(delete, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            store
                .inner
                .list(Some(&reserved))
                .try_collect::<Vec<_>>()
                .await?,
            before
        );
        assert!(
            !exists(
                store.as_ref(),
                &table.clone().join(".ordinary").join("file")
            )
            .await?
        );
        let replacement = table.clone().join("p=two").join("static");
        store
            .put(&replacement, bytes::Bytes::from_static(b"static").into())
            .await?;
        commit_partitions(
            store.as_ref(),
            &table,
            &staging,
            &columns,
            &string_schema(&columns),
            vec![source],
        )
        .await?;
        assert!(
            commit_partitions(
                store.as_ref(),
                &table,
                &unknown_stage,
                &columns,
                &string_schema(&columns),
                vec![]
            )
            .await
            .is_err()
        );
        assert_eq!(store.get(&replacement).await?.bytes().await?, "static");
        assert_eq!(
            store
                .inner
                .list(Some(&reserved))
                .try_collect::<Vec<_>>()
                .await?,
            before
        );
        Ok(())
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
        for path in [
            "table/p=/q=one/file",
            "table/p=__HIVE_DEFAULT_PARTITION__/q=one/file",
        ] {
            assert_eq!(
                partition_key(&Path::parse(path)?, &root, &columns),
                Some(vec!["__HIVE_DEFAULT_PARTITION__".into(), "one".into()])
            );
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
        use crate::listing::internal::tests::{Event, RecordingStore};
        let store = RecordingStore::default();
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
        assert!(
            !store
                .events
                .lock()
                .map_err(|e| exec_datafusion_err!("{e}"))?
                .iter()
                .any(|event| matches!(event, Event::List(_) | Event::Delimiter(_)))
        );
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
