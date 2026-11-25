use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, exec_err, plan_err};
use datafusion::execution::TaskContext;
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakeprocedure::LakeProcedureInvocation;

use super::arguments::{optional_bool, optional_i32, optional_string_map, required_string};
use super::snapshot::operation::PreparedSnapshotOperation;
use crate::io::StoreContext;
use crate::spec::manifest_list::{ManifestListWriteContext, ManifestListWriter};
use crate::spec::*;
use crate::table::files::{ManifestScope, SnapshotScope, collect_live_files, scan_manifests};
use crate::table::metadata_loader::table_metadata_location;

pub(crate) enum ImportOutput {
    AddFiles(SchemaRef),
    FileCount(SchemaRef),
}

impl ImportOutput {
    fn batch(self, count: i64, partitions: Option<i64>) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![count]))];
        let schema = match self {
            Self::FileCount(schema) => schema,
            Self::AddFiles(schema) => {
                columns.push(Arc::new(Int64Array::from(vec![partitions])));
                schema
            }
        };
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

pub(crate) struct AddFiles {
    source: Vec<String>,
    filter: BTreeMap<String, String>,
    check_duplicates: bool,
    parallelism: usize,
    ignore_missing_files: bool,
}

impl AddFiles {
    pub(super) fn for_migration(
        source: Vec<String>,
        parallelism: usize,
        ignore_missing_files: bool,
    ) -> Self {
        Self {
            source,
            filter: BTreeMap::new(),
            check_duplicates: true,
            parallelism,
            ignore_missing_files,
        }
    }

    pub(super) fn from_invocation(invocation: &LakeProcedureInvocation) -> Result<Self> {
        let parallelism = optional_i32(invocation, "parallelism")?.unwrap_or(1);
        if parallelism <= 0 {
            return plan_err!("Parallelism should be larger than 0");
        }
        let name = sail_sql_analyzer::parser::parse_object_name(&required_string(
            invocation,
            "source_table",
        )?)
        .and_then(sail_sql_analyzer::expression::from_ast_object_name)
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;

        Ok(Self {
            source: name.into(),
            filter: optional_string_map(invocation, "partition_filter")?.unwrap_or_default(),
            check_duplicates: optional_bool(invocation, "check_duplicate_files")?.unwrap_or(true),
            parallelism: parallelism as usize,
            ignore_missing_files: false,
        })
    }

    pub(crate) async fn prepare(
        &self,
        ctx: &TaskContext,
        store: &StoreContext,
        table_url: &url::Url,
        metadata: &TableMetadata,
        output_schema: ImportOutput,
    ) -> Result<PreparedSnapshotOperation> {
        let parts = &self.source;
        let filter = self
            .filter
            .iter()
            .map(|(key, value)| (key.to_lowercase(), value))
            .collect::<HashMap<_, _>>();
        let (format, location, declared_parts, managed_source) = match parts.as_slice() {
            [format, location]
                if ["parquet", "avro", "orc"].contains(&format.to_lowercase().as_str()) =>
            {
                (format.to_lowercase(), location.clone(), None, false)
            }
            _ => {
                let manager = ctx.extension::<CatalogManager>()?;
                let source = manager
                    .get_table(parts)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                match source.kind {
                    TableKind::Table {
                        format,
                        location: Some(location),
                        partition_by,
                        is_external,
                        ..
                    } => (
                        format.to_lowercase(),
                        location,
                        Some(
                            partition_by
                                .into_iter()
                                .map(|field| field.column)
                                .collect::<Vec<_>>(),
                        ),
                        !is_external,
                    ),
                    _ => return plan_err!("add_files source must be a stored file table"),
                }
            }
        };
        if !["parquet", "avro"].contains(&format.as_str()) {
            return plan_err!("Unsupported add_files source format: {format}");
        }
        let catalog_source = declared_parts.is_some();
        let declared_spec = declared_parts
            .as_deref()
            .map(|names| self.source_spec(metadata, names))
            .transpose()?;
        let catalog_partitions = if let Some(names) = &declared_parts {
            let values = names
                .iter()
                .map(|name| {
                    filter
                        .get(&name.to_lowercase())
                        .map(|value| (*value).clone())
                })
                .collect::<Vec<_>>();
            ctx.extension::<CatalogManager>()?
                .list_file_partitions(parts, &values)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            None
        };
        let sources = match catalog_partitions {
            Some(partitions) => {
                let names = declared_parts.as_deref().unwrap_or_default();
                let mut sources = Vec::new();
                for partition in partitions {
                    if partition.values.len() != names.len() {
                        return plan_err!(
                            "Catalog partition value count does not match its columns"
                        );
                    }
                    let values = names
                        .iter()
                        .cloned()
                        .zip(partition.values)
                        .collect::<Vec<_>>();
                    if values.iter().any(|(key, value)| {
                        filter
                            .get(&key.to_lowercase())
                            .is_some_and(|wanted| *wanted != value)
                    }) {
                        continue;
                    }
                    let location = partition.location.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Catalog partition is missing its storage location".into(),
                        )
                    })?;
                    sources.push((
                        location,
                        partition.format,
                        (!names.is_empty()).then_some(values),
                    ));
                }
                sources
            }
            None => vec![(location, format, None)],
        };
        // A path source has no declared columns. Preserve spec/filter validation when no
        // historical spec accepts the supplied keys.
        let prune_directories = !filter.is_empty()
            && metadata.partition_specs.iter().any(|spec| {
                self.filter
                    .keys()
                    .all(|key| spec.fields().iter().any(|field| &field.name == key))
            });
        let mut pruned_directory = false;
        let mut candidates = vec![];
        let mut inferred_names = declared_parts;
        for (location, format, registered_partition) in sources {
            if !["parquet", "avro"].contains(&format.as_str()) {
                return plan_err!("Unsupported add_files partition format: {format}");
            }
            let source_url = if catalog_source {
                match sail_data_source::parse_absolute_directory_url(&location) {
                    Some(url) => url,
                    None => sail_data_source::resolve_table_url(location)?,
                }
            } else {
                crate::lake_source::IcebergLakeSource::parse_table_url(vec![location]).await?
            };
            validate_source_store(&source_url, table_url)?;
            let source_store = ctx
                .runtime_env()
                .object_store_registry
                .get_store(&source_url)?;
            let prefix = object_store::path::Path::from_url_path(source_url.path())?;
            let single_file = if catalog_source {
                None
            } else {
                match source_store.head(&prefix).await {
                    Ok(file) => Some(file),
                    Err(object_store::Error::NotFound { .. }) => None,
                    Err(error) => return Err(error.into()),
                }
            };
            let objects = if let Some(file) = single_file {
                vec![file]
            } else {
                let mut directories = vec![prefix.clone()];
                let mut files = Vec::new();
                while let Some(directory) = directories.pop() {
                    let listing = source_store.list_with_delimiter(Some(&directory)).await?;
                    // S3 can list a zero-byte directory marker as the prefix itself.
                    files.extend(
                        listing
                            .objects
                            .into_iter()
                            .filter(|object| object.location != directory),
                    );
                    if registered_partition.is_none() {
                        for child in listing.common_prefixes {
                            let Some(segment) =
                                child.filename().filter(|s| !s.starts_with(['_', '.']))
                            else {
                                continue;
                            };
                            let Some((name, value)) = segment.split_once('=') else {
                                continue;
                            };
                            if prune_directories
                                && let Some(expected) =
                                    filter.get(&decode_partition(name)?.to_lowercase())
                                && **expected != decode_partition(value)?
                            {
                                pruned_directory = true;
                                continue;
                            }
                            directories.push(child);
                        }
                    }
                }
                files
            };
            if objects.is_empty() && source_url.scheme() == "file" {
                let path = source_url.to_file_path().map_err(|_| {
                    DataFusionError::Plan(format!("Invalid local source: {source_url}"))
                })?;
                // A managed file table may have no warehouse directory until its first write.
                let allow_missing =
                    self.ignore_missing_files || (managed_source && registered_partition.is_none());
                if let Err(error) = tokio::fs::metadata(path).await
                    && !(allow_missing && error.kind() == std::io::ErrorKind::NotFound)
                {
                    return Err(error.into());
                }
            }
            for object in objects {
                let Some(relative) = object
                    .location
                    .as_ref()
                    .strip_prefix(prefix.as_ref())
                    .map(|p| p.trim_start_matches('/'))
                else {
                    continue;
                };
                let segments = relative.split('/').collect::<Vec<_>>();
                if segments
                    .iter()
                    .any(|p| p.starts_with('.') || p.starts_with('_'))
                {
                    continue;
                }
                let directories = &segments[..segments.len() - 1];
                let partition = match &registered_partition {
                    Some(partition) => partition.clone(),
                    None => {
                        let mut partition = vec![];
                        for segment in directories {
                            if let Some((name, value)) = segment.split_once('=') {
                                partition.push((decode_partition(name)?, decode_partition(value)?));
                            }
                        }
                        partition
                    }
                };
                let names = partition
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>();
                if let Some(expected) = &inferred_names {
                    if expected
                        .iter()
                        .map(|n| n.to_lowercase())
                        .collect::<Vec<_>>()
                        != names.iter().map(|n| n.to_lowercase()).collect::<Vec<_>>()
                    {
                        return plan_err!("Conflicting partition columns in add_files source");
                    }
                } else {
                    inferred_names = Some(names);
                }
                candidates.push((
                    object,
                    partition,
                    source_store.clone(),
                    source_url.clone(),
                    format.clone(),
                ));
            }
        }
        if inferred_names.is_none() && pruned_directory {
            return plan_err!("Cannot find any matching partitions in table");
        }
        let spec = match declared_spec {
            Some(spec) => spec,
            None => self.source_spec(metadata, &inferred_names.unwrap_or_default())?,
        };
        let schema = metadata
            .current_schema()
            .ok_or_else(|| DataFusionError::Plan("Missing current Iceberg schema".into()))?;
        if !catalog_source && !spec.is_unpartitioned() && candidates.is_empty() {
            return plan_err!("Cannot find any matching partitions in table");
        }
        let files = futures::stream::iter(
            candidates
                .into_iter()
                .map(|(object, partition, source_store, source_url, format)| {
                    async move {
                        let partition = partition
                            .iter()
                            .zip(spec.fields())
                            .map(|((_, value), field)| {
                                let field =
                                    schema.field_by_id(field.source_id).ok_or_else(|| {
                                        DataFusionError::Plan(
                                            "Missing partition source field".into(),
                                        )
                                    })?;
                                if value == "__HIVE_DEFAULT_PARTITION__" {
                                    return Ok(None);
                                }
                                if *field.field_type == Type::Primitive(PrimitiveType::String) {
                                    return Ok(Some(Literal::Primitive(PrimitiveLiteral::String(
                                        value.clone(),
                                    ))));
                                }
                                Literal::try_from_str(value, &field.field_type)
                                    .map_err(DataFusionError::Plan)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let (file_format, record_count) =
                            match import_record_count(source_store.clone(), &object, &format).await
                            {
                                Ok(metrics) => metrics,
                                Err(_)
                                    if self.ignore_missing_files
                                        && matches!(
                                            source_store.head(&object.location).await,
                                            Err(object_store::Error::NotFound { .. })
                                        ) =>
                                {
                                    return Ok(None);
                                }
                                Err(error) => return Err(error),
                            };
                        let file_path = format!(
                            "{}/{}",
                            &source_url[..url::Position::BeforePath],
                            object.location
                        );
                        Ok::<_, DataFusionError>(Some(DataFile {
                            content: DataContentType::Data,
                            file_path,
                            file_format,
                            partition,
                            record_count,
                            file_size_in_bytes: object.size,
                            column_sizes: HashMap::new(),
                            value_counts: HashMap::new(),
                            null_value_counts: HashMap::new(),
                            nan_value_counts: HashMap::new(),
                            lower_bounds: HashMap::new(),
                            upper_bounds: HashMap::new(),
                            block_size_in_bytes: None,
                            key_metadata: None,
                            split_offsets: vec![],
                            equality_ids: vec![],
                            sort_order_id: None,
                            first_row_id: None,
                            partition_spec_id: spec.spec_id(),
                            referenced_data_file: None,
                            content_offset: None,
                            content_size_in_bytes: None,
                        }))
                    }
                    .boxed()
                })
                .collect::<Vec<_>>(),
        )
        .buffer_unordered(self.parallelism)
        .try_filter_map(futures::future::ok)
        .try_collect::<Vec<_>>()
        .await?;
        if self.check_duplicates {
            let mut seen_paths = collect_live_files(store, metadata, self.parallelism, |file| {
                store.resolve_to_absolute_path(&file.file_path).map(Some)
            })
            .await?
            .into_iter()
            .collect::<HashSet<_>>();
            for file in &files {
                if !seen_paths.insert(store.resolve_to_absolute_path(&file.file_path)?) {
                    return exec_err!(
                        "Cannot add file already referenced by the table or repeated in this import: {}",
                        file.file_path
                    );
                }
            }
        }
        let report_partitions = files.is_empty()
            || (catalog_source && spec.is_unpartitioned())
            || (metadata.format_version == FormatVersion::V1
                && !metadata
                    .properties
                    .get("compatibility.snapshot-id-inheritance.enabled")
                    .is_some_and(|v| v.eq_ignore_ascii_case("true")));
        prepare_append(
            store,
            table_url,
            metadata,
            spec,
            files,
            report_partitions,
            output_schema,
        )
        .await
    }
    fn source_spec<'a>(
        &self,
        metadata: &'a TableMetadata,
        names: &[String],
    ) -> Result<&'a PartitionSpec> {
        let spec = metadata.partition_specs.iter().find(|spec| {
            spec.fields().iter().all(|f| f.transform == Transform::Identity)
                && spec.fields().iter().map(|f| f.name.to_lowercase()).eq(names.iter().map(|n| n.to_lowercase()))
        }).ok_or_else(|| DataFusionError::Plan(format!("Cannot find a partition spec that matches the partition columns {names:?} in input table")))?;
        if self
            .filter
            .keys()
            .any(|key| !spec.fields().iter().any(|f| &f.name == key))
        {
            return plan_err!("Partition filter refers to columns that are not partitioned");
        }
        if spec.is_unpartitioned() && !self.filter.is_empty() {
            return plan_err!("Cannot use partition filter with an unpartitioned table");
        }
        Ok(spec)
    }
}

fn validate_source_store(source_url: &url::Url, table_url: &url::Url) -> Result<()> {
    if source_url.scheme() != table_url.scheme() || source_url.authority() != table_url.authority()
    {
        return plan_err!("add_files requires source and target to use the same object store");
    }
    Ok(())
}

async fn import_record_count(
    store: Arc<dyn object_store::ObjectStore>,
    object: &object_store::ObjectMeta,
    format: &str,
) -> Result<(DataFileFormat, u64)> {
    let mut reader = object_store::buffered::BufReader::new(store, object);
    match format {
        "parquet" => {
            use parquet::arrow::async_reader::AsyncFileReader;
            let metadata = reader.get_metadata(None).await?;
            let rows = u64::try_from(metadata.file_metadata().num_rows())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok((DataFileFormat::Parquet, rows))
        }
        "avro" => tokio::task::spawn_blocking(move || {
            let reader = tokio_util::io::SyncIoBridge::new(reader);
            let rows = datafusion::arrow_avro::reader::ReaderBuilder::new()
                .build(reader)?
                .try_fold(0u64, |rows, batch| {
                    rows.checked_add(batch?.num_rows() as u64).ok_or_else(|| {
                        DataFusionError::Execution("Avro record count overflow".into())
                    })
                })?;
            Ok((DataFileFormat::Avro, rows))
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?,
        _ => plan_err!("Unsupported add_files source format: {format}"),
    }
}

fn decode_partition(value: &str) -> Result<String> {
    let mut bytes = vec![];
    let mut input = value.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        if byte == b'%' {
            let hi = input.next().and_then(|b| (b as char).to_digit(16));
            let lo = input.next().and_then(|b| (b as char).to_digit(16));
            match (hi, lo) {
                (Some(hi), Some(lo)) => bytes.push((hi * 16 + lo) as u8),
                _ => return plan_err!("Invalid partition path escape"),
            }
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).map_err(|e| DataFusionError::Plan(e.to_string()))
}

async fn prepare_append(
    store: &StoreContext,
    table_url: &url::Url,
    metadata: &TableMetadata,
    spec: &PartitionSpec,
    files: Vec<DataFile>,
    report_partitions: bool,
    output_schema: ImportOutput,
) -> Result<PreparedSnapshotOperation> {
    let schema = metadata
        .current_schema()
        .ok_or_else(|| DataFusionError::Plan("Missing schema".into()))?;
    let snapshot_id = (uuid::Uuid::new_v4().as_u128() as i64) & i64::MAX;
    let sequence_number = if metadata.format_version == FormatVersion::V1 {
        0
    } else {
        metadata
            .last_sequence_number
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("Sequence number overflow".into()))?
    };
    let first_row_id =
        (metadata.format_version == FormatVersion::V3).then_some(metadata.next_row_id.unwrap_or(0));
    let count = files.len() as i64;
    let sum = |get: fn(&DataFile) -> u64| {
        files.iter().try_fold(0u64, |total, file| {
            total
                .checked_add(get(file))
                .ok_or_else(|| DataFusionError::Execution("Imported file totals overflow".into()))
        })
    };
    let rows = sum(|file| file.record_count)?;
    let size = sum(|file| file.file_size_in_bytes)?;
    let partitions = if report_partitions {
        Some(
            files
                .iter()
                .map(|f| serde_json::to_string(&f.partition))
                .collect::<std::result::Result<HashSet<_>, _>>()
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .len() as i64,
        )
    } else {
        None
    };
    let mut manifests = vec![];
    let mut list = ManifestListWriter::new();
    let mut parents = scan_manifests(
        store,
        metadata,
        &ManifestScope {
            snapshots: SnapshotScope::Current,
            content: None,
        },
    )
    .await?;
    let imported_first_row_id = first_row_id
        .map(|start| crate::operations::snapshot::assign_parent_row_ids(&mut parents, start))
        .transpose()
        .map_err(DataFusionError::Execution)?;
    for manifest in parents {
        list.append(manifest);
    }
    if !files.is_empty() {
        let header = ManifestMetadata::new(
            Arc::new(schema.clone()),
            metadata.current_schema_id,
            spec.clone(),
            metadata.format_version,
            ManifestContentType::Data,
        );
        let mut writer = ManifestWriter::new(Some(snapshot_id), None, header);
        for file in files {
            writer.add(file);
        }
        let bytes = writer
            .to_avro_bytes_v2()
            .map_err(DataFusionError::Execution)?;
        let path = format!("metadata/add-files-{}.avro", uuid::Uuid::new_v4());
        let mut descriptor = writer
            .into_manifest_file(
                table_metadata_location(table_url, &path)?,
                sequence_number,
                snapshot_id,
            )
            .map_err(DataFusionError::Execution)?;
        descriptor.manifest_length = bytes.len() as i64;
        descriptor.first_row_id = imported_first_row_id;
        list.append(descriptor);
        manifests.push((path, bytes));
    }
    let parent = metadata.current_snapshot();
    let list_path = format!("metadata/snap-{snapshot_id}-{}.avro", uuid::Uuid::new_v4());
    let bytes = list
        .to_bytes_with_context(ManifestListWriteContext {
            version: metadata.format_version,
            snapshot_id,
            parent_snapshot_id: parent.map(|s| s.snapshot_id()),
            sequence_number,
            first_row_id,
        })
        .map_err(DataFusionError::Execution)?;
    manifests.push((list_path.clone(), bytes));
    let mut summary = Summary::new(Operation::Append)
        .with_property("added-data-files", count.to_string())
        .with_property("added-records", rows.to_string())
        .with_property("added-files-size", size.to_string());
    if let Some(partitions) = partitions {
        summary = summary.with_property("changed-partition-count", partitions.to_string());
    }
    for (key, added) in [
        ("total-data-files", count as u64),
        ("total-records", rows),
        ("total-files-size", size),
        ("total-delete-files", 0),
        ("total-position-deletes", 0),
        ("total-equality-deletes", 0),
    ] {
        if parent.is_some_and(|s| !s.summary.additional_properties.contains_key(key)) {
            continue;
        }
        let previous = parent
            .and_then(|s| s.summary.additional_properties.get(key))
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?
            .unwrap_or(0);
        summary = summary.with_property(
            key,
            previous
                .checked_add(added)
                .ok_or_else(|| DataFusionError::Execution("Snapshot totals overflow".into()))?
                .to_string(),
        );
    }
    let mut snapshot = SnapshotBuilder::new()
        .with_snapshot_id(snapshot_id)
        .with_sequence_number(sequence_number)
        .with_schema_id(metadata.current_schema_id)
        .with_manifest_list(table_metadata_location(table_url, &list_path)?)
        .with_summary(summary);
    if let Some(parent) = parent {
        snapshot = snapshot.with_parent_snapshot_id(parent.snapshot_id());
    }
    if let (Some(start), Some(import_start)) = (first_row_id, imported_first_row_id) {
        let next = i64::try_from(rows)
            .ok()
            .and_then(|count| import_start.checked_add(count))
            .ok_or_else(|| DataFusionError::Execution("Imported row IDs exceed int64".into()))?;
        snapshot = snapshot
            .with_first_row_id(start)
            .with_added_rows(next - start);
    }
    let retention = metadata
        .refs
        .get(MAIN_BRANCH)
        .map(|r| r.retention.clone())
        .unwrap_or(SnapshotRetention::Branch {
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        });
    let mut updates = vec![
        TableUpdate::AddSnapshot {
            snapshot: snapshot.build().map_err(DataFusionError::Execution)?,
        },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.into(),
            reference: SnapshotReference {
                snapshot_id,
                retention,
            },
        },
    ];
    if !metadata
        .properties
        .contains_key(DEFAULT_SCHEMA_NAME_MAPPING)
    {
        let mapping = NameMapping::new(
            schema
                .as_struct()
                .fields()
                .iter()
                .map(|f| mapped_field(f))
                .collect(),
        );
        updates.push(TableUpdate::SetProperties {
            updates: HashMap::from([(
                DEFAULT_SCHEMA_NAME_MAPPING.into(),
                serde_json::to_string(&mapping)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            )]),
        });
    }
    Ok(PreparedSnapshotOperation {
        changed: true,
        requirements: vec![
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.into(),
                snapshot_id: parent.map(|s| s.snapshot_id()),
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id,
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_spec_id,
            },
        ],
        updates,
        manifests,
        output: output_schema.batch(count, partitions)?,
    })
}

fn mapped_field(field: &NestedField) -> MappedField {
    let fields = match field.field_type.as_ref() {
        Type::Struct(s) => s.fields().iter().map(|f| mapped_field(f)).collect(),
        Type::List(l) => vec![mapped_field(&l.element_field)],
        Type::Map(m) => vec![mapped_field(&m.key_field), mapped_field(&m.value_field)],
        Type::Primitive(_) => vec![],
    };
    MappedField::new(Some(field.id), vec![field.name.clone()], fields)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
    use object_store::ObjectStoreExt;
    use object_store::buffered::DEFAULT_BUFFER_SIZE;
    use object_store::memory::InMemory;

    use super::{import_record_count, validate_source_store};
    use crate::io::test_store::TestStore;

    #[test]
    fn source_store_includes_azure_container() -> Result<(), Box<dyn std::error::Error>> {
        let target = url::Url::parse("abfss://target@account.dfs.core.windows.net/table")?;
        let same = url::Url::parse("abfss://target@account.dfs.core.windows.net/source")?;
        let other = url::Url::parse("abfss://other@account.dfs.core.windows.net/source")?;
        validate_source_store(&same, &target)?;
        assert!(validate_source_store(&other, &target).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn import_counts_with_bounded_object_reads() -> Result<(), Box<dyn std::error::Error>> {
        let values: Vec<_> = (0..256)
            .map(|i| format!("{i:04}{}", "x".repeat(16 * 1024)))
            .collect();
        let column: ArrayRef = Arc::new(StringArray::from(values.clone()));
        let batch = RecordBatch::try_from_iter([("value", column)])?;
        let mut parquet = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut parquet, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"row","fields":[{"name":"value","type":"string"}]}"#,
        )?;
        let mut writer = apache_avro::Writer::new(&schema, Vec::new())?;
        for value in values {
            writer.append_value(apache_avro::types::Value::Record(vec![(
                "value".into(),
                value.into(),
            )]))?;
        }
        let avro = writer.into_inner()?;
        for (format, bytes) in [("parquet", parquet), ("avro", avro)] {
            let size = bytes.len();
            assert!(size > DEFAULT_BUFFER_SIZE);
            let inner = Arc::new(InMemory::new());
            let path = format.into();
            inner.put(&path, bytes.into()).await?;
            let meta = inner.head(&path).await?;
            let store = Arc::new(TestStore::new(inner));
            let (_, rows) = import_record_count(store.clone(), &meta, format).await?;
            assert_eq!(rows, batch.num_rows() as u64);
            if format == "avro" {
                assert!(store.largest_read.load(Ordering::Relaxed) <= DEFAULT_BUFFER_SIZE);
            } else {
                assert!(store.bytes_read.load(Ordering::Relaxed) < size);
            }
        }
        Ok(())
    }
}
