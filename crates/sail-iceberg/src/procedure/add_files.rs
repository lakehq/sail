// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
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

pub(crate) struct AddFiles {
    source: String,
    filter: BTreeMap<String, String>,
    check_duplicates: bool,
    parallelism: usize,
}

impl AddFiles {
    pub(super) fn from_invocation(invocation: &LakeProcedureInvocation) -> Result<Self> {
        let parallelism = optional_i32(invocation, "parallelism")?.unwrap_or(1);
        if parallelism <= 0 {
            return plan_err!("Parallelism should be larger than 0");
        }
        Ok(Self {
            source: required_string(invocation, "source_table")?,
            filter: optional_string_map(invocation, "partition_filter")?.unwrap_or_default(),
            check_duplicates: optional_bool(invocation, "check_duplicate_files")?.unwrap_or(true),
            parallelism: parallelism as usize,
        })
    }

    pub(crate) async fn prepare(
        &self,
        ctx: &TaskContext,
        store: &StoreContext,
        table_url: &url::Url,
        metadata: &TableMetadata,
        output_schema: SchemaRef,
    ) -> Result<PreparedSnapshotOperation> {
        let name = sail_sql_analyzer::parser::parse_object_name(&self.source)
            .and_then(sail_sql_analyzer::expression::from_ast_object_name)
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
        let parts: Vec<String> = name.into();
        let mut catalog_partitions = None;
        let (format, location, declared_parts) = match parts.as_slice() {
            [format, location]
                if ["parquet", "avro", "orc"].contains(&format.to_lowercase().as_str()) =>
            {
                (format.to_lowercase(), location.clone(), None)
            }
            _ => {
                let manager = ctx.extension::<CatalogManager>()?;
                let source = manager
                    .get_table(&parts)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let partition_names = source
                    .kind
                    .partition_columns()
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>();
                catalog_partitions = manager
                    .list_file_partitions(&parts)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                match source.kind {
                    TableKind::Table {
                        format,
                        location: Some(location),
                        ..
                    } => (format.to_lowercase(), location, Some(partition_names)),
                    _ => return plan_err!("add_files source must be a stored file table"),
                }
            }
        };
        if !["parquet", "avro", "orc"].contains(&format.as_str()) {
            return plan_err!("Unsupported add_files source format: {format}");
        }
        let catalog_source = declared_parts.is_some();
        let sources = match catalog_partitions {
            Some(partitions) => {
                let names = declared_parts.as_deref().unwrap_or_default();
                partitions
                    .into_iter()
                    .map(|partition| {
                        if partition.values.len() != names.len() {
                            return plan_err!(
                                "Catalog partition value count does not match its columns"
                            );
                        }
                        Ok((
                            partition.location,
                            partition.format,
                            Some(
                                names
                                    .iter()
                                    .cloned()
                                    .zip(partition.values)
                                    .collect::<Vec<_>>(),
                            ),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
            }
            None => vec![(location, format, None)],
        };
        let mut candidates = vec![];
        let mut inferred_names = declared_parts;
        for (location, format, registered_partition) in sources {
            if !["parquet", "avro", "orc"].contains(&format.as_str()) {
                return plan_err!("Unsupported add_files partition format: {format}");
            }
            let source_url =
                crate::lake_source::IcebergLakeSource::parse_table_url(vec![location]).await?;
            if (
                source_url.scheme(),
                source_url.host_str(),
                source_url.port(),
            ) != (table_url.scheme(), table_url.host_str(), table_url.port())
            {
                return plan_err!(
                    "add_files requires source and target to use the same object store"
                );
            }
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
                        directories.extend(listing.common_prefixes.into_iter().filter(|child| {
                            child.filename().is_some_and(|name| {
                                name.contains('=') && !name.starts_with(['_', '.'])
                            })
                        }));
                    }
                }
                files
            };
            if objects.is_empty() && source_url.scheme() == "file" {
                let path = source_url.to_file_path().map_err(|_| {
                    DataFusionError::Plan(format!("Invalid local source: {source_url}"))
                })?;
                tokio::fs::metadata(path).await?;
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
        let names = inferred_names.unwrap_or_default();
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
        let schema = metadata
            .current_schema()
            .ok_or_else(|| DataFusionError::Plan("Missing current Iceberg schema".into()))?;
        let candidates = candidates
            .into_iter()
            .filter(|(_, partition, _, _, _)| {
                self.filter.iter().all(|(key, value)| {
                    spec.fields()
                        .iter()
                        .position(|field| &field.name == key)
                        .and_then(|index| partition.get(index))
                        .is_some_and(|(_, part)| part == value)
                })
            })
            .collect::<Vec<_>>();
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
                        let bytes = source_store.get(&object.location).await?.bytes().await?;
                        let (file_format, record_count) = match format.as_str() {
                            "parquet" => {
                                let reader =
                                    parquet::file::reader::SerializedFileReader::new(bytes)
                                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                                use parquet::file::reader::FileReader;
                                (
                                    DataFileFormat::Parquet,
                                    u64::try_from(reader.metadata().file_metadata().num_rows())
                                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                                )
                            }
                            "avro" => {
                                let reader = datafusion::arrow_avro::reader::ReaderBuilder::new()
                                    .build(std::io::Cursor::new(bytes))?;
                                let mut count = 0u64;
                                for batch in reader {
                                    let batch = batch?;
                                    count = count.checked_add(batch.num_rows() as u64).ok_or_else(
                                        || {
                                            DataFusionError::Execution(
                                                "Avro record count overflow".into(),
                                            )
                                        },
                                    )?;
                                }
                                (DataFileFormat::Avro, count)
                            }
                            _ => {
                                let builder = orc_rust::ArrowReaderBuilder::try_new(bytes)
                                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                                (
                                    DataFileFormat::Orc,
                                    builder.file_metadata().number_of_rows(),
                                )
                            }
                        };
                        let file_path = format!(
                            "{}/{}",
                            &source_url[..url::Position::BeforePath],
                            object.location
                        );
                        Ok::<_, DataFusionError>(DataFile {
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
                        })
                    }
                    .boxed()
                })
                .collect::<Vec<_>>(),
        )
        .buffer_unordered(self.parallelism)
        .try_collect::<Vec<_>>()
        .await?;
        if self.check_duplicates {
            let existing = collect_live_files(store, metadata, self.parallelism, |file| {
                store.resolve_to_absolute_path(&file.file_path).map(Some)
            })
            .await?
            .into_iter()
            .collect::<HashSet<_>>();
            for file in &files {
                if !existing.contains(&store.resolve_to_absolute_path(&file.file_path)?) {
                    continue;
                }
                return exec_err!(
                    "Cannot add files that are already referenced by table: {}",
                    file.file_path
                );
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
    output_schema: SchemaRef,
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
        descriptor.first_row_id = first_row_id;
        list.append(descriptor);
        manifests.push((path, bytes));
    }
    for manifest in scan_manifests(
        store,
        metadata,
        &ManifestScope {
            snapshots: SnapshotScope::Current,
            content: None,
        },
    )
    .await?
    {
        list.append(manifest);
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
    if let Some(id) = first_row_id {
        snapshot =
            snapshot
                .with_first_row_id(id)
                .with_added_rows(i64::try_from(rows).map_err(|_| {
                    DataFusionError::Execution("Imported row count exceeds int64".into())
                })?);
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
        output: RecordBatch::try_new(
            output_schema,
            vec![
                Arc::new(Int64Array::from(vec![count])),
                Arc::new(Int64Array::from(vec![partitions])),
            ],
        )?,
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
