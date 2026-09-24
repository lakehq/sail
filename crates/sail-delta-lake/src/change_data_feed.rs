use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::common::{JoinType, NullEquality, Result, ScalarValue, plan_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::TableSource;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use object_store::ObjectStoreExt;
use sail_common_datafusion::schema_evolution::FIELD_ALIASES_METADATA_KEY;

use crate::datasource::scan::{
    FileScanParams, TableStatsMode, build_file_scan_config, file_scan_projection_for_schema,
};
use crate::datasource::{
    COMMIT_TIMESTAMP_COLUMN, COMMIT_VERSION_COLUMN, DeltaScanConfig, df_logical_schema,
};
use crate::delta_log::{
    LogStoreRef, get_actions, resolve_commit_timestamp_from_actions,
    resolve_effective_protocol_and_metadata,
};
use crate::options::r#gen::DeltaReadOptions;
use crate::physical::scan_planner::{align_delta_scan_output, build_eager_adds_input};
use crate::physical_plan::DeltaScanByAddsExec;
use crate::schema::get_physical_schema;
use crate::snapshot::catalog_managed_commit_path;
use crate::spec::{Action, Add, Metadata, Remove, StructType, commit_path};
use crate::table::{DeltaSnapshot, DeltaTable, parse_timestamp_as_of};

pub(crate) const CHANGE_TYPE_COLUMN: &str = "_change_type";

pub(crate) fn enabled(metadata: &Metadata) -> bool {
    metadata
        .configuration()
        .get("delta.enableChangeDataFeed")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

pub(crate) fn validate_schema(schema: &Schema) -> Result<()> {
    for field in schema.fields() {
        if [
            CHANGE_TYPE_COLUMN,
            COMMIT_VERSION_COLUMN,
            COMMIT_TIMESTAMP_COLUMN,
        ]
        .iter()
        .any(|name| field.name().eq_ignore_ascii_case(name))
        {
            return plan_err!("Change data feed reserves column {}", field.name());
        }
    }
    Ok(())
}

fn feed_schema(schema: &Schema) -> SchemaRef {
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new(CHANGE_TYPE_COLUMN, DataType::Utf8, true),
        Field::new(COMMIT_VERSION_COLUMN, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]);
    Arc::new(Schema::new(fields))
}

#[derive(Debug)]
struct ChangeFiles {
    snapshot: Arc<DeltaSnapshot>,
    files: Vec<Add>,
    exclude: Option<Add>,
    change_type: Option<&'static str>,
    timestamp: i64,
}

pub(crate) struct ChangeDataFeedSource {
    snapshot: Arc<DeltaSnapshot>,
    log_store: LogStoreRef,
    schema: SchemaRef,
    changes: Vec<ChangeFiles>,
}

impl std::fmt::Debug for ChangeDataFeedSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeDataFeedSource")
            .field("snapshot_version", &self.snapshot.version())
            .field("file_groups", &self.changes.len())
            .finish()
    }
}

impl TableSource for ChangeDataFeedSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ChangeDataFeedSource {
    pub(crate) async fn try_new(
        snapshot: Arc<DeltaSnapshot>,
        log_store: LogStoreRef,
        options: &DeltaReadOptions,
    ) -> Result<Self> {
        if options.version_as_of.is_some() || options.timestamp_as_of.is_some() {
            return plan_err!("Change data feed cannot be combined with time travel options");
        }
        if options.starting_version.is_some() && options.starting_timestamp.is_some() {
            return plan_err!("Specify only one of startingVersion and startingTimestamp");
        }
        if options.ending_version.is_some() && options.ending_timestamp.is_some() {
            return plan_err!("Specify only one of endingVersion and endingTimestamp");
        }
        validate_schema(snapshot.schema())?;
        let latest = snapshot.version();
        let mut table = DeltaTable::new(log_store.clone(), snapshot.load_config().clone());
        table.load_version(latest).await?;
        let start = match (options.starting_version, &options.starting_timestamp) {
            (Some(version), _) => version,
            (_, Some(timestamp)) => timestamp_bound(&mut table, latest, timestamp, true).await?,
            _ => {
                return plan_err!("Change data feed requires startingVersion or startingTimestamp");
            }
        };
        let end = match (options.ending_version, &options.ending_timestamp) {
            (Some(version), _) => version.min(latest),
            (_, Some(timestamp)) => timestamp_bound(&mut table, latest, timestamp, false).await?,
            _ => latest,
        };
        if start < 0 || end < 0 || start > end || start > latest {
            return plan_err!(
                "Invalid change data feed version range [{start}, {end}]; latest version is {latest}"
            );
        }
        let snapshot =
            if snapshot.effective_column_mapping_mode() != crate::spec::ColumnMappingMode::None {
                table.load_version(end).await?;
                table.snapshot()?.clone()
            } else {
                snapshot
            };
        table.load_version(start).await?;
        let mut current = table.snapshot()?.clone();
        let mut changes = Vec::new();
        let store = log_store.object_store(None);
        for version in start..=end {
            let path = snapshot
                .load_config()
                .catalog_managed_commits
                .as_ref()
                .and_then(|commits| {
                    commits
                        .commits
                        .iter()
                        .find(|commit| commit.version == version)
                })
                .map(|commit| catalog_managed_commit_path(&commit.file_name))
                .unwrap_or_else(|| commit_path(version));
            let result = store.get(&path).await.map_err(|error| {
                datafusion::common::DataFusionError::Execution(format!(
                    "Cannot read change data feed commit {version}: {error}"
                ))
            })?;
            let meta = result.meta.clone();
            let actions = get_actions(version, &result.bytes().await?)?;
            let timestamp = resolve_commit_timestamp_from_actions(
                version,
                &meta,
                Some(current.protocol()),
                Some(current.metadata()),
                &actions,
            )?;
            let (protocol, metadata) = resolve_effective_protocol_and_metadata(
                Some(current.protocol()),
                Some(current.metadata()),
                &actions,
            )
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Plan("Missing Delta metadata".into())
            })?;
            current = Arc::new(DeltaSnapshot::from_metadata_only_parts(
                log_store.as_ref(),
                snapshot.load_config().clone(),
                version,
                protocol,
                metadata,
                HashMap::new(),
                HashMap::new(),
                BTreeMap::new(),
            )?);
            if !enabled(current.metadata()) {
                return plan_err!("Change data feed is not enabled at version {version}");
            }
            current.verify_change_data_feed()?;
            current.ensure_data_read_supported()?;
            validate_read_schema(current.schema(), snapshot.schema(), version)?;
            let cdc = actions
                .iter()
                .filter_map(|action| match action {
                    Action::Cdc(cdc) => Some(Add {
                        path: cdc.path.clone(),
                        partition_values: cdc.partition_values.clone(),
                        size: cdc.size,
                        ..Default::default()
                    }),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if !cdc.is_empty() {
                changes.push(ChangeFiles {
                    snapshot: current.clone(),
                    files: cdc,
                    exclude: None,
                    change_type: None,
                    timestamp,
                });
                continue;
            }
            let mut adds = BTreeMap::new();
            let mut removes = Vec::new();
            for action in actions {
                match action {
                    Action::Add(add) if add.data_change => {
                        adds.insert(add.path.clone(), add);
                    }
                    Action::Remove(remove) if remove.data_change => {
                        removes.push(remove);
                    }
                    _ => {}
                }
            }
            let mut deleted = Vec::new();
            for remove in removes {
                let old = removed_file(&mut table, version, remove).await?;
                if let Some(new) = adds.remove(&old.path) {
                    // A DV replacement changes visibility within one physical file.
                    if old.deletion_vector != new.deletion_vector {
                        changes.push(ChangeFiles {
                            snapshot: current.clone(),
                            files: vec![old.clone()],
                            exclude: Some(new.clone()),
                            change_type: Some("delete"),
                            timestamp,
                        });
                        changes.push(ChangeFiles {
                            snapshot: current.clone(),
                            files: vec![new],
                            exclude: Some(old),
                            change_type: Some("insert"),
                            timestamp,
                        });
                    }
                } else {
                    deleted.push(old);
                }
            }
            for (files, change_type) in [
                (deleted, "delete"),
                (adds.into_values().collect(), "insert"),
            ] {
                if !files.is_empty() {
                    changes.push(ChangeFiles {
                        snapshot: current.clone(),
                        files,
                        exclude: None,
                        change_type: Some(change_type),
                        timestamp,
                    });
                }
            }
        }
        Ok(Self {
            schema: feed_schema(snapshot.schema()),
            snapshot,
            log_store,
            changes,
        })
    }

    fn scan_files(
        &self,
        session: &dyn Session,
        change: &ChangeFiles,
        config: &DeltaScanConfig,
        files: &[Add],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = config.schema.as_ref().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal("Missing change data read schema".into())
        })?;
        if config.row_index_column_name.is_some()
            || files.iter().any(|file| file.deletion_vector.is_some())
        {
            let output = df_logical_schema(
                &self.snapshot,
                &None,
                &config.row_index_column_name,
                &None,
                &None,
                Some(schema.clone()),
            )?;
            return Ok(Arc::new(DeltaScanByAddsExec::new(
                build_eager_adds_input(files, session.config().target_partitions())?,
                self.log_store.config().location.clone(),
                self.snapshot.version(),
                schema.clone(),
                output,
                config.clone(),
                None,
                None,
                None,
                None,
                self.snapshot.load_config().catalog_managed_commits.clone(),
            )));
        }
        // The CDC type is a reserved, unmapped Parquet column even on mapped tables.
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == CHANGE_TYPE_COLUMN {
                    Arc::new(field.as_ref().clone().with_metadata(HashMap::from([
                        (
                            "delta.columnMapping.physicalName".to_string(),
                            CHANGE_TYPE_COLUMN.to_string(),
                        ),
                        (
                            FIELD_ALIASES_METADATA_KEY.to_string(),
                            format!("[\"{CHANGE_TYPE_COLUMN}\"]"),
                        ),
                    ])))
                } else {
                    field.clone()
                }
            })
            .collect::<Vec<_>>();
        let read_schema = Arc::new(Schema::new(fields));
        let kernel = StructType::try_from(read_schema.as_ref())?;
        // CDC files include _change_type; historical files use the selected read schema,
        // which can include newly added columns that must be filled with nulls.
        let snapshot = DeltaSnapshot::from_metadata_only_parts(
            self.log_store.as_ref(),
            change.snapshot.load_config().clone(),
            change.snapshot.version(),
            change.snapshot.protocol().clone(),
            change.snapshot.metadata().clone().with_schema(&kernel)?,
            HashMap::new(),
            HashMap::new(),
            BTreeMap::new(),
        )?;
        let physical = get_physical_schema(&kernel, snapshot.effective_column_mapping_mode())?;
        let partition_columns = snapshot.physical_partition_columns();
        let file_schema = Arc::new(Schema::new(
            physical
                .fields()
                .iter()
                .filter(|field| {
                    !partition_columns
                        .iter()
                        .any(|column| column.physical_name == *field.name())
                })
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let output = df_logical_schema(
            &snapshot,
            &None,
            &config.row_index_column_name,
            &None,
            &None,
            Some(schema.clone()),
        )?;
        let projection = file_scan_projection_for_schema(&snapshot, config, &file_schema, &output)?;
        let scan_config = DeltaScanConfig {
            schema: Some(read_schema),
            ..config.clone()
        };
        let scan = build_file_scan_config(
            &snapshot,
            &self.log_store,
            files,
            &scan_config,
            FileScanParams {
                projection: Some(&projection),
                limit: None,
                pushdown_filter: None,
                sort_order: None,
                table_stats_mode: TableStatsMode::Unknown,
            },
            session,
            file_schema,
        )?;
        align_delta_scan_output(DataSourceExec::from_data_source(scan), output)
    }

    pub(crate) fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&[usize]>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut plans = Vec::new();
        let mut row_index_column = "__sail_cdf_row_index".to_string();
        while self.schema.field_with_name(&row_index_column).is_ok() {
            row_index_column.push('_');
        }
        for change in &self.changes {
            let mut fields = self.snapshot.schema().fields().to_vec();
            if change.change_type.is_none() {
                fields.push(Arc::new(Field::new(
                    CHANGE_TYPE_COLUMN,
                    DataType::Utf8,
                    true,
                )));
            }
            let config = DeltaScanConfig {
                schema: Some(Arc::new(Schema::new(fields))),
                row_index_column_name: change.exclude.as_ref().map(|_| row_index_column.clone()),
                ..Default::default()
            };
            let mut plan = self.scan_files(session, change, &config, &change.files)?;
            if let Some(exclude) = &change.exclude {
                let other =
                    self.scan_files(session, change, &config, std::slice::from_ref(exclude))?;
                let left_key = Arc::new(Column::new(
                    &row_index_column,
                    plan.schema().index_of(&row_index_column)?,
                ));
                let right_key = Arc::new(Column::new(
                    &row_index_column,
                    other.schema().index_of(&row_index_column)?,
                ));
                plan = Arc::new(HashJoinExec::try_new(
                    Arc::new(CoalescePartitionsExec::new(plan)),
                    Arc::new(CoalescePartitionsExec::new(other)),
                    vec![(left_key, right_key)],
                    None,
                    &JoinType::LeftAnti,
                    None,
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )?);
            }
            let mut expressions = self
                .snapshot
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    Ok((
                        Arc::new(Column::new(
                            field.name(),
                            plan.schema().index_of(field.name())?,
                        )) as Arc<dyn PhysicalExpr>,
                        field.name().clone(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let change_type: Arc<dyn PhysicalExpr> = match change.change_type {
                Some(value) => Arc::new(Literal::new(ScalarValue::Utf8(Some(value.to_string())))),
                None => Arc::new(Column::new(
                    CHANGE_TYPE_COLUMN,
                    plan.schema().index_of(CHANGE_TYPE_COLUMN)?,
                )),
            };
            expressions.extend([
                (change_type, CHANGE_TYPE_COLUMN.to_string()),
                (
                    Arc::new(Literal::new(ScalarValue::Int64(Some(
                        change.snapshot.version(),
                    )))),
                    COMMIT_VERSION_COLUMN.to_string(),
                ),
                (
                    Arc::new(Literal::new(ScalarValue::TimestampMicrosecond(
                        Some(change.timestamp.checked_mul(1000).ok_or_else(|| {
                            datafusion::common::DataFusionError::Plan(
                                "Delta commit timestamp exceeds microsecond range".into(),
                            )
                        })?),
                        Some("UTC".into()),
                    ))),
                    COMMIT_TIMESTAMP_COLUMN.to_string(),
                ),
            ]);
            plans.push(
                Arc::new(ProjectionExec::try_new(expressions, plan)?) as Arc<dyn ExecutionPlan>
            );
        }
        let plan: Arc<dyn ExecutionPlan> = if plans.is_empty() {
            Arc::new(EmptyExec::new(self.schema.clone()))
        } else {
            UnionExec::try_new(plans)?
        };
        match projection {
            Some(indices) => Ok(Arc::new(ProjectionExec::try_new(
                indices
                    .iter()
                    .map(|&index| {
                        let name = self.schema.field(index).name();
                        (
                            Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                            name.clone(),
                        )
                    })
                    .collect::<Vec<_>>(),
                plan,
            )?)),
            None => Ok(plan),
        }
    }
}

fn validate_read_schema(historical: &Schema, read: &Schema, version: i64) -> Result<()> {
    for field in historical.fields() {
        let compatible = read.field_with_name(field.name()).is_ok_and(|target| {
            field.data_type() == target.data_type()
                && ["delta.columnMapping.id", "delta.columnMapping.physicalName"]
                    .iter()
                    .all(|key| field.metadata().get(*key) == target.metadata().get(*key))
        });
        if !compatible {
            return plan_err!(
                "Change data feed cannot read incompatible schema at version {version}: column {}",
                field.name()
            );
        }
    }
    Ok(())
}

async fn removed_file(table: &mut DeltaTable, version: i64, remove: Remove) -> Result<Add> {
    if let (Some(size), Some(partition_values)) = (remove.size, remove.partition_values.clone()) {
        return Ok(Add {
            path: remove.path,
            size,
            partition_values,
            deletion_vector: remove.deletion_vector,
            ..Default::default()
        });
    }
    if version == 0 {
        return plan_err!("Invalid remove action in Delta version 0");
    }
    let mut config = table.snapshot()?.load_config().clone();
    config.require_files = true;
    let mut previous = DeltaTable::new(table.log_store(), config);
    previous.load_version(version - 1).await?;
    previous
        .snapshot()?
        .adds()
        .iter()
        .find(|add| add.path == remove.path && add.deletion_vector == remove.deletion_vector)
        .cloned()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(format!(
                "Cannot resolve removed file {} at version {version}",
                remove.path
            ))
        })
}

async fn timestamp_bound(
    table: &mut DeltaTable,
    latest: i64,
    timestamp: &str,
    starting: bool,
) -> Result<i64> {
    let timestamp = parse_timestamp_as_of(timestamp)?.timestamp_millis();
    if starting && timestamp > table.get_version_timestamp(latest).await? {
        return plan_err!("startingTimestamp is after the latest Delta commit");
    }
    let (mut low, mut high) = (0, latest);
    let mut result = if starting { latest + 1 } else { -1 };
    while low <= high {
        let middle = low + (high - low) / 2;
        let value = table.get_version_timestamp(middle).await?;
        if (starting && value >= timestamp) || (!starting && value > timestamp) {
            if starting {
                result = middle;
            }
            high = middle - 1;
        } else {
            if !starting {
                result = middle;
            }
            low = middle + 1;
        }
    }
    Ok(result)
}
