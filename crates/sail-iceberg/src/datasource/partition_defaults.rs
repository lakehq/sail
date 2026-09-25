use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_common::{Result, ScalarValue, internal_datafusion_err, plan_datafusion_err};
use datafusion_datasource::file::FileSource;
use sail_common_datafusion::schema_evolution::{
    FIELD_DEFAULT_METADATA_KEY, SchemaEvolutionCastColumnExpr, StructFieldMatching,
    encode_field_default,
};

use crate::datasource::type_converter::{iceberg_field_id, iceberg_type_to_arrow};
use crate::spec::{DataFile, PartitionSpec, Transform};
use crate::utils::conversions::to_scalar;

/// Consumed during scan construction; defaults travel to workers in field metadata.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct IdentityPartitionDefaults(BTreeMap<i32, String>);

impl IdentityPartitionDefaults {
    pub(super) fn from_file(
        file: &DataFile,
        specs: &[PartitionSpec],
        schema: &crate::spec::Schema,
    ) -> Result<Self> {
        let spec = specs
            .iter()
            .find(|spec| spec.spec_id() == file.partition_spec_id)
            .ok_or_else(|| {
                plan_datafusion_err!("Unknown Iceberg partition spec {}", file.partition_spec_id)
            })?;
        let mut defaults = BTreeMap::new();
        for (index, partition_field) in spec.fields().iter().enumerate() {
            if partition_field.transform != Transform::Identity {
                continue;
            }
            let Some(field) = schema.field_by_id(partition_field.source_id) else {
                continue;
            };
            let value = file.partition.get(index).ok_or_else(|| {
                plan_datafusion_err!(
                    "Missing Iceberg partition value for {}",
                    partition_field.name
                )
            })?;
            let scalar = match value {
                Some(value) => to_scalar(value, &field.field_type)?,
                None => ScalarValue::try_new_null(&iceberg_type_to_arrow(&field.field_type)?)?,
            };
            defaults.insert(field.id, encode_field_default(&scalar)?);
        }
        Ok(Self(defaults))
    }

    fn field(&self, field: &Field) -> Result<Field> {
        let mut field = field.clone();
        if let DataType::Struct(fields) = field.data_type() {
            let fields = fields
                .iter()
                .map(|field| self.field(field).map(Arc::new))
                .collect::<Result<Vec<_>>>()?;
            field = field.with_data_type(DataType::Struct(fields.into()));
        }
        if let Some(id) = iceberg_field_id(&field)?
            && let Some(value) = self.0.get(&id)
        {
            let mut metadata = field.metadata().clone();
            metadata.insert(FIELD_DEFAULT_METADATA_KEY.to_string(), value.clone());
            field.set_metadata(metadata);
        }
        Ok(field)
    }

    fn schema(&self, schema: &Schema) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|field| self.field(field))
                .collect::<Result<Vec<_>>>()?,
            schema.metadata().clone(),
        )))
    }
}

pub(super) fn create_data_scan(config: FileScanConfig) -> Result<Arc<dyn ExecutionPlan>> {
    let mut groups = BTreeMap::<IdentityPartitionDefaults, Vec<FileGroup>>::new();
    for group in &config.file_groups {
        let mut files = BTreeMap::<IdentityPartitionDefaults, Vec<_>>::new();
        for file in group.files() {
            let defaults = file
                .extensions
                .get::<IdentityPartitionDefaults>()
                .cloned()
                .unwrap_or_default();
            let mut file = file.clone();
            file.extensions = Default::default();
            files.entry(defaults).or_default().push(file);
        }
        for (defaults, files) in files {
            groups
                .entry(defaults)
                .or_default()
                .push(FileGroup::from(files));
        }
    }
    fn needs_default(field: &Field, defaults: &IdentityPartitionDefaults) -> bool {
        iceberg_field_id(field)
            .ok()
            .flatten()
            .is_some_and(|id| defaults.0.contains_key(&id))
            || matches!(field.data_type(), DataType::Struct(fields) if fields.iter().any(|field| needs_default(field, defaults)))
    }
    let projected = config.projected_schema()?;
    let parquet_filter = config
        .file_source
        .downcast_ref::<ParquetSource>()
        .and_then(|source| source.filter());
    if groups.keys().all(|defaults| defaults.0.is_empty())
        || (parquet_filter.is_none()
            && groups.keys().all(|defaults| {
                !projected
                    .fields()
                    .iter()
                    .any(|field| needs_default(field, defaults))
            }))
    {
        let mut config = config;
        for group in &mut config.file_groups {
            *group = FileGroup::from(
                group
                    .files()
                    .iter()
                    .cloned()
                    .map(|mut file| {
                        file.extensions = Default::default();
                        file
                    })
                    .collect::<Vec<_>>(),
            );
        }
        return Ok(DataSourceExec::from_data_source(config));
    }

    let target = config.projected_schema()?;
    let parquet = config
        .file_source
        .downcast_ref::<ParquetSource>()
        .ok_or_else(|| internal_datafusion_err!("Iceberg scan requires a Parquet source"))?;
    let mut scans = Vec::with_capacity(groups.len());
    for (defaults, file_groups) in groups {
        let table_schema =
            TableSchema::builder(defaults.schema(parquet.table_schema().file_schema())?)
                .with_table_partition_cols(parquet.table_schema().table_partition_cols().to_vec())
                .build();
        // Preserve whole-file partitions without advertising compatible hash
        // partitioning: interleaving these scans would reset file-local row offsets.
        let partitioning = config
            .output_partitioning
            .as_ref()
            .map(|_| Partitioning::UnknownPartitioning(file_groups.len()));
        let mut source = ParquetSource::new(table_schema)
            .with_table_parquet_options(parquet.table_parquet_options().clone());
        if let Some(predicate) = parquet.filter() {
            source = source.with_predicate(predicate);
        }
        let source = match parquet.projection() {
            Some(projection) => source
                .try_pushdown_projection(projection)?
                .ok_or_else(|| internal_datafusion_err!("Cannot project Iceberg Parquet scan"))?,
            None => Arc::new(source),
        };
        let unknown =
            datafusion_common::Statistics::new_unknown(source.table_schema().table_schema());
        let statistics = sail_common_datafusion::statistics::aggregate_statistics(
            source.table_schema().table_schema(),
            file_groups
                .iter()
                .flat_map(|group| group.files())
                .map(|file| file.statistics.as_deref().unwrap_or(&unknown)),
        );
        let scan = FileScanConfigBuilder::from(config.clone())
            .with_source(source)
            .with_file_groups(file_groups)
            .with_statistics(statistics)
            .with_output_partitioning(partitioning)
            .build();
        let scan_schema = scan.projected_schema()?;
        let expressions = target
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), index));
                let column = if scan_schema.field(index).data_type() == field.data_type() {
                    column
                } else {
                    Arc::new(SchemaEvolutionCastColumnExpr::new_with_matching(
                        column,
                        scan_schema.fields()[index].clone(),
                        field.clone(),
                        None,
                        StructFieldMatching::FieldId,
                    ))
                };
                (column, field.name().clone())
            })
            .collect::<Vec<_>>();
        scans.push(Arc::new(ProjectionExec::try_new_with_schema_metadata(
            expressions,
            DataSourceExec::from_data_source(scan),
            &target,
        )?) as Arc<dyn ExecutionPlan>);
    }
    let scan = if scans.len() == 1 {
        scans.remove(0)
    } else {
        UnionExec::try_new(scans)?
    };
    Ok(match config.limit {
        Some(limit) => Arc::new(datafusion::physical_plan::limit::GlobalLimitExec::new(
            scan,
            0,
            Some(limit),
        )),
        None => scan,
    })
}
