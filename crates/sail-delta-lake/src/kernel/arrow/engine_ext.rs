// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
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

// [Credit]: <https://github.com/delta-io/delta-rs/blob/1f0b4d0965a85400c1effc6e9b4c7ebbb6795978/crates/core/src/kernel/arrow/engine_ext.rs>

//! Utilities for interacting with Kernel APIs using Arrow data structures.
//!
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Fields;
use datafusion::arrow::array::{Array, BooleanArray, MapArray, StringArray, StructArray};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::parse_json;
use delta_kernel::expressions::{ColumnName, Scalar, StructData};
use delta_kernel::scan::{Scan, ScanMetadata};
use delta_kernel::schema::{
    ArrayType, DataType, MapType, PrimitiveType, Schema, SchemaRef, SchemaTransform, StructField,
    StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::table_properties::{DataSkippingNumIndexedCols, TableProperties};
use delta_kernel::{
    DeltaResult, Engine, EngineData, ExpressionEvaluator, ExpressionRef, PredicateRef, Version,
};
use itertools::Itertools;

use crate::conversion::ScalarConverter;
use crate::kernel::snapshot::SCAN_ROW_ARROW_SCHEMA;
use crate::kernel::{DeltaResult as DeltaResultLocal, DeltaTableError};

/// [`ScanMetadata`] contains (1) a [`RecordBatch`] specifying data files to be scanned
/// and (2) a vector of transforms (one transform per scan file) that must be applied to the data read
/// from those files.
pub(crate) struct ScanMetadataArrow {
    /// Record batch with one row per file to scan
    pub scan_files: RecordBatch,

    /// Row-level transformations to apply to data read from files.
    ///
    /// Each entry in this vector corresponds to a row in the `scan_files` data. The entry is an
    /// expression that must be applied to convert the file's data into the logical schema
    /// expected by the scan:
    ///
    /// - `Some(expr)`: Apply this expression to transform the data to match [`Scan::schema()`].
    /// - `None`: No transformation is needed; the data is already in the correct logical form.
    ///
    /// Note: This vector can be indexed by row number.
    #[allow(dead_code)]
    pub scan_file_transforms: Vec<Option<ExpressionRef>>,
}

/// Internal extension trait to streamline working with Kernel scan objects.
///
/// THe trait mainly handles conversion between arrow `RecordBatch` and `ArrowEngineData`.
/// The exposed methods are arrow-variants of methods already exposed on the kernel scan.
pub(crate) trait ScanExt {
    /// Get the metadata for a table scan.
    ///
    /// This method handles translation between `EngineData` and `RecordBatch`
    /// and will already apply any selection vectors to the data.
    /// See [`Scan::scan_metadata`] for details.
    fn scan_metadata_arrow(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>>;

    fn scan_metadata_from_arrow(
        &self,
        engine: &dyn Engine,
        existing_version: Version,
        existing_data: Box<dyn Iterator<Item = RecordBatch>>,
        existing_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>>;
}

impl ScanExt for Scan {
    fn scan_metadata_arrow(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>> {
        Ok(self
            .scan_metadata(engine)?
            .map_ok(kernel_to_arrow)
            .flatten())
    }

    fn scan_metadata_from_arrow(
        &self,
        engine: &dyn Engine,
        existing_version: Version,
        existing_data: Box<dyn Iterator<Item = RecordBatch>>,
        existing_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>> {
        let engine_iter =
            existing_data.map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>);
        Ok(self
            .scan_metadata_from(engine, existing_version, engine_iter, existing_predicate)?
            .map_ok(kernel_to_arrow)
            .flatten())
    }
}

/// Internal extension traits to the Kernel Snapshot.
///
/// These traits provide additional convenience functionality for working with Kernel snapshots.
/// Some of this may eventually be upstreamed as the kernel implementation matures.
pub(crate) trait SnapshotExt {
    /// Returns the expected file statistics schema for the snapshot.
    fn stats_schema(&self) -> DeltaResult<SchemaRef>;

    /// The expected schema for partition values
    fn partitions_schema(&self) -> DeltaResultLocal<Option<SchemaRef>>;

    /// The scheme expected for the data returned from a scan.
    ///
    /// This is an extended version of the raw schema that includes additional
    /// computations by delta-rs. Specifically the `stats_parsed` and
    /// `partitionValues_parsed` fields are added.
    fn scan_row_parsed_schema_arrow(&self) -> DeltaResultLocal<ArrowSchemaRef>;

    /// Parse stats column into a struct array.
    fn parse_stats_column(&self, batch: &RecordBatch) -> DeltaResultLocal<RecordBatch>;
}

impl SnapshotExt for Snapshot {
    fn stats_schema(&self) -> DeltaResult<SchemaRef> {
        let partition_columns = self.table_configuration().metadata().partition_columns();
        let column_mapping_mode = self.table_configuration().column_mapping_mode();
        let physical_schema = StructType::try_new(
            self.schema()
                .fields()
                .filter(|field| !partition_columns.contains(field.name()))
                .map(|field| field.make_physical(column_mapping_mode)),
        )?;
        Ok(Arc::new(stats_schema(
            &physical_schema,
            self.table_properties(),
        )?))
    }

    fn partitions_schema(&self) -> DeltaResultLocal<Option<SchemaRef>> {
        Ok(partitions_schema(
            self.schema().as_ref(),
            self.table_configuration().metadata().partition_columns(),
        )?
        .map(Arc::new))
    }

    /// Arrow schema for a parsed (including stats_parsed and partitionValues_parsed)
    /// scan row (file data).
    fn scan_row_parsed_schema_arrow(&self) -> DeltaResultLocal<ArrowSchemaRef> {
        let mut fields = SCAN_ROW_ARROW_SCHEMA.fields().to_vec();

        let stats_schema = self.stats_schema()?;
        let stats_schema: ArrowSchema = stats_schema.as_ref().try_into_arrow()?;
        fields.push(Arc::new(Field::new(
            "stats_parsed",
            ArrowDataType::Struct(stats_schema.fields().to_owned()),
            true,
        )));

        if let Some(partition_schema) = self.partitions_schema()? {
            let partition_schema: ArrowSchema = partition_schema.as_ref().try_into_arrow()?;
            fields.push(Arc::new(Field::new(
                "partitionValues_parsed",
                ArrowDataType::Struct(partition_schema.fields().to_owned()),
                false,
            )));
        }

        let schema = Arc::new(ArrowSchema::new(fields));
        Ok(schema)
    }

    fn parse_stats_column(&self, batch: &RecordBatch) -> DeltaResultLocal<RecordBatch> {
        let Some((stats_idx, _)) = batch.schema_ref().column_with_name("stats") else {
            return Err(DeltaTableError::schema(
                "stats column not found".to_string(),
            ));
        };

        let mut columns = batch.columns().to_vec();
        let mut fields = batch.schema().fields().to_vec();
        let column_mapping_mode = self.table_configuration().column_mapping_mode();

        let stats_schema = self.stats_schema()?;
        let stats_batch = batch.project(&[stats_idx])?;
        let stats_data = Box::new(ArrowEngineData::new(stats_batch));

        let parsed = parse_json(stats_data, stats_schema)?;
        let parsed: RecordBatch = ArrowEngineData::try_from_engine_data(parsed)?.into();

        let stats_array: Arc<StructArray> = Arc::new(parsed.into());
        fields.push(Arc::new(Field::new(
            "stats_parsed",
            stats_array.data_type().to_owned(),
            true,
        )));
        columns.push(stats_array.clone());

        if let Some(partition_schema) = self.partitions_schema()? {
            let partition_array = parse_partition_values_array(
                batch,
                partition_schema.as_ref(),
                "fileConstantValues.partitionValues",
                column_mapping_mode,
            )?;
            fields.push(Arc::new(Field::new(
                "partitionValues_parsed",
                partition_array.data_type().to_owned(),
                false,
            )));
            columns.push(Arc::new(partition_array));
        }

        Ok(RecordBatch::try_new(
            Arc::new(ArrowSchema::new(fields)),
            columns,
        )?)
    }
}

fn parse_partition_values_array(
    batch: &RecordBatch,
    partition_schema: &StructType,
    path: &str,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResultLocal<StructArray> {
    let partitions = map_array_from_path(batch, path)?;
    let num_rows = partitions.len();

    let mut collected: HashMap<String, Vec<Scalar>> = partition_schema
        .fields()
        .map(|f| {
            (
                f.physical_name(column_mapping_mode).to_string(),
                Vec::with_capacity(num_rows),
            )
        })
        .collect();

    for row in 0..num_rows {
        if partitions.is_null(row) {
            return Err(DeltaTableError::generic(
                "Expected partition values map, found null entry.",
            ));
        }
        let raw_values = collect_partition_row(&partitions.value(row))?;

        for field in partition_schema.fields() {
            let physical_name = field.physical_name(column_mapping_mode);
            let value = raw_values
                .get(physical_name)
                .or_else(|| raw_values.get(field.name()));
            let scalar = match field.data_type() {
                DataType::Primitive(primitive) => match value {
                    Some(Some(raw)) => primitive.parse_scalar(raw)?,
                    _ => Scalar::Null(field.data_type().clone()),
                },
                _ => {
                    return Err(DeltaTableError::generic(
                        "nested partitioning values are not supported",
                    ))
                }
            };
            collected
                .get_mut(physical_name)
                .ok_or_else(|| DeltaTableError::schema("partition field missing".to_string()))?
                .push(scalar);
        }
    }

    let columns = partition_schema
        .fields()
        .map(|field| {
            let physical_name = field.physical_name(column_mapping_mode);
            ScalarConverter::scalars_to_arrow_array(
                field,
                collected.get(physical_name).ok_or_else(|| {
                    DeltaTableError::schema("partition field missing".to_string())
                })?,
            )
        })
        .collect::<DeltaResultLocal<Vec<_>>>()?;

    let arrow_fields: Fields = Fields::from(
        partition_schema
            .fields()
            .map(|f| f.try_into_arrow())
            .collect::<Result<Vec<Field>, _>>()?,
    );

    Ok(StructArray::try_new(arrow_fields, columns, None)?)
}

fn map_array_from_path<'a>(batch: &'a RecordBatch, path: &str) -> DeltaResultLocal<&'a MapArray> {
    let mut segments = path.split('.');
    let first = segments
        .next()
        .ok_or_else(|| DeltaTableError::generic("partition column path must not be empty"))?;

    let mut current: &dyn Array = batch
        .column_by_name(first)
        .map(|col| col.as_ref())
        .ok_or_else(|| {
            DeltaTableError::schema(format!("{first} column not found when parsing partitions"))
        })?;

    for segment in segments {
        let struct_array = current
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DeltaTableError::schema(format!("Expected struct column while traversing {path}"))
            })?;
        current = struct_array
            .column_by_name(segment)
            .map(|col| col.as_ref())
            .ok_or_else(|| {
                DeltaTableError::schema(format!(
                    "{segment} column not found while traversing {path}"
                ))
            })?;
    }

    current
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DeltaTableError::schema(format!("Column {path} is not a map")))
}

fn collect_partition_row(value: &StructArray) -> DeltaResultLocal<HashMap<String, Option<String>>> {
    let keys = value
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DeltaTableError::schema("map key column is not Utf8".to_string()))?;
    let vals = value
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DeltaTableError::schema("map value column is not Utf8".to_string()))?;

    let mut result = HashMap::with_capacity(keys.len());
    for (key, value) in keys.iter().zip(vals.iter()) {
        if let Some(k) = key {
            result.insert(k.to_string(), value.map(|v| v.to_string()));
        }
    }
    Ok(result)
}

fn partitions_schema(
    schema: &StructType,
    partition_columns: &[String],
) -> DeltaResultLocal<Option<StructType>> {
    if partition_columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(StructType::try_new(
        partition_columns
            .iter()
            .map(|col| {
                schema.field(col).cloned().ok_or_else(|| {
                    DeltaTableError::generic(format!("Partition column {col} not found in schema"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    )?))
}

/// Generates the expected schema for file statistics.
///
/// The base stats schema is dependent on the current table configuration and derived via:
/// - only fields present in data files are included (use physical names, no partition columns)
/// - if `dataSkippingStatsColumns` is set, include only those columns.
///   Column names may refer to struct fields in which case all child fields are included.
/// - otherwise the first `dataSkippingNumIndexedCols` (default 32) leaf fields are included.
/// - all fields are made nullable.
///
/// For the `nullCount` schema, we consider the whole base schema and convert all leaf fields
/// to data type LONG. Maps, arrays, and variant are considered leaf fields in this case.
///
/// For the min / max schemas, we non-eligible leaf fields from the base schema.
/// Field eligibility is determined by the fields data type via [`is_skipping_eligeble_datatype`].
///
/// The overall schema is then:
/// ```ignored
/// {
///    numRecords: long,
///    nullCount: <derived null count schema>,
///    minValues: <derived min/max schema>,
///    maxValues: <derived min/max schema>,
/// }
/// ```
pub(crate) fn stats_schema(
    physical_file_schema: &Schema,
    table_properties: &TableProperties,
) -> DeltaResult<Schema> {
    let mut fields = Vec::with_capacity(4);
    fields.push(StructField::nullable("numRecords", DataType::LONG));

    // generate the base stats schema:
    // - make all fields nullable
    // - include fields according to table properties (num_indexed_cols, stats_coliumns, ...)
    let mut base_transform = BaseStatsTransform::new(table_properties);
    if let Some(base_schema) = base_transform.transform_struct(physical_file_schema) {
        let base_schema = base_schema.into_owned();

        // convert all leaf fields to data type LONG for null count
        let mut null_count_transform = NullCountStatsTransform;
        if let Some(null_count_schema) = null_count_transform.transform_struct(&base_schema) {
            fields.push(StructField::nullable(
                "nullCount",
                null_count_schema.into_owned(),
            ));
        };

        // include only min/max skipping eligible fields (data types)
        let mut min_max_transform = MinMaxStatsTransform;
        if let Some(min_max_schema) = min_max_transform.transform_struct(&base_schema) {
            let min_max_schema = min_max_schema.into_owned();
            fields.push(StructField::nullable("minValues", min_max_schema.clone()));
            fields.push(StructField::nullable("maxValues", min_max_schema));
        }
    }
    StructType::try_new(fields)
}

// Convert a min/max stats schema into a nullcount schema (all leaf fields are LONG)
pub(crate) struct NullCountStatsTransform;
impl<'a> SchemaTransform<'a> for NullCountStatsTransform {
    fn transform_primitive(&mut self, _ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        Some(Cow::Owned(PrimitiveType::Long))
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        if matches!(
            &field.data_type,
            DataType::Array(_) | DataType::Map(_) | DataType::Variant(_)
        ) {
            return Some(Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: DataType::LONG,
                nullable: true,
                metadata: Default::default(),
            }));
        }

        match self.transform(&field.data_type)? {
            Cow::Borrowed(_) => Some(Cow::Borrowed(field)),
            dt => Some(Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: dt.into_owned(),
                nullable: true,
                metadata: Default::default(),
            })),
        }
    }
}

/// Transforms a table schema into a base stats schema.
///
/// Base stats schema in this case refers the subsets of fields in the table schema
/// that may be considered for stats collection. Depending on the type of stats - min/max/nullcount/... -
/// additional transformations may be applied.
///
/// The concrete shape of the schema depends on the table configuration.
/// * `dataSkippingStatsColumns` - used to explicitly specify the columns
///   to be used for data skipping statistics. (takes precedence)
/// * `dataSkippingNumIndexedCols` - used to specify the number of columns
///   to be used for data skipping statistics. Defaults to 32.
///
/// All fields are nullable.
struct BaseStatsTransform {
    n_columns: Option<DataSkippingNumIndexedCols>,
    added_columns: u64,
    column_names: Option<Vec<ColumnName>>,
    path: Vec<String>,
}

impl BaseStatsTransform {
    fn new(props: &TableProperties) -> Self {
        // if data_skipping_stats_columns is specified, it takes precedence
        // over data_skipping_num_indexed_cols, even if that is also specified
        if let Some(columns_names) = &props.data_skipping_stats_columns {
            Self {
                n_columns: None,
                added_columns: 0,
                column_names: Some(columns_names.clone()),
                path: Vec::new(),
            }
        } else {
            Self {
                n_columns: Some(
                    props
                        .data_skipping_num_indexed_cols
                        .unwrap_or(DataSkippingNumIndexedCols::NumColumns(32)),
                ),
                added_columns: 0,
                column_names: None,
                path: Vec::new(),
            }
        }
    }
}

impl<'a> SchemaTransform<'a> for BaseStatsTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        // Check if the number of columns is set and if the added columns exceed the limit
        // In the constructor we assert this will always be None if column_names are specified
        if let Some(DataSkippingNumIndexedCols::NumColumns(n_cols)) = self.n_columns {
            if self.added_columns >= n_cols {
                return None;
            }
        }

        self.path.push(field.name.clone());
        let data_type = field.data_type();

        // keep the field if it:
        // - is a struct field and we need to traverse its children
        // - OR it is referenced by the column names
        // - OR it is a primitive type / leaf field
        let should_include = matches!(data_type, DataType::Struct(_))
            || self
                .column_names
                .as_ref()
                .map(|ns| should_include_column(&ColumnName::new(&self.path), ns))
                .unwrap_or(true);

        if !should_include {
            self.path.pop();
            return None;
        }

        // increment count only for leaf columns.
        if !matches!(data_type, DataType::Struct(_)) {
            self.added_columns += 1;
        }

        let field = match self.transform(&field.data_type)? {
            Cow::Borrowed(_) if field.is_nullable() => Cow::Borrowed(field),
            data_type => Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: true,
                metadata: Default::default(),
            }),
        };

        self.path.pop();

        // exclude struct fields with no children
        if matches!(
            field.data_type(),
            DataType::Struct(dt) if dt.fields().count() == 0
        ) {
            None
        } else {
            Some(field)
        }
    }
}

// removes all fields with non eligible data types
//
// should only be applied to schema oricessed via `BaseStatsTransform`.
struct MinMaxStatsTransform;

impl<'a> SchemaTransform<'a> for MinMaxStatsTransform {
    // array and map fields are not eligible for data skipping, so filter them out.
    fn transform_array(&mut self, _: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        None
    }
    fn transform_map(&mut self, _: &'a MapType) -> Option<Cow<'a, MapType>> {
        None
    }
    fn transform_variant(&mut self, _: &'a StructType) -> Option<Cow<'a, StructType>> {
        None
    }

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if is_skipping_eligeble_datatype(ptype) {
            Some(Cow::Borrowed(ptype))
        } else {
            None
        }
    }
}

// Checks if a column should be included or traversed into.
//
// Returns true if the column name is included in the list of column names
// or if the column name is a prefix of any column name in the list
// or if the column name is a child of any column name in the list
fn should_include_column(column_name: &ColumnName, column_names: &[ColumnName]) -> bool {
    column_names.iter().any(|name| {
        name.as_ref().starts_with(column_name) || column_name.as_ref().starts_with(name)
    })
}

/// Checks if a data type is eligible for min/max file skipping.
/// https://github.com/delta-io/delta/blob/143ab3337121248d2ca6a7d5bc31deae7c8fe4be/kernel/kernel-api/src/main/java/io/delta/kernel/internal/skipping/StatsSchemaHelper.java#L61
fn is_skipping_eligeble_datatype(data_type: &PrimitiveType) -> bool {
    matches!(
        data_type,
        &PrimitiveType::Byte
            | &PrimitiveType::Short
            | &PrimitiveType::Integer
            | &PrimitiveType::Long
            | &PrimitiveType::Float
            | &PrimitiveType::Double
            | &PrimitiveType::Date
            | &PrimitiveType::Timestamp
            | &PrimitiveType::TimestampNtz
            | &PrimitiveType::String
            // | &PrimitiveType::Boolean
            | PrimitiveType::Decimal(_)
    )
}

fn kernel_to_arrow(metadata: ScanMetadata) -> DeltaResult<ScanMetadataArrow> {
    let scan_file_transforms = metadata
        .scan_file_transforms
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| metadata.scan_files.selection_vector()[i].then_some(v))
        .collect();
    let (data, selection) = metadata.scan_files.into_parts();
    let batch = ArrowEngineData::try_from_engine_data(data)?.into();
    let scan_files = filter_record_batch(&batch, &BooleanArray::from(selection))?;
    Ok(ScanMetadataArrow {
        scan_files,
        scan_file_transforms,
    })
}

/// Internal extension trait for expression evaluators.
///
/// This just abstracts the conversion between Arrow [`RecoedBatch`]es and
/// Kernel's [`ArrowEngineData`].
pub(crate) trait ExpressionEvaluatorExt {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch>;
}

impl<T: ExpressionEvaluator + ?Sized> ExpressionEvaluatorExt for T {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        let engine_data = ArrowEngineData::new(batch);
        Ok(ArrowEngineData::try_from_engine_data(T::evaluate(self, &engine_data)?)?.into())
    }
}

/// Extension trait for Kernel's [`StructData`].
///
/// StructData is the data structure contained in a Struct scalar.
/// The exposed API on kernels struct data is very minimal and does not allow
/// for conveniently probing the fields / values contained within [`StructData`].
///
/// This trait therefore adds convenience methods for accessing fields and values.
#[allow(dead_code)]
pub trait StructDataExt {
    /// Returns a reference to the field with the given name, if it exists.
    fn field(&self, name: &str) -> Option<&StructField>;

    /// Returns a reference to the value with the given index, if it exists.
    fn value(&self, index: usize) -> Option<&Scalar>;

    /// Returns the index of the field with the given name, if it exists.
    fn index_of(&self, name: &str) -> Option<usize>;
}

impl StructDataExt for StructData {
    fn field(&self, name: &str) -> Option<&StructField> {
        self.fields().iter().find(|f| f.name() == name)
    }

    fn index_of(&self, name: &str) -> Option<usize> {
        self.fields().iter().position(|f| f.name() == name)
    }

    fn value(&self, index: usize) -> Option<&Scalar> {
        self.values().get(index)
    }
}
